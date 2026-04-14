"""
BioTrack v1.0 — Alert Engine (Week 5–6)
Author : Satyam W. (Python Automation Team)
Purpose: Receive sensor readings, evaluate thresholds, and dispatch
         WARNING / CRITICAL / EMERGENCY notifications via Email + SMS.
         Includes cooldown deduplication to prevent alert storms.
         Every alert is logged to the audit API (21 CFR Part 11).

Usage:
    # Run standalone — listens for sensor data via queue
    python biotrack_alert_engine.py

    # Test all alert levels immediately
    python biotrack_alert_engine.py --self-test

    # Custom SMTP config via env
    SMTP_HOST=smtp.gmail.com SMTP_PORT=587 \
    SMTP_USER=you@gmail.com SMTP_PASS=apppassword \
    python biotrack_alert_engine.py --self-test

Integration (import in biotrack_virtual_sensors.py):
    from biotrack_alert_engine import alert_engine, SeverityLevel
    alert_engine.evaluate(sensor_id, storage_unit, temperature, humidity, door_open)

Dependencies:
    pip install requests python-dotenv
    (smtplib is built into Python — no install needed)
"""

import argparse
import json
import logging
import os
import queue
import smtplib
import ssl
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Optional

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # dotenv optional — fall back to real env vars


# ─────────────────────────────────────────────
# 1. LOGGING
# ─────────────────────────────────────────────

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/alerts.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("BioTrack.AlertEngine")


# ─────────────────────────────────────────────
# 2. CONFIGURATION
# ─────────────────────────────────────────────

# ── SMTP (email) ──────────────────────────────
SMTP_HOST  = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT  = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER  = os.getenv("SMTP_USER", "biotrack.alerts@gmail.com")
SMTP_PASS  = os.getenv("SMTP_PASS", "")           # use App Password for Gmail
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER)

# Recipients per severity (comma-separated in env)
EMAIL_WARNING   = os.getenv("EMAIL_WARNING",   "nurse-station@hospital.com").split(",")
EMAIL_CRITICAL  = os.getenv("EMAIL_CRITICAL",  "pharmacist@hospital.com,nurse-station@hospital.com").split(",")
EMAIL_EMERGENCY = os.getenv("EMAIL_EMERGENCY", "cmo@hospital.com,pharmacist@hospital.com,nurse-station@hospital.com").split(",")

# ── API ───────────────────────────────────────
API_BASE_URL    = os.getenv("API_BASE_URL", "http://localhost:5000")
AUDIT_ENDPOINT  = "/api/audit/log"
REQUEST_TIMEOUT = 10

# ── Thresholds (°C) ───────────────────────────
# Fridge (2–8°C)   Freezer (≤ -18°C)   Ambient (15–25°C)
THRESHOLDS = {
    "fridge": {
        "WARNING":   {"temp_above": 8.0,  "temp_below": 2.0},
        "CRITICAL":  {"temp_above": 10.0, "temp_below": 0.0},
        "EMERGENCY": {"temp_above": 25.0, "temp_below": -5.0},
    },
    "freezer": {
        "WARNING":   {"temp_above": -18.0},
        "CRITICAL":  {"temp_above": -15.0},
        "EMERGENCY": {"temp_above": -5.0},
    },
    "ambient": {
        "WARNING":   {"temp_above": 25.0, "temp_below": 15.0},
        "CRITICAL":  {"temp_above": 30.0, "temp_below": 10.0},
        "EMERGENCY": {"temp_above": 35.0},
    },
}

# ── Cooldown windows (seconds) ────────────────
COOLDOWN = {
    "WARNING":   300,   # 5 minutes
    "CRITICAL":  120,   # 2 minutes
    "EMERGENCY": 0,     # never suppress — always fire
}

# ── Humidity threshold ────────────────────────
HUMIDITY_WARNING   = 80.0   # %
HUMIDITY_CRITICAL  = 90.0


# ─────────────────────────────────────────────
# 3. SEVERITY LEVEL
# ─────────────────────────────────────────────

class SeverityLevel(str, Enum):
    WARNING   = "WARNING"
    CRITICAL  = "CRITICAL"
    EMERGENCY = "EMERGENCY"

    @property
    def recipients(self) -> list[str]:
        return {
            "WARNING":   EMAIL_WARNING,
            "CRITICAL":  EMAIL_CRITICAL,
            "EMERGENCY": EMAIL_EMERGENCY,
        }[self.value]

    @property
    def cooldown(self) -> int:
        return COOLDOWN[self.value]

    @property
    def subject_prefix(self) -> str:
        return {
            "WARNING":   "[BioTrack WARNING]",
            "CRITICAL":  "[BioTrack CRITICAL]",
            "EMERGENCY": "[BioTrack EMERGENCY]",
        }[self.value]


# ─────────────────────────────────────────────
# 4. ALERT PAYLOAD
# ─────────────────────────────────────────────

@dataclass
class Alert:
    alert_id:     str
    sensor_id:    str
    storage_unit: str
    severity:     str
    alert_type:   str          # e.g. TEMP_SPIKE, DOOR_OPEN, HUMIDITY_HIGH, SILENT
    temperature:  Optional[float]
    humidity:     Optional[float]
    message:      str
    triggered_at: str          # ISO-8601
    notified:     bool = False
    audit_logged: bool = False
    metadata:     dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────
# 5. COOLDOWN REGISTRY (deduplication)
# ─────────────────────────────────────────────

class CooldownRegistry:
    """
    Prevents alert storms. Tracks last-fired time per (sensor_id, severity).
    Thread-safe via Lock.

    Example: if Fridge-4 fires CRITICAL at 14:00:00, the next CRITICAL
    for that same sensor is suppressed until 14:02:00.
    EMERGENCY is never suppressed (cooldown = 0).
    """

    def __init__(self):
        self._lock   = threading.Lock()
        self._last: dict[str, float] = {}   # key → epoch timestamp

    def _key(self, sensor_id: str, severity: str) -> str:
        return f"{sensor_id}::{severity}"

    def should_fire(self, sensor_id: str, severity: SeverityLevel) -> bool:
        """Returns True if the alert should be dispatched, False if in cooldown."""
        if severity.cooldown == 0:
            return True   # EMERGENCY always fires

        key = self._key(sensor_id, severity.value)
        now = time.time()
        with self._lock:
            last = self._last.get(key, 0)
            if (now - last) >= severity.cooldown:
                return True
            remaining = int(severity.cooldown - (now - last))
            log.debug(
                f"[COOLDOWN] {sensor_id} {severity.value} suppressed — "
                f"{remaining}s remaining"
            )
            return False

    def record(self, sensor_id: str, severity: SeverityLevel) -> None:
        key = self._key(sensor_id, severity.value)
        with self._lock:
            self._last[key] = time.time()

    def clear(self, sensor_id: str) -> None:
        """Clear all cooldowns for a sensor (e.g. when it recovers)."""
        prefix = f"{sensor_id}::"
        with self._lock:
            keys = [k for k in self._last if k.startswith(prefix)]
            for k in keys:
                del self._last[k]


# ─────────────────────────────────────────────
# 6. EMAIL NOTIFIER
# ─────────────────────────────────────────────

class EmailNotifier:
    """
    Sends HTML alert emails via SMTP (TLS).
    Falls back to dry-run logging if SMTP_PASS is not set.
    """

    def send(self, alert: Alert) -> bool:
        severity   = SeverityLevel(alert.severity)
        recipients = severity.recipients

        if not SMTP_PASS:
            log.warning(
                f"[EMAIL DRY-RUN] SMTP_PASS not set. "
                f"Would send {alert.severity} to {recipients}"
            )
            return True   # treat as success in dev

        subject = f"{severity.subject_prefix} {alert.alert_type} — {alert.storage_unit}"
        body    = self._build_html(alert)
        msg     = self._build_message(subject, body, recipients)

        try:
            ctx = ssl.create_default_context()
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.ehlo()
                server.starttls(context=ctx)
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(EMAIL_FROM, recipients, msg.as_string())

            log.info(
                f"[EMAIL SENT] {alert.severity} → {recipients}  "
                f"subject: {subject}"
            )
            return True

        except smtplib.SMTPAuthenticationError:
            log.error("[EMAIL FAIL] SMTP authentication failed — check SMTP_USER / SMTP_PASS")
        except smtplib.SMTPException as e:
            log.error(f"[EMAIL FAIL] SMTP error: {e}")
        except OSError as e:
            log.error(f"[EMAIL FAIL] Network error: {e}")
        return False

    def _build_message(
        self, subject: str, html_body: str, recipients: list[str]
    ) -> MIMEMultipart:
        msg              = MIMEMultipart("alternative")
        msg["Subject"]   = subject
        msg["From"]      = f"BioTrack Alerts <{EMAIL_FROM}>"
        msg["To"]        = ", ".join(recipients)
        msg.attach(MIMEText(html_body, "html"))
        return msg

    def _build_html(self, alert: Alert) -> str:
        color = {
            "WARNING":   "#854F0B",
            "CRITICAL":  "#A32D2D",
            "EMERGENCY": "#501313",
        }.get(alert.severity, "#333")

        bg = {
            "WARNING":   "#FAEEDA",
            "CRITICAL":  "#FCEBEB",
            "EMERGENCY": "#F09595",
        }.get(alert.severity, "#f5f5f5")

        temp_str = f"{alert.temperature:+.1f}°C" if alert.temperature is not None else "N/A"
        hum_str  = f"{alert.humidity:.0f}%" if alert.humidity is not None else "N/A"

        return f"""
        <html><body style="font-family:Arial,sans-serif;max-width:600px;margin:auto;padding:20px">
          <div style="background:{bg};border-left:6px solid {color};padding:16px 20px;border-radius:4px">
            <h2 style="color:{color};margin:0 0 8px">{alert.severity} — {alert.alert_type}</h2>
            <p style="margin:0;color:{color};font-size:14px">{alert.message}</p>
          </div>
          <table style="width:100%;margin-top:20px;border-collapse:collapse;font-size:14px">
            <tr style="background:#f9f9f9">
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-weight:bold">Sensor</td>
              <td style="padding:8px 12px;border:1px solid #e0e0e0">{alert.sensor_id}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-weight:bold">Storage Unit</td>
              <td style="padding:8px 12px;border:1px solid #e0e0e0">{alert.storage_unit}</td>
            </tr>
            <tr style="background:#f9f9f9">
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-weight:bold">Temperature</td>
              <td style="padding:8px 12px;border:1px solid #e0e0e0">{temp_str}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-weight:bold">Humidity</td>
              <td style="padding:8px 12px;border:1px solid #e0e0e0">{hum_str}</td>
            </tr>
            <tr style="background:#f9f9f9">
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-weight:bold">Alert ID</td>
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-family:monospace">{alert.alert_id}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;border:1px solid #e0e0e0;font-weight:bold">Triggered At</td>
              <td style="padding:8px 12px;border:1px solid #e0e0e0">{alert.triggered_at}</td>
            </tr>
          </table>
          <p style="margin-top:20px;font-size:12px;color:#888">
            BioTrack v1.0 — Automated Medical Asset Monitoring<br>
            This alert was generated automatically. Do not reply to this email.
          </p>
        </body></html>
        """


# ─────────────────────────────────────────────
# 7. AUDIT LOGGER (21 CFR Part 11)
# ─────────────────────────────────────────────

class AuditLogger:
    """
    POSTs every alert to /api/audit/log regardless of cooldown.
    The audit trail must be complete — suppressed notifications still
    get logged (the cooldown only controls who gets emailed, not
    what gets recorded).
    """

    def __init__(self, api_url: str):
        self.url = api_url.rstrip("/") + AUDIT_ENDPOINT

    def log(self, alert: Alert) -> bool:
        payload = {
            **alert.to_dict(),
            "logged_at": datetime.utcnow().isoformat(),
        }
        try:
            resp = requests.post(
                self.url,
                json    = payload,
                timeout = REQUEST_TIMEOUT,
                headers = {"Content-Type": "application/json"},
            )
            if resp.ok:
                log.info(f"[AUDIT OK ] {alert.alert_id} logged to API")
                return True
            else:
                log.error(f"[AUDIT ERR] HTTP {resp.status_code} for alert {alert.alert_id}")
        except requests.exceptions.ConnectionError:
            log.warning(f"[AUDIT SKIP] API unreachable — alert {alert.alert_id} not persisted")
        except requests.exceptions.Timeout:
            log.warning(f"[AUDIT SKIP] Timeout posting alert {alert.alert_id}")
        return False


# ─────────────────────────────────────────────
# 8. THRESHOLD EVALUATOR
# ─────────────────────────────────────────────

def _unit_type(storage_unit: str) -> str:
    """Infer unit type from storage unit ID string."""
    u = storage_unit.lower()
    if "ultracold" in u or "freezer" in u:
        return "freezer"
    if "ambient" in u or "pharmacy" in u or "shelf" in u:
        return "ambient"
    return "fridge"   # default


def evaluate_thresholds(
    sensor_id:    str,
    storage_unit: str,
    temperature:  float,
    humidity:     float,
    door_open:    bool,
) -> Optional[Alert]:
    """
    Compare readings against THRESHOLDS config.
    Returns the highest-severity Alert triggered, or None if all OK.
    """
    unit_type = _unit_type(storage_unit)
    rules     = THRESHOLDS.get(unit_type, THRESHOLDS["fridge"])

    triggered_severity: Optional[SeverityLevel] = None
    alert_type = "TEMP_SPIKE"
    message    = ""

    # Check temperature thresholds — highest severity wins
    for level_name in ("EMERGENCY", "CRITICAL", "WARNING"):
        bounds = rules.get(level_name, {})
        breached = False

        if "temp_above" in bounds and temperature > bounds["temp_above"]:
            breached   = True
            alert_type = "TEMP_HIGH"
            message    = (
                f"Temperature {temperature:+.1f}°C exceeds {level_name} "
                f"threshold of {bounds['temp_above']}°C "
                f"for {storage_unit} ({unit_type})."
            )
        elif "temp_below" in bounds and temperature < bounds["temp_below"]:
            breached   = True
            alert_type = "TEMP_LOW"
            message    = (
                f"Temperature {temperature:+.1f}°C is below {level_name} "
                f"threshold of {bounds['temp_below']}°C "
                f"for {storage_unit} ({unit_type})."
            )

        if breached:
            triggered_severity = SeverityLevel(level_name)
            break   # highest severity found — stop checking lower ones

    # Check humidity (adds to existing alert or creates WARNING)
    if humidity >= HUMIDITY_CRITICAL:
        hum_sev = SeverityLevel.CRITICAL
        if triggered_severity is None or hum_sev.value > triggered_severity.value:
            triggered_severity = hum_sev
            alert_type = "HUMIDITY_HIGH"
            message    = (
                f"Humidity {humidity:.0f}% exceeds CRITICAL level "
                f"({HUMIDITY_CRITICAL}%) in {storage_unit}."
            )
    elif humidity >= HUMIDITY_WARNING:
        if triggered_severity is None:
            triggered_severity = SeverityLevel.WARNING
            alert_type = "HUMIDITY_HIGH"
            message    = (
                f"Humidity {humidity:.0f}% exceeds WARNING level "
                f"({HUMIDITY_WARNING}%) in {storage_unit}."
            )

    # Door open always adds WARNING at minimum
    if door_open and triggered_severity is None:
        triggered_severity = SeverityLevel.WARNING
        alert_type = "DOOR_OPEN"
        message    = f"Door open detected on {storage_unit}. Temperature may rise."

    if triggered_severity is None:
        return None   # all readings are within safe range

    import uuid as _uuid
    return Alert(
        alert_id     = f"ALT-{_uuid.uuid4().hex[:10].upper()}",
        sensor_id    = sensor_id,
        storage_unit = storage_unit,
        severity     = triggered_severity.value,
        alert_type   = alert_type,
        temperature  = temperature,
        humidity     = humidity,
        message      = message,
        triggered_at = datetime.utcnow().isoformat(),
        metadata     = {"unit_type": unit_type, "door_open": door_open},
    )


# ─────────────────────────────────────────────
# 9. ALERT ENGINE (main orchestrator)
# ─────────────────────────────────────────────

class AlertEngine:
    """
    Central orchestrator. Receives sensor readings, evaluates thresholds,
    applies cooldown deduplication, dispatches emails, and logs to audit API.

    Uses an internal queue + worker thread so sensor threads are never
    blocked waiting for SMTP or HTTP calls to complete.

    Usage (from sensor threads):
        alert_engine.evaluate(sensor_id, storage_unit, temp, humidity, door_open)
    """

    def __init__(self, api_url: str = API_BASE_URL):
        self._cooldown  = CooldownRegistry()
        self._email     = EmailNotifier()
        self._audit     = AuditLogger(api_url)
        self._queue: queue.Queue[Alert] = queue.Queue()
        self._stats     = {
            "total_evaluated": 0,
            "total_fired":     0,
            "suppressed":      0,
            "by_severity":     {"WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0},
        }
        self._stats_lock = threading.Lock()
        self._worker = threading.Thread(
            target=self._dispatch_loop,
            name="AlertWorker",
            daemon=True,
        )
        self._worker.start()
        log.info("Alert engine started — worker thread running")

    # ── Public API ──────────────────────────────

    def evaluate(
        self,
        sensor_id:    str,
        storage_unit: str,
        temperature:  float,
        humidity:     float,
        door_open:    bool = False,
    ) -> None:
        """
        Called by each sensor thread after every ping.
        Non-blocking — pushes to internal queue and returns immediately.
        """
        with self._stats_lock:
            self._stats["total_evaluated"] += 1

        alert = evaluate_thresholds(
            sensor_id, storage_unit, temperature, humidity, door_open
        )
        if alert is None:
            return   # healthy reading — nothing to do

        severity = SeverityLevel(alert.severity)

        # Always audit-log regardless of cooldown
        self._audit.log(alert)
        alert.audit_logged = True

        # Cooldown check — controls email dispatch only
        if not self._cooldown.should_fire(sensor_id, severity):
            with self._stats_lock:
                self._stats["suppressed"] += 1
            return

        # Queue for async dispatch (non-blocking for sensor thread)
        self._cooldown.record(sensor_id, severity)
        self._queue.put(alert)

    def fire_silent_alert(self, sensor_id: str, storage_unit: str) -> None:
        """Called by the watchdog for silent sensor failures."""
        import uuid as _uuid
        alert = Alert(
            alert_id     = f"ALT-{_uuid.uuid4().hex[:10].upper()}",
            sensor_id    = sensor_id,
            storage_unit = storage_unit,
            severity     = SeverityLevel.CRITICAL.value,
            alert_type   = "SILENT_FAILURE",
            temperature  = None,
            humidity     = None,
            message      = (
                f"Sensor {sensor_id} has stopped pinging. "
                f"Possible hardware failure on {storage_unit}. "
                f"Immediate investigation required."
            ),
            triggered_at = datetime.utcnow().isoformat(),
        )
        self._audit.log(alert)
        self._queue.put(alert)

    def print_stats(self) -> None:
        with self._stats_lock:
            s = self._stats.copy()
        log.info(
            f"ALERT STATS — evaluated:{s['total_evaluated']}  "
            f"fired:{s['total_fired']}  "
            f"suppressed:{s['suppressed']}  "
            f"by_severity:{s['by_severity']}"
        )

    # ── Worker Thread ───────────────────────────

    def _dispatch_loop(self) -> None:
        """Processes queued alerts one at a time — SMTP is synchronous."""
        while True:
            try:
                alert = self._queue.get(timeout=1)
            except queue.Empty:
                continue

            self._dispatch(alert)
            self._queue.task_done()

    def _dispatch(self, alert: Alert) -> None:
        severity = SeverityLevel(alert.severity)

        log.warning(
            f"[ALERT] {alert.severity:<9} | {alert.alert_type:<15} | "
            f"{alert.storage_unit} | {alert.message[:60]}"
        )

        # Send email
        ok = self._email.send(alert)
        if ok:
            alert.notified = True

        with self._stats_lock:
            self._stats["total_fired"] += 1
            self._stats["by_severity"][alert.severity] += 1


# ─────────────────────────────────────────────
# 10. MODULE-LEVEL SINGLETON
# ─────────────────────────────────────────────

# Import this in biotrack_virtual_sensors.py:
#   from biotrack_alert_engine import alert_engine
alert_engine = AlertEngine()


# ─────────────────────────────────────────────
# 11. SELF-TEST
# ─────────────────────────────────────────────

def run_self_test() -> None:
    """
    Fires one alert of each severity so you can verify
    email delivery and audit logging before going live.
    """
    log.info("=" * 55)
    log.info("  BioTrack Alert Engine — Self Test")
    log.info("=" * 55)

    test_cases = [
        # (sensor_id, storage_unit, temp, humidity, door_open, label)
        ("SEN-Fridge-1-00", "Fridge-1-NorthWing", 9.5,  46.0, False, "WARNING  — temp slightly high"),
        ("SEN-Fridge-2-00", "Fridge-2-NorthWing", 12.0, 47.0, False, "CRITICAL — temp breach"),
        ("SEN-Fridge-4-00", "Fridge-4-WestWing",  26.0, 48.0, False, "EMERGENCY — compressor failure"),
        ("SEN-Fridge-3-00", "Fridge-3-SouthWing", 4.2,  91.0, True,  "CRITICAL — door open + humidity"),
        ("SEN-UltraCold-00", "UltraCold-1-Lab",   -12.0, 22.0, False, "CRITICAL — freezer warming"),
    ]

    for sensor_id, unit, temp, hum, door, label in test_cases:
        log.info(f"\n[TEST] {label}")
        alert_engine.evaluate(sensor_id, unit, temp, hum, door)
        time.sleep(0.5)   # give worker thread time to process

    # Also test silent alert
    log.info("\n[TEST] CRITICAL — silent sensor failure")
    alert_engine.fire_silent_alert("SEN-Freezer-1-00", "Freezer-1-ColdRoom")

    time.sleep(2)   # wait for worker thread to drain queue
    log.info("\n")
    alert_engine.print_stats()
    log.info("\nSelf-test complete. Check logs/alerts.log for full output.")
    log.info("If SMTP_PASS is set, check your inbox for 5 test emails.")


# ─────────────────────────────────────────────
# 12. ENTRY POINT
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BioTrack Alert Engine")
    p.add_argument("--self-test", action="store_true",
                   help="Fire test alerts for all severity levels and exit")
    p.add_argument("--api-url",  type=str, default=API_BASE_URL,
                   help="BioTrack API base URL for audit logging")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.self_test:
        run_self_test()
        return

    log.info("Alert engine running. Import alert_engine and call evaluate().")
    log.info("Run with --self-test to verify email delivery.")
    try:
        while True:
            time.sleep(60)
            alert_engine.print_stats()
    except KeyboardInterrupt:
        log.info("Shutting down alert engine.")
        alert_engine.print_stats()


if __name__ == "__main__":
    main()
