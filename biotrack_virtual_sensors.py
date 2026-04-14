"""
BioTrack v1.0 — Virtual Sensor Suite (Week 3–4)
Author : Satyam W. (Python Automation Team)
Purpose: Simulate 50+ IoT temperature/humidity sensors using threads.
         Each sensor pings the BioTrack API every 30–60 seconds.
         A watchdog thread detects silent failures.

Usage:
    # Normal run — 50 sensors
    python biotrack_virtual_sensors.py

    # Custom sensor count
    python biotrack_virtual_sensors.py --sensors 100

    # Chaos Day — spike a specific fridge
    python biotrack_virtual_sensors.py --chaos Fridge-4-WestWing

    # Run for limited time (seconds), useful for testing
    python biotrack_virtual_sensors.py --duration 120

    # Point at real API
    python biotrack_virtual_sensors.py --api-url http://localhost:5000

Dependencies:
    pip install requests
"""

import argparse
import json
import logging
import random
import time
import threading
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import requests
from biotrack_alert_engine import AlertEngine


# ─────────────────────────────────────────────
# 1. LOGGING SETUP
# ─────────────────────────────────────────────

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/sensors.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("BioTrack.Sensors")


# ─────────────────────────────────────────────
# 2. CONFIGURATION
# ─────────────────────────────────────────────

API_BASE_URL      = "http://localhost:5000"
PING_ENDPOINT     = "/api/sensor/ping"
ALERT_ENDPOINT    = "/api/audit/log"
PING_INTERVAL_MIN = 30      # seconds between pings (min)
PING_INTERVAL_MAX = 60      # seconds between pings (max)
WATCHDOG_INTERVAL = 30      # how often watchdog checks heartbeats
SILENT_THRESHOLD  = 90      # seconds before a sensor is flagged as silent
REQUEST_TIMEOUT   = 10      # HTTP timeout per ping


# ─────────────────────────────────────────────
# 3. STORAGE UNITS (fridges / freezers)
# ─────────────────────────────────────────────

STORAGE_UNITS = [
    {"id": "Fridge-1-NorthWing", "nominal_temp": 4.0,  "nominal_humidity": 45.0},
    {"id": "Fridge-2-NorthWing", "nominal_temp": 4.0,  "nominal_humidity": 45.0},
    {"id": "Fridge-3-SouthWing", "nominal_temp": 4.0,  "nominal_humidity": 45.0},
    {"id": "Fridge-4-WestWing",  "nominal_temp": 4.0,  "nominal_humidity": 45.0},  # problem unit
    {"id": "Fridge-5-WestWing",  "nominal_temp": 4.0,  "nominal_humidity": 45.0},
    {"id": "Freezer-1-ColdRoom", "nominal_temp": -20.0, "nominal_humidity": 30.0},
    {"id": "Freezer-2-ColdRoom", "nominal_temp": -20.0, "nominal_humidity": 30.0},
    {"id": "UltraCold-1-Lab",    "nominal_temp": -70.0, "nominal_humidity": 20.0},
    {"id": "Ambient-1-Pharmacy", "nominal_temp": 22.0,  "nominal_humidity": 55.0},
    {"id": "Ambient-2-Pharmacy", "nominal_temp": 22.0,  "nominal_humidity": 55.0},
]


# ─────────────────────────────────────────────
# 4. FAILURE MODE ENUM
# ─────────────────────────────────────────────

class FailureMode(str, Enum):
    NORMAL    = "NORMAL"      # healthy — Gaussian noise around nominal
    SPIKE     = "SPIKE"       # temp jumps far above safe range (compressor fails)
    SILENT    = "SILENT"      # sensor stops pinging entirely (power cut / hardware dead)
    DOOR_OPEN = "DOOR_OPEN"   # temp drifts up slowly + humidity spikes (door left ajar)
    DRIFT     = "DRIFT"       # slow creep toward dangerous temp (insulation failing)


# ─────────────────────────────────────────────
# 5. PING PAYLOAD
# ─────────────────────────────────────────────

@dataclass
class SensorPing:
    """Exactly what gets POSTed to /api/sensor/ping"""
    sensor_id:    str
    storage_unit: str
    temperature:  float
    humidity:     float
    door_open:    bool
    failure_mode: str
    ping_time:    str        # ISO-8601 timestamp
    ping_id:      str        # unique per ping for dedup

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


# ─────────────────────────────────────────────
# 6. SHARED HEARTBEAT REGISTRY (thread-safe)
# ─────────────────────────────────────────────

class HeartbeatRegistry:
    """
    Tracks the last successful ping time for every sensor.
    The watchdog thread reads this; sensor threads write to it.
    Protected by a threading.Lock to prevent race conditions.
    """

    def __init__(self):
        self._lock    = threading.Lock()
        self._beats: dict[str, float] = {}   # sensor_id → epoch timestamp

    def record(self, sensor_id: str) -> None:
        with self._lock:
            self._beats[sensor_id] = time.time()

    def get_silent_sensors(self, threshold_seconds: float) -> list[str]:
        """Return sensor IDs that haven't pinged within threshold."""
        now = time.time()
        with self._lock:
            return [
                sid for sid, last in self._beats.items()
                if (now - last) > threshold_seconds
            ]

    def register(self, sensor_id: str) -> None:
        """Register a sensor at startup so watchdog knows to watch it."""
        with self._lock:
            self._beats[sensor_id] = time.time()

    def all_sensors(self) -> list[str]:
        with self._lock:
            return list(self._beats.keys())


# Singleton — shared across all threads
heartbeat_registry = HeartbeatRegistry()


# ─────────────────────────────────────────────
# 7. VIRTUAL SENSOR CLASS
# ─────────────────────────────────────────────

class VirtualSensor:
    """
    Simulates one physical IoT sensor mounted inside a storage unit.
    Runs in its own thread. Calls ping() every 30–60 seconds.

    Failure modes are either assigned at construction (for deterministic
    testing / Chaos Day) or randomly triggered mid-run to simulate
    real-world hardware degradation.
    """

    # Probability of spontaneous failure each ping cycle
    SPIKE_PROBABILITY     = 0.03   # 3%
    DOOR_OPEN_PROBABILITY = 0.05   # 5%
    DRIFT_PROBABILITY     = 0.04   # 4%

    def __init__(
        self,
        storage_unit: dict,
        api_url: str,
        failure_mode: FailureMode = FailureMode.NORMAL,
        sensor_index: int = 0,
    ):
        self.sensor_id    = f"SEN-{storage_unit['id']}-{sensor_index:02d}"
        self.unit_id      = storage_unit["id"]
        self.nominal_temp = storage_unit["nominal_temp"]
        self.nominal_hum  = storage_unit["nominal_humidity"]
        self.api_url      = api_url.rstrip("/")
        self.failure_mode = failure_mode
        self._stop_event  = threading.Event()
        self._thread      = threading.Thread(
            target=self._run_loop,
            name=self.sensor_id,
            daemon=True,        # exits when main thread exits
        )
        self._log = logging.getLogger(f"Sensor.{self.unit_id}")
        self._consecutive_failures = 0
        self._drift_offset = 0.0   # accumulates for DRIFT mode

        # Register with heartbeat registry immediately
        heartbeat_registry.register(self.sensor_id)
        

    # ── Public API ──────────────────────────────

    def start(self) -> None:
        self._thread.start()
        self._log.info(f"Started  {self.sensor_id}  mode={self.failure_mode.value}")

    def stop(self) -> None:
        """Signal the sensor thread to stop gracefully."""
        self._stop_event.set()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    # ── Core Loop ───────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():

            # SILENT mode — just sleep without pinging
            if self.failure_mode == FailureMode.SILENT:
                self._log.warning(
                    f"[SILENT] {self.sensor_id} — hardware unresponsive, skipping ping"
                )
                # Sleep in small chunks so stop_event is checked regularly
                for _ in range(PING_INTERVAL_MAX):
                    if self._stop_event.is_set():
                        return
                    time.sleep(1)
                continue

            # All other modes — generate and post a ping
            self._maybe_escalate_failure_mode()
            ping = self._generate_ping()
            self._post_ping(ping)

            # Random interval between pings (30–60 s)
            interval = random.uniform(PING_INTERVAL_MIN, PING_INTERVAL_MAX)
            for _ in range(int(interval)):
                if self._stop_event.is_set():
                    return
                time.sleep(1)

    # ── Failure Mode Escalation ─────────────────

    def _maybe_escalate_failure_mode(self) -> None:
        """
        Randomly flip a healthy sensor into a failure state.
        Mirrors real-world hardware degradation (unpredictable).
        Only escalates from NORMAL — don't override an intentional Chaos mode.
        """
        if self.failure_mode != FailureMode.NORMAL:
            return

        roll = random.random()
        if roll < self.SPIKE_PROBABILITY:
            self.failure_mode = FailureMode.SPIKE
            self._log.error(
                f"[ESCALATE] {self.sensor_id} → SPIKE (compressor failure simulated)"
            )
        elif roll < self.SPIKE_PROBABILITY + self.DOOR_OPEN_PROBABILITY:
            self.failure_mode = FailureMode.DOOR_OPEN
            self._log.warning(
                f"[ESCALATE] {self.sensor_id} → DOOR_OPEN"
            )
        elif roll < self.SPIKE_PROBABILITY + self.DOOR_OPEN_PROBABILITY + self.DRIFT_PROBABILITY:
            self.failure_mode = FailureMode.DRIFT
            self._log.warning(
                f"[ESCALATE] {self.sensor_id} → DRIFT (insulation degrading)"
            )

    # ── Reading Generation ──────────────────────

    def _generate_ping(self) -> SensorPing:
        temp, humidity, door_open = self._readings_for_mode()
        return SensorPing(
            sensor_id    = self.sensor_id,
            storage_unit = self.unit_id,
            temperature  = round(temp, 2),
            humidity     = round(humidity, 2),
            door_open    = door_open,
            failure_mode = self.failure_mode.value,
            ping_time    = datetime.utcnow().isoformat(),
            ping_id      = uuid.uuid4().hex,
        )

    def _readings_for_mode(self) -> tuple[float, float, bool]:
        t = self.nominal_temp
        h = self.nominal_hum

        if self.failure_mode == FailureMode.NORMAL:
            # Gaussian noise — realistic sensor jitter
            temp     = random.gauss(t, 0.3)
            humidity = random.gauss(h, 2.0)
            door     = False

        elif self.failure_mode == FailureMode.SPIKE:
            # Sharp jump — compressor dead, ambient temp flooding in
            spike    = random.uniform(15, 30)   # +15°C to +30°C above nominal
            temp     = t + spike
            humidity = random.gauss(h, 5.0)
            door     = False

        elif self.failure_mode == FailureMode.DOOR_OPEN:
            # Gradual drift up + humidity spike (moist outside air entering)
            temp     = t + random.uniform(2, 8)
            humidity = min(95.0, h + random.uniform(20, 40))
            door     = True

        elif self.failure_mode == FailureMode.DRIFT:
            # Slow, insidious creep — hardest to catch without trend analysis
            self._drift_offset += random.uniform(0.1, 0.4)
            temp     = t + self._drift_offset
            humidity = random.gauss(h, 3.0)
            door     = False

        else:  # fallback
            temp     = random.gauss(t, 0.3)
            humidity = random.gauss(h, 2.0)
            door     = False

        return temp, humidity, door

    # ── HTTP ────────────────────────────────────

    def _post_ping(self, ping: SensorPing) -> None:
        url = self.api_url + PING_ENDPOINT
        try:
            t0   = time.perf_counter()
            resp = requests.post(
                url,
                data    = ping.to_json(),
                headers = {"Content-Type": "application/json"},
                timeout = REQUEST_TIMEOUT,
            )
            ms = (time.perf_counter() - t0) * 1000

            if resp.ok:
                heartbeat_registry.record(self.sensor_id)
                self._consecutive_failures = 0
                self._log.info(
                    f"[PING OK ] {self.sensor_id} "
                    f"temp={ping.temperature:+.1f}°C  "
                    f"hum={ping.humidity:.0f}%  "
                    f"mode={ping.failure_mode:<9}  "
                    f"{ms:.0f}ms"
                )
            else:
                self._on_http_error(resp.status_code, ping)

        except requests.exceptions.ConnectionError:
            self._on_connection_error(ping)
        except requests.exceptions.Timeout:
            self._log.warning(f"[TIMEOUT ] {self.sensor_id} — API did not respond in {REQUEST_TIMEOUT}s")
            self._consecutive_failures += 1
            alert_engine.evaluate(
                sensor_id    = self.sensor_id,
                storage_unit = self.unit_id,
                temperature  = ping.temperature,
                humidity     = ping.humidity,
                door_open    = ping.door_open,
            )
    def _on_http_error(self, status: int, ping: SensorPing) -> None:
        self._consecutive_failures += 1
        self._log.error(
            f"[HTTP ERR] {self.sensor_id} — status {status}  "
            f"(consecutive failures: {self._consecutive_failures})"
        )

    def _on_connection_error(self, ping: SensorPing) -> None:
        self._consecutive_failures += 1
        # Only log first failure + every 5th to avoid log spam
        if self._consecutive_failures == 1 or self._consecutive_failures % 5 == 0:
            self._log.warning(
                f"[NO CONN ] {self.sensor_id} — API unreachable "
                f"(attempt {self._consecutive_failures}) — running in dry-run mode"
            )
        # Still record heartbeat — the sensor is alive even if API isn't
        heartbeat_registry.record(self.sensor_id)


# ─────────────────────────────────────────────
# 8. WATCHDOG THREAD
# ─────────────────────────────────────────────

class SensorWatchdog:
    """
    Runs in its own thread. Every WATCHDOG_INTERVAL seconds it checks
    the heartbeat registry. Any sensor silent for > SILENT_THRESHOLD
    seconds triggers an investigator alert.

    This is the 21 CFR Part 11 "silent failure" detector mentioned
    in the BioTrack charter.
    """

    def __init__(self, api_url: str):
        self.api_url     = api_url.rstrip("/")
        self._stop_event = threading.Event()
        self._alerted: set[str] = set()    # avoid repeat alerts
        self._thread = threading.Thread(
            target=self._run_loop,
            name="Watchdog",
            daemon=True,
        )
        self._log = logging.getLogger("Watchdog")

    def start(self) -> None:
        self._thread.start()
        self._log.info("Watchdog started — checking every %ds", WATCHDOG_INTERVAL)

    def stop(self) -> None:
        self._stop_event.set()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=WATCHDOG_INTERVAL)
            if self._stop_event.is_set():
                break
            self._check_heartbeats()

    def _check_heartbeats(self) -> None:
        silent = heartbeat_registry.get_silent_sensors(SILENT_THRESHOLD)
        for sensor_id in silent:
            if sensor_id not in self._alerted:
                self._log.critical(
                    f"[SILENT ALERT] {sensor_id} — no ping for >{SILENT_THRESHOLD}s. "
                    f"Possible hardware failure. Triggering investigator alert."
                )
                self._post_alert(sensor_id)
                self._alerted.add(sensor_id)

        # Clear alert state if sensor comes back online
        recovered = self._alerted - set(silent)
        for sensor_id in recovered:
            self._log.info(f"[RECOVERED] {sensor_id} — back online, clearing alert")
            self._alerted.discard(sensor_id)

    def _post_alert(self, sensor_id: str) -> None:
        """POST a silent-failure alert to the audit log endpoint."""
        url     = self.api_url + ALERT_ENDPOINT
        payload = {
            "alert_type":    "SILENT_FAILURE",
            "sensor_id":     sensor_id,
            "severity":      "CRITICAL",
            "triggered_at":  datetime.utcnow().isoformat(),
            "message":       f"Sensor {sensor_id} has not pinged in over {SILENT_THRESHOLD} seconds.",
        }
        try:
            resp = requests.post(
                url,
                json    = payload,
                timeout = REQUEST_TIMEOUT,
                headers = {"Content-Type": "application/json"},
            )
            if resp.ok:
                self._log.info(f"[ALERT SENT] Investigator alerted for {sensor_id}")
            else:
                self._log.error(f"[ALERT FAIL] HTTP {resp.status_code} posting alert for {sensor_id}")
        except requests.exceptions.RequestException as e:
            self._log.error(f"[ALERT FAIL] Could not reach API: {e}")


# ─────────────────────────────────────────────
# 9. SENSOR MANAGER
# ─────────────────────────────────────────────

class SensorManager:
    """
    Spawns and manages all sensor threads + the watchdog.
    One sensor per storage unit slot (multiple sensors per unit
    if --sensors > len(STORAGE_UNITS)).
    """

    def __init__(self, total_sensors: int, api_url: str, chaos_unit: Optional[str] = None):
        self.api_url      = api_url
        self.chaos_unit   = chaos_unit
        self.sensors: list[VirtualSensor] = []
        self.watchdog     = SensorWatchdog(api_url)
        self._build_sensors(total_sensors)

    def _build_sensors(self, total: int) -> None:
        units = STORAGE_UNITS * (total // len(STORAGE_UNITS) + 1)  # repeat if needed
        for i in range(total):
            unit   = units[i % len(STORAGE_UNITS)]
            mode   = self._assign_mode(unit["id"])
            sensor = VirtualSensor(
                storage_unit  = unit,
                api_url       = self.api_url,
                failure_mode  = mode,
                sensor_index  = i,
            )
            self.sensors.append(sensor)

    def _assign_mode(self, unit_id: str) -> FailureMode:
        """Chaos Day: force SPIKE on the target unit; rest start NORMAL."""
        if self.chaos_unit and unit_id == self.chaos_unit:
            log.warning(f"[CHAOS DAY] Forcing SPIKE on {unit_id}")
            return FailureMode.SPIKE
        return FailureMode.NORMAL

    def start_all(self) -> None:
        log.info(f"Starting {len(self.sensors)} sensors + watchdog ...")
        self.watchdog.start()
        for sensor in self.sensors:
            sensor.start()
            time.sleep(0.05)    # stagger starts — avoids thundering herd on API
        log.info("All sensors running. Press Ctrl+C to stop.")

    def stop_all(self) -> None:
        log.info("Stopping all sensors ...")
        self.watchdog.stop()
        for sensor in self.sensors:
            sensor.stop()
        log.info("All sensors stopped.")

    def status(self) -> dict:
        mode_counts: dict[str, int] = {}
        for s in self.sensors:
            m = s.failure_mode.value
            mode_counts[m] = mode_counts.get(m, 0) + 1
        return {
            "total_sensors":   len(self.sensors),
            "alive":           sum(1 for s in self.sensors if s.is_alive()),
            "by_mode":         mode_counts,
            "silent_detected": len(heartbeat_registry.get_silent_sensors(SILENT_THRESHOLD)),
        }

    def print_status(self) -> None:
        s = self.status()
        log.info(
            f"STATUS — total:{s['total_sensors']}  "
            f"alive:{s['alive']}  "
            f"silent:{s['silent_detected']}  "
            f"modes:{s['by_mode']}"
        )


# ─────────────────────────────────────────────
# 10. ENTRY POINT
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BioTrack Virtual Sensor Suite")
    p.add_argument("--sensors",  type=int, default=50,
                   help="Number of concurrent sensor threads (default: 50)")
    p.add_argument("--api-url",  type=str, default=API_BASE_URL,
                   help="BioTrack API base URL")
    p.add_argument("--chaos",    type=str, default=None, metavar="UNIT_ID",
                   help="Chaos Day: force SPIKE failure on this storage unit ID")
    p.add_argument("--duration", type=int, default=None, metavar="SECONDS",
                   help="Auto-stop after N seconds (default: run until Ctrl+C)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    log.info("=" * 60)
    log.info("  BioTrack Virtual Sensor Suite — Satyam W.")
    log.info(f"  Sensors   : {args.sensors}")
    log.info(f"  API URL   : {args.api_url}")
    log.info(f"  Chaos unit: {args.chaos or 'none'}")
    log.info(f"  Duration  : {args.duration or 'until Ctrl+C'}")
    log.info("=" * 60)

    manager = SensorManager(
        total_sensors = args.sensors,
        api_url       = args.api_url,
        chaos_unit    = args.chaos,
    )
    manager.start_all()

    try:
        elapsed = 0
        while True:
            time.sleep(60)
            elapsed += 60
            manager.print_status()
            if args.duration and elapsed >= args.duration:
                log.info(f"Duration {args.duration}s reached — shutting down.")
                break

    except KeyboardInterrupt:
        log.info("Ctrl+C received — shutting down gracefully ...")

    finally:
        manager.stop_all()
        log.info("Final status:")
        manager.print_status()


if __name__ == "__main__":
    main()
