"""
BioTrack v1.0 — Stress Test & Chaos Day Runner (Week 7–8)
Author : Satyam W. (Python Automation Team)
Purpose: Simulate 100 concurrent sensors, log per-ping latency to CSV,
         measure server concurrency limits, and run "Chaos Day" emergency.

Usage:
    # Standard stress test — 100 sensors, 5 minutes
    python biotrack_stress_test.py

    # Custom sensor count and duration
    python biotrack_stress_test.py --sensors 150 --duration 300

    # Chaos Day — spike Fridge-4-WestWing at t=60s
    python biotrack_stress_test.py --chaos --chaos-unit Fridge-4-WestWing --chaos-delay 60

    # Point at real API
    python biotrack_stress_test.py --api-url http://localhost:5000

    # Quick smoke test (10 sensors, 30 seconds)
    python biotrack_stress_test.py --sensors 10 --duration 30

Outputs (in stress_test_output/):
    ping_latency.csv        — every single ping with latency, status, sensor
    chaos_timeline.csv      — chaos event timestamps and detection times
    stress_summary.txt      — human-readable final report
    logs/stress_test.log    — full debug log
"""

import argparse
import csv
import json
import logging
import os
import random
import statistics
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import requests


# 1. OUTPUT DIRECTORIES + LOGGING


OUT_DIR = Path("stress_test_output")
OUT_DIR.mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/stress_test.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("BioTrack.StressTest")



# 2. CONFIGURATION


API_BASE_URL      = "http://localhost:5000"
PING_ENDPOINT     = "/api/sensor/ping"
ALERT_ENDPOINT    = "/api/audit/log"
REQUEST_TIMEOUT   = 10          # seconds per HTTP request
PING_INTERVAL_MIN = 5           # shorter intervals for stress test
PING_INTERVAL_MAX = 15          # (real sensors: 30–60s; test: 5–15s)
STAGGER_DELAY     = 0.05        # seconds between thread starts (avoid thundering herd)

# Chaos Day config
CHAOS_TEMP        = 26.0        # °C — triggers EMERGENCY threshold
CHAOS_HUMIDITY    = 88.0        # %
CHAOS_DURATION    = 120         # seconds the chaos condition persists

# CSV field names
LATENCY_CSV_FIELDS = [
    "ping_id", "sensor_id", "storage_unit", "ping_time",
    "temperature", "humidity", "door_open", "failure_mode",
    "http_status", "latency_ms", "success", "error",
]
CHAOS_CSV_FIELDS = [
    "event", "sensor_id", "storage_unit", "triggered_at",
    "detected_at", "detection_latency_s", "temperature", "notes",
]


# ─────────────────────────────────────────────
# 3. STORAGE UNITS
# ─────────────────────────────────────────────

STORAGE_UNITS = [
    {"id": "Fridge-1-NorthWing",  "nominal_temp":  4.0, "nominal_hum": 45.0},
    {"id": "Fridge-2-NorthWing",  "nominal_temp":  4.0, "nominal_hum": 45.0},
    {"id": "Fridge-3-SouthWing",  "nominal_temp":  4.0, "nominal_hum": 45.0},
    {"id": "Fridge-4-WestWing",   "nominal_temp":  4.0, "nominal_hum": 45.0},
    {"id": "Fridge-5-WestWing",   "nominal_temp":  4.0, "nominal_hum": 45.0},
    {"id": "Freezer-1-ColdRoom",  "nominal_temp": -20.0, "nominal_hum": 30.0},
    {"id": "Freezer-2-ColdRoom",  "nominal_temp": -20.0, "nominal_hum": 30.0},
    {"id": "UltraCold-1-Lab",     "nominal_temp": -70.0, "nominal_hum": 20.0},
    {"id": "Ambient-1-Pharmacy",  "nominal_temp":  22.0, "nominal_hum": 55.0},
    {"id": "Ambient-2-Pharmacy",  "nominal_temp":  22.0, "nominal_hum": 55.0},
]


# ─────────────────────────────────────────────
# 4. FAILURE MODE
# ─────────────────────────────────────────────

class FailureMode(str, Enum):
    NORMAL    = "NORMAL"
    SPIKE     = "SPIKE"
    DOOR_OPEN = "DOOR_OPEN"
    DRIFT     = "DRIFT"
    CHAOS     = "CHAOS"       # forced emergency for Chaos Day


# ─────────────────────────────────────────────
# 5. THREAD-SAFE RESULTS COLLECTOR
# ─────────────────────────────────────────────

@dataclass
class PingResult:
    """One recorded ping — written to CSV."""
    ping_id:      str
    sensor_id:    str
    storage_unit: str
    ping_time:    str
    temperature:  float
    humidity:     float
    door_open:    bool
    failure_mode: str
    http_status:  int           # 0 = connection error
    latency_ms:   float
    success:      bool
    error:        str = ""

    def to_row(self) -> dict:
        return asdict(self)


@dataclass
class ChaosEvent:
    """One chaos detection record — written to chaos_timeline.csv."""
    event:               str    # TRIGGERED or DETECTED
    sensor_id:           str
    storage_unit:        str
    triggered_at:        str
    detected_at:         str    = ""
    detection_latency_s: float  = -1.0
    temperature:         float  = 0.0
    notes:               str    = ""

    def to_row(self) -> dict:
        return asdict(self)


class ResultsCollector:
    """
    Thread-safe store for all ping results.
    Sensor threads write; the reporter thread reads.
    """

    def __init__(self):
        self._lock    = threading.Lock()
        self._results: list[PingResult] = []
        self._chaos:   list[ChaosEvent] = []

    def add_ping(self, result: PingResult) -> None:
        with self._lock:
            self._results.append(result)

    def add_chaos_event(self, event: ChaosEvent) -> None:
        with self._lock:
            self._chaos.append(event)

    def snapshot(self) -> tuple[list[PingResult], list[ChaosEvent]]:
        with self._lock:
            return list(self._results), list(self._chaos)

    def total_pings(self) -> int:
        with self._lock:
            return len(self._results)

    def successful_pings(self) -> int:
        with self._lock:
            return sum(1 for r in self._results if r.success)


# ─────────────────────────────────────────────
# 6. CHAOS DAY COORDINATOR
# ─────────────────────────────────────────────

class ChaosDayCoordinator:
    """
    At chaos_delay seconds into the test, secretly signals a target
    storage unit to start emitting EMERGENCY readings.

    The Chaos Day protocol (from the BioTrack charter):
    - Trigger is kept secret from Frontend + Alert teams
    - We measure how long until the alert/dashboard catches it
    - Target: detection within 60 seconds end-to-end
    """

    def __init__(
        self,
        chaos_unit:  str,
        chaos_delay: int,
        collector:   ResultsCollector,
    ):
        self.chaos_unit      = chaos_unit
        self.chaos_delay     = chaos_delay
        self.collector       = collector
        self._active         = threading.Event()
        self._triggered_at:  Optional[float]  = None
        self._triggered_iso: Optional[str]    = None
        self._detected_at:   Optional[float]  = None
        self._lock           = threading.Lock()
        self._timer: Optional[threading.Timer] = None

    def start(self) -> None:
        """Schedule chaos trigger after chaos_delay seconds."""
        self._timer = threading.Timer(self.chaos_delay, self._trigger_chaos)
        self._timer.daemon = True
        self._timer.start()
        log.info(
            f"[CHAOS] Chaos Day armed — "
            f"will spike {self.chaos_unit} in {self.chaos_delay}s"
        )

    def stop(self) -> None:
        if self._timer:
            self._timer.cancel()

    def is_active(self, unit_id: str) -> bool:
        return self._active.is_set() and unit_id == self.chaos_unit

    def record_detection(self, sensor_id: str) -> None:
        """Called when alert engine / dashboard acknowledges the chaos reading."""
        with self._lock:
            if self._detected_at is None and self._triggered_at is not None:
                self._detected_at = time.time()
                latency = self._detected_at - self._triggered_at
                log.critical(
                    f"[CHAOS DETECTED] Alert caught by system in "
                    f"{latency:.1f}s  (target: <60s)  "
                    f"sensor={sensor_id}"
                )
                event = ChaosEvent(
                    event               = "DETECTED",
                    sensor_id           = sensor_id,
                    storage_unit        = self.chaos_unit,
                    triggered_at        = self._triggered_iso or "",
                    detected_at         = datetime.utcnow().isoformat(),
                    detection_latency_s = round(latency, 2),
                    temperature         = CHAOS_TEMP,
                    notes               = f"Detection latency: {latency:.1f}s",
                )
                self.collector.add_chaos_event(event)

    def detection_latency(self) -> Optional[float]:
        with self._lock:
            if self._triggered_at and self._detected_at:
                return round(self._detected_at - self._triggered_at, 2)
        return None

    def _trigger_chaos(self) -> None:
        self._active.set()
        self._triggered_at  = time.time()
        self._triggered_iso = datetime.utcnow().isoformat()
        log.critical(
            f"\n{'='*55}\n"
            f"  *** CHAOS DAY TRIGGERED ***\n"
            f"  Unit     : {self.chaos_unit}\n"
            f"  Temp     : {CHAOS_TEMP}°C (EMERGENCY threshold)\n"
            f"  Time     : {self._triggered_iso}\n"
            f"  Target   : detected within 60 seconds\n"
            f"{'='*55}"
        )
        event = ChaosEvent(
            event        = "TRIGGERED",
            sensor_id    = f"SEN-{self.chaos_unit}-chaos",
            storage_unit = self.chaos_unit,
            triggered_at = self._triggered_iso,
            temperature  = CHAOS_TEMP,
            notes        = "Chaos Day EMERGENCY simulation started",
        )
        self.collector.add_chaos_event(event)

        # Auto-deactivate after CHAOS_DURATION seconds
        threading.Timer(CHAOS_DURATION, self._end_chaos).start()

    def _end_chaos(self) -> None:
        self._active.clear()
        log.info(f"[CHAOS] Chaos condition ended on {self.chaos_unit} — returning to NORMAL")


# ─────────────────────────────────────────────
# 7. STRESS SENSOR THREAD
# ─────────────────────────────────────────────

class StressSensor:
    """
    One sensor thread for the stress test.
    Pings more frequently than the real sensor suite (5–15s vs 30–60s)
    to generate enough load to stress the server concurrency.
    Records every ping result — latency, status code, errors — to the collector.
    """

    def __init__(
        self,
        unit:        dict,
        sensor_index: int,
        api_url:     str,
        collector:   ResultsCollector,
        chaos:       Optional[ChaosDayCoordinator] = None,
    ):
        self.sensor_id   = f"STR-{unit['id']}-{sensor_index:03d}"
        self.unit_id     = unit["id"]
        self.nominal_t   = unit["nominal_temp"]
        self.nominal_h   = unit["nominal_hum"]
        self.api_url     = api_url.rstrip("/")
        self.collector   = collector
        self.chaos       = chaos
        self._stop       = threading.Event()
        self._drift_off  = 0.0
        self._thread     = threading.Thread(
            target=self._run,
            name=self.sensor_id,
            daemon=True,
        )
        self._log = logging.getLogger(f"Stress.{unit['id'][:12]}")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._ping_once()
            interval = random.uniform(PING_INTERVAL_MIN, PING_INTERVAL_MAX)
            # Sleep in 1s chunks — responds quickly to stop signal
            for _ in range(int(interval)):
                if self._stop.is_set():
                    return
                time.sleep(1)

    def _ping_once(self) -> None:
        temp, humidity, door, mode = self._generate_readings()
        payload = {
            "sensor_id":    self.sensor_id,
            "storage_unit": self.unit_id,
            "temperature":  round(temp, 2),
            "humidity":     round(humidity, 2),
            "door_open":    door,
            "failure_mode": mode.value,
            "ping_time":    datetime.utcnow().isoformat(),
            "ping_id":      uuid.uuid4().hex,
        }

        t0         = time.perf_counter()
        http_status = 0
        success    = False
        error_msg  = ""

        try:
            resp = requests.post(
                self.api_url + PING_ENDPOINT,
                json    = payload,
                timeout = REQUEST_TIMEOUT,
                headers = {"Content-Type": "application/json"},
            )
            http_status = resp.status_code
            success     = resp.ok

            # If chaos is active and this is the chaos unit,
            # a successful 2xx means the server saw the EMERGENCY reading
            if (
                success
                and self.chaos
                and self.chaos.is_active(self.unit_id)
                and mode == FailureMode.CHAOS
            ):
                self.chaos.record_detection(self.sensor_id)

        except requests.exceptions.ConnectionError as e:
            error_msg = f"ConnectionError: {e}"
        except requests.exceptions.Timeout:
            error_msg = f"Timeout after {REQUEST_TIMEOUT}s"
        except requests.exceptions.RequestException as e:
            error_msg = f"RequestException: {e}"

        latency_ms = (time.perf_counter() - t0) * 1000

        result = PingResult(
            ping_id      = payload["ping_id"],
            sensor_id    = self.sensor_id,
            storage_unit = self.unit_id,
            ping_time    = payload["ping_time"],
            temperature  = payload["temperature"],
            humidity     = payload["humidity"],
            door_open    = payload["door_open"],
            failure_mode = payload["failure_mode"],
            http_status  = http_status,
            latency_ms   = round(latency_ms, 2),
            success      = success,
            error        = error_msg,
        )
        self.collector.add_ping(result)

        level = "INFO" if success else "WARNING"
        getattr(self._log, level.lower())(
            f"{self.sensor_id[-16:]:<18} "
            f"temp={temp:+6.1f}°C  "
            f"mode={mode.value:<9}  "
            f"HTTP {http_status or 'ERR'}  "
            f"{latency_ms:>6.0f}ms"
        )

    def _generate_readings(self) -> tuple[float, float, bool, FailureMode]:
        # Chaos Day override takes absolute priority
        if self.chaos and self.chaos.is_active(self.unit_id):
            return CHAOS_TEMP, CHAOS_HUMIDITY, False, FailureMode.CHAOS

        # Small random chance of spontaneous failure
        roll = random.random()
        if roll < 0.03:
            spike = random.uniform(15, 30)
            return self.nominal_t + spike, self.nominal_h, False, FailureMode.SPIKE
        elif roll < 0.07:
            return self.nominal_t + random.uniform(2, 6), min(95, self.nominal_h + 30), True, FailureMode.DOOR_OPEN
        elif roll < 0.10:
            self._drift_off += random.uniform(0.1, 0.3)
            return self.nominal_t + self._drift_off, self.nominal_h, False, FailureMode.DRIFT

        # Normal — Gaussian jitter around nominal
        return (
            random.gauss(self.nominal_t, 0.3),
            random.gauss(self.nominal_h, 2.0),
            False,
            FailureMode.NORMAL,
        )


# ─────────────────────────────────────────────
# 8. LIVE STATS REPORTER (prints every 30s)
# ─────────────────────────────────────────────

class LiveReporter:
    """
    Background thread — prints a live dashboard line every 30 seconds.
    Gives you real-time visibility during the stress test without
    scrolling through individual ping logs.
    """

    def __init__(self, collector: ResultsCollector, total_sensors: int):
        self.collector     = collector
        self.total_sensors = total_sensors
        self._stop         = threading.Event()
        self._start_time   = time.time()
        self._thread       = threading.Thread(
            target=self._run, name="LiveReporter", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(timeout=30)
            self._print_line()

    def _print_line(self) -> None:
        results, _ = self.collector.snapshot()
        if not results:
            return

        elapsed     = time.time() - self._start_time
        total       = len(results)
        successes   = sum(1 for r in results if r.success)
        errors      = total - successes
        latencies   = [r.latency_ms for r in results if r.success]
        avg_lat     = statistics.mean(latencies) if latencies else 0
        p95_lat     = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
        throughput  = total / elapsed if elapsed > 0 else 0

        log.info(
            f"\n{'─'*60}\n"
            f"  LIVE  t={elapsed:.0f}s  sensors={self.total_sensors}\n"
            f"  Pings   : {total:,}  success={successes:,}  errors={errors:,}\n"
            f"  Latency : avg={avg_lat:.0f}ms  p95={p95_lat:.0f}ms\n"
            f"  Throughput: {throughput:.1f} pings/sec\n"
            f"{'─'*60}"
        )


# ─────────────────────────────────────────────
# 9. CSV WRITER
# ─────────────────────────────────────────────

def write_latency_csv(results: list[PingResult], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LATENCY_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(r.to_row() for r in results)
    log.info(f"[CSV] Latency data → {path}  ({len(results):,} rows)")


def write_chaos_csv(events: list[ChaosEvent], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CHAOS_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(e.to_row() for e in events)
    log.info(f"[CSV] Chaos timeline → {path}  ({len(events)} events)")


# ─────────────────────────────────────────────
# 10. FINAL REPORT
# ─────────────────────────────────────────────

def build_report(
    results:       list[PingResult],
    chaos_events:  list[ChaosEvent],
    total_sensors: int,
    duration_s:    float,
    chaos_unit:    Optional[str],
) -> str:
    total      = len(results)
    successes  = sum(1 for r in results if r.success)
    errors     = total - successes
    error_rate = (errors / total * 100) if total else 0
    throughput = total / duration_s if duration_s > 0 else 0

    latencies = [r.latency_ms for r in results if r.success]
    if latencies:
        s_lat    = sorted(latencies)
        avg_lat  = statistics.mean(latencies)
        med_lat  = statistics.median(latencies)
        p95_lat  = s_lat[int(len(s_lat) * 0.95)]
        p99_lat  = s_lat[int(len(s_lat) * 0.99)]
        max_lat  = max(latencies)
        min_lat  = min(latencies)
        stdev    = statistics.stdev(latencies) if len(latencies) > 1 else 0
    else:
        avg_lat = med_lat = p95_lat = p99_lat = max_lat = min_lat = stdev = 0

    # Error breakdown by type
    error_types: dict[str, int] = defaultdict(int)
    for r in results:
        if not r.success:
            key = r.error.split(":")[0] if r.error else f"HTTP {r.http_status}"
            error_types[key] += 1

    # Mode breakdown
    mode_counts: dict[str, int] = defaultdict(int)
    for r in results:
        mode_counts[r.failure_mode] += 1

    # Chaos result
    detection_event = next(
        (e for e in chaos_events if e.event == "DETECTED"), None
    )
    chaos_result = "N/A (no chaos run)"
    if chaos_unit:
        if detection_event:
            lat = detection_event.detection_latency_s
            verdict = "PASS ✓" if lat <= 60 else "FAIL ✗"
            chaos_result = (
                f"{verdict}  detection latency = {lat:.1f}s  "
                f"(target: <60s)"
            )
        else:
            chaos_result = "NOT DETECTED within test window ✗"

    report = f"""
{'='*60}
  BIOTRACK STRESS TEST — FINAL REPORT
  Satyam W. | Python Automation Team
{'='*60}

RUN PARAMETERS
  Sensors         : {total_sensors}
  Duration        : {duration_s:.0f}s ({duration_s/60:.1f} min)
  Ping interval   : {PING_INTERVAL_MIN}–{PING_INTERVAL_MAX}s per sensor
  Chaos unit      : {chaos_unit or 'none'}
  API URL         : {API_BASE_URL}

THROUGHPUT
  Total pings     : {total:,}
  Successful      : {successes:,}
  Errors          : {errors:,}
  Error rate      : {error_rate:.2f}%
  Pings / second  : {throughput:.2f}

LATENCY (ms)  [successful pings only]
  Min             : {min_lat:.1f}
  Average         : {avg_lat:.1f}
  Median          : {med_lat:.1f}
  Std dev         : {stdev:.1f}
  p95             : {p95_lat:.1f}
  p99             : {p99_lat:.1f}
  Max             : {max_lat:.1f}

FAILURE MODE DISTRIBUTION
"""
    for mode, count in sorted(mode_counts.items()):
        pct = count / total * 100 if total else 0
        bar = "█" * int(pct / 2)
        report += f"  {mode:<12} : {count:>5}  ({pct:5.1f}%)  {bar}\n"

    if error_types:
        report += "\nERROR BREAKDOWN\n"
        for etype, count in sorted(error_types.items(), key=lambda x: -x[1]):
            report += f"  {etype:<30} : {count}\n"

    report += f"""
CHAOS DAY RESULT
  {chaos_result}

VERDICT
"""
    checks = [
        ("Error rate < 5%",        error_rate < 5),
        ("p95 latency < 500ms",    p95_lat < 500),
        ("p99 latency < 1000ms",   p99_lat < 1000),
        ("Zero connection errors",  error_types.get("ConnectionError", 0) == 0),
    ]
    if chaos_unit:
        detected = detection_event is not None
        fast     = detection_event and detection_event.detection_latency_s <= 60
        checks.append(("Chaos detected",           detected))
        checks.append(("Detection within 60s",     bool(fast)))

    all_pass = all(v for _, v in checks)
    for label, passed in checks:
        icon = "✓" if passed else "✗"
        report += f"  [{icon}] {label}\n"

    report += f"\n  OVERALL: {'PASS ✓' if all_pass else 'FAIL ✗'}\n"
    report += f"\n{'='*60}\n"
    report += f"  Generated: {datetime.utcnow().isoformat()}\n"
    report += f"{'='*60}\n"

    return report


# ─────────────────────────────────────────────
# 11. STRESS TEST RUNNER
# ─────────────────────────────────────────────

class StressTestRunner:

    def __init__(
        self,
        total_sensors: int,
        duration_s:    int,
        api_url:       str,
        chaos_unit:    Optional[str] = None,
        chaos_delay:   int           = 60,
    ):
        self.total_sensors = total_sensors
        self.duration_s    = duration_s
        self.api_url       = api_url
        self.chaos_unit    = chaos_unit
        self.chaos_delay   = chaos_delay
        self.collector     = ResultsCollector()
        self.sensors:      list[StressSensor] = []
        self.chaos_coord:  Optional[ChaosDayCoordinator] = None
        self.reporter      = LiveReporter(self.collector, total_sensors)

    def run(self) -> None:
        log.info("=" * 60)
        log.info("  BioTrack Stress Test — Satyam W.")
        log.info(f"  Sensors   : {self.total_sensors}")
        log.info(f"  Duration  : {self.duration_s}s")
        log.info(f"  API URL   : {self.api_url}")
        log.info(f"  Chaos unit: {self.chaos_unit or 'disabled'}")
        log.info("=" * 60)

        # Arm chaos coordinator if requested
        if self.chaos_unit:
            self.chaos_coord = ChaosDayCoordinator(
                self.chaos_unit, self.chaos_delay, self.collector
            )

        # Build sensor threads
        units = STORAGE_UNITS * (self.total_sensors // len(STORAGE_UNITS) + 1)
        for i in range(self.total_sensors):
            unit   = units[i % len(STORAGE_UNITS)]
            sensor = StressSensor(
                unit          = unit,
                sensor_index  = i,
                api_url       = self.api_url,
                collector     = self.collector,
                chaos         = self.chaos_coord,
            )
            self.sensors.append(sensor)

        # Start everything
        self.reporter.start()
        if self.chaos_coord:
            self.chaos_coord.start()

        log.info(f"Launching {self.total_sensors} sensor threads ...")
        t_start = time.time()
        for sensor in self.sensors:
            sensor.start()
            time.sleep(STAGGER_DELAY)

        alive = sum(1 for s in self.sensors if s.is_alive())
        log.info(f"All {alive} sensors running. Test duration: {self.duration_s}s")

        # Wait for duration
        try:
            time.sleep(self.duration_s)
        except KeyboardInterrupt:
            log.info("Ctrl+C — stopping test early ...")

        actual_duration = time.time() - t_start

        # Stop everything
        log.info("Stopping sensors ...")
        self.reporter.stop()
        if self.chaos_coord:
            self.chaos_coord.stop()
        for sensor in self.sensors:
            sensor.stop()

        # Give threads 3s to finish in-flight requests
        time.sleep(3)

        self._write_outputs(actual_duration)

    def _write_outputs(self, duration_s: float) -> None:
        results, chaos_events = self.collector.snapshot()

        # Write CSVs
        write_latency_csv(results, OUT_DIR / "ping_latency.csv")
        if chaos_events:
            write_chaos_csv(chaos_events, OUT_DIR / "chaos_timeline.csv")

        # Build and write report
        report = build_report(
            results       = results,
            chaos_events  = chaos_events,
            total_sensors = self.total_sensors,
            duration_s    = duration_s,
            chaos_unit    = self.chaos_unit,
        )
        report_path = OUT_DIR / "stress_summary.txt"
        report_path.write_text(report, encoding="utf-8")

        print(report)
        log.info(f"[REPORT] Full report → {report_path}")
        log.info(f"[CSV]    Latency data → {OUT_DIR / 'ping_latency.csv'}")


# ─────────────────────────────────────────────
# 12. ENTRY POINT
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BioTrack Stress Test & Chaos Day Runner (Satyam W.)"
    )
    p.add_argument("--sensors",     type=int, default=100,
                   help="Number of concurrent sensor threads (default: 100)")
    p.add_argument("--duration",    type=int, default=300,
                   help="Test duration in seconds (default: 300 = 5 min)")
    p.add_argument("--api-url",     type=str, default=API_BASE_URL,
                   help="BioTrack API base URL")
    p.add_argument("--chaos",       action="store_true",
                   help="Enable Chaos Day — trigger EMERGENCY on --chaos-unit")
    p.add_argument("--chaos-unit",  type=str, default="Fridge-4-WestWing",
                   help="Storage unit ID to spike during Chaos Day")
    p.add_argument("--chaos-delay", type=int, default=60,
                   help="Seconds into the test before chaos triggers (default: 60)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    runner = StressTestRunner(
        total_sensors = args.sensors,
        duration_s    = args.duration,
        api_url       = args.api_url,
        chaos_unit    = args.chaos_unit if args.chaos else None,
        chaos_delay   = args.chaos_delay,
    )
    runner.run()


if __name__ == "__main__":
    main()
