"""
BioTrack v1.0 — Synthetic Medical Inventory Data Generator
Author : Satyam W. (Python Automation Team)
Purpose: Generate 5,000+ realistic medical inventory records and POST them
         to the BioTrack API via concurrent threads.

Usage:
    # Generate only (saves to CSV + JSON)
    python biotrack_data_generator.py

    # Generate AND seed the API
    python biotrack_data_generator.py --seed-api --api-url http://localhost:5000

    # Custom record count
    python biotrack_data_generator.py --count 10000

Dependencies:
    pip install faker requests
"""

import argparse
import csv
import json
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from enum import Enum
from pathlib import Path

import requests
from faker import Faker

fake = Faker()
random.seed(42)          # reproducible runs; remove for true randomness


# ─────────────────────────────────────────────
# 1. CONFIGURATION
# ─────────────────────────────────────────────

TOTAL_RECORDS   = 5_000
API_ENDPOINT    = "/api/inventory/batch"
POST_WORKERS    = 20     # concurrent POST threads when seeding API
POST_BATCH_SIZE = 50     # records per POST request

OUTPUT_DIR = Path("biotrack_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Real-world medical drug catalogue (drug → [min_temp°C, max_temp°C, unit])
DRUG_CATALOGUE = {
    # Vaccines (cold-chain 2–8°C)
    "Influenza Vaccine":       (2,  8,  "vial"),
    "MMR Vaccine":             (2,  8,  "vial"),
    "Hepatitis B Vaccine":     (2,  8,  "vial"),
    "COVID-19 mRNA Vaccine":   (-80, -60, "vial"),   # ultra-cold
    "Varicella Vaccine":       (-50, -15, "vial"),   # frozen
    "Rotavirus Vaccine":       (2,  8,  "vial"),
    # Blood products
    "Packed Red Blood Cells":  (1,  6,  "unit"),
    "Fresh Frozen Plasma":     (-25, -18, "unit"),
    "Platelet Concentrate":    (20, 24,  "unit"),
    # Biological reagents
    "Insulin (Regular)":       (2,  8,  "mL"),
    "Epinephrine 1mg/mL":      (15, 25, "mL"),
    "Monoclonal Antibody A":   (2,  8,  "mg"),
    # Chemotherapy
    "Methotrexate 50mg":       (15, 25, "vial"),
    "Carboplatin 150mg":       (2,  8,  "vial"),
    # General medications
    "Amoxicillin 500mg":       (15, 25, "tablet"),
    "Paracetamol 500mg":       (15, 25, "tablet"),
}

STORAGE_UNITS = [
    "Fridge-1-NorthWing",
    "Fridge-2-NorthWing",
    "Fridge-3-SouthWing",
    "Fridge-4-WestWing",     # the "problem unit" — mentioned in the charter
    "Fridge-5-WestWing",
    "Freezer-1-ColdRoom",
    "Freezer-2-ColdRoom",
    "UltraCold-1-Lab",
    "AmbientShelf-1-Pharmacy",
    "AmbientShelf-2-Pharmacy",
]


# ─────────────────────────────────────────────
# 2. DATA MODEL
# ─────────────────────────────────────────────

class BatchStatus(str, Enum):
    ACTIVE    = "ACTIVE"
    EXPIRING  = "EXPIRING_SOON"   # < 30 days
    EXPIRED   = "EXPIRED"
    QUARANTINE = "QUARANTINE"


@dataclass
class MedicalBatch:
    """
    Core inventory record — maps 1:1 to the SQL `medical_batches` table.
    All fields must be JSON-serialisable (dates as ISO strings).
    """
    batch_id:       str
    drug_name:      str
    quantity:       int
    unit:           str
    mfg_date:       str           # ISO-8601 date
    expiry_date:    str           # ISO-8601 date
    storage_temp_c: float         # actual recorded temp at intake
    min_temp_c:     float         # drug's required min temp
    max_temp_c:     float         # drug's required max temp
    storage_unit_id: str
    supplier:       str
    lot_number:     str
    status:         str
    created_at:     str           # ISO-8601 datetime
    notes:          str           = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def to_csv_row(self) -> dict:
        """Flat dict for CSV writer."""
        return {
            "batch_id":       self.batch_id,
            "drug_name":      self.drug_name,
            "quantity":       self.quantity,
            "unit":           self.unit,
            "mfg_date":       self.mfg_date,
            "expiry_date":    self.expiry_date,
            "storage_temp_c": self.storage_temp_c,
            "min_temp_c":     self.min_temp_c,
            "max_temp_c":     self.max_temp_c,
            "storage_unit_id": self.storage_unit_id,
            "supplier":       self.supplier,
            "lot_number":     self.lot_number,
            "status":         self.status,
            "created_at":     self.created_at,
            "notes":          self.notes,
        }


# ─────────────────────────────────────────────
# 3. EDGE-CASE PROFILES
# ─────────────────────────────────────────────
# These ensure the Data team has real anomalies to detect.

def _expiry_date_by_profile(profile: str) -> date:
    today = date.today()
    if profile == "normal":
        return today + timedelta(days=random.randint(60, 730))
    elif profile == "expiring_soon":
        return today + timedelta(days=random.randint(1, 29))
    elif profile == "expired":
        return today - timedelta(days=random.randint(1, 365))
    elif profile == "long_shelf":
        return today + timedelta(days=random.randint(730, 1825))
    return today + timedelta(days=180)


def _temp_by_profile(min_t: float, max_t: float, profile: str) -> float:
    """
    Simulate realistic temperature readings at intake.
    Occasionally returns out-of-range values (real-world cold-chain breaks).
    """
    if profile == "in_range":
        # Gaussian centred on midpoint — most readings cluster around ideal
        mid = (min_t + max_t) / 2
        sigma = (max_t - min_t) / 6      # 99.7% within range
        t = random.gauss(mid, sigma)
        return round(max(min_t, min(max_t, t)), 2)

    elif profile == "spike":
        # Temp breach — a cold-chain failure the alert engine should catch
        spike_margin = random.uniform(2, 8)
        return round(max_t + spike_margin, 2)

    elif profile == "drift":
        # Borderline — just inside or just outside range
        drift = random.uniform(-1, 1)
        return round(max_t + drift, 2)

    return round((min_t + max_t) / 2, 2)


def _determine_status(expiry: date, storage_temp: float,
                       min_t: float, max_t: float) -> BatchStatus:
    today = date.today()
    days_left = (expiry - today).days

    if storage_temp > max_t + 2:        # significant temp breach
        return BatchStatus.QUARANTINE
    if days_left < 0:
        return BatchStatus.EXPIRED
    if days_left <= 29:
        return BatchStatus.EXPIRING
    return BatchStatus.ACTIVE


# ─────────────────────────────────────────────
# 4. GENERATOR
# ─────────────────────────────────────────────

def _choose_profile_weights() -> tuple[str, str]:
    """
    Returns (expiry_profile, temp_profile) based on realistic distribution.
    ~70% normal, ~15% expiring soon, ~8% expired, ~7% anomalies.
    """
    expiry_profile = random.choices(
        ["normal", "expiring_soon", "expired", "long_shelf"],
        weights=[70, 15, 8, 7],
        k=1
    )[0]

    temp_profile = random.choices(
        ["in_range", "drift", "spike"],
        weights=[85, 10, 5],
        k=1
    )[0]

    return expiry_profile, temp_profile


def generate_batch() -> MedicalBatch:
    """Generate one realistic MedicalBatch record."""
    drug_name = random.choice(list(DRUG_CATALOGUE.keys()))
    min_t, max_t, unit = DRUG_CATALOGUE[drug_name]

    expiry_profile, temp_profile = _choose_profile_weights()

    expiry_date   = _expiry_date_by_profile(expiry_profile)
    mfg_date      = expiry_date - timedelta(days=random.randint(180, 1095))
    storage_temp  = _temp_by_profile(min_t, max_t, temp_profile)
    status        = _determine_status(expiry_date, storage_temp, min_t, max_t)

    notes = ""
    if status == BatchStatus.QUARANTINE:
        notes = f"Temp breach detected at intake: {storage_temp}°C (range {min_t}–{max_t}°C)"
    elif status == BatchStatus.EXPIRING:
        notes = f"Expires in {(expiry_date - date.today()).days} day(s). Prioritise for FEFO dispatch."

    return MedicalBatch(
        batch_id        = f"BTK-{uuid.uuid4().hex[:10].upper()}",
        drug_name       = drug_name,
        quantity        = random.randint(10, 2000),
        unit            = unit,
        mfg_date        = mfg_date.isoformat(),
        expiry_date     = expiry_date.isoformat(),
        storage_temp_c  = storage_temp,
        min_temp_c      = min_t,
        max_temp_c      = max_t,
        storage_unit_id = random.choice(STORAGE_UNITS),
        supplier        = fake.company(),
        lot_number      = f"LOT-{fake.bothify('??###??').upper()}",
        status          = status.value,
        created_at      = fake.date_time_between(
                            start_date="-1y", end_date="now"
                          ).isoformat(),
        notes           = notes,
    )


def generate_dataset(count: int = TOTAL_RECORDS) -> list[MedicalBatch]:
    print(f"[GEN] Generating {count:,} medical inventory records...")
    t0 = time.perf_counter()
    records = [generate_batch() for _ in range(count)]
    elapsed = time.perf_counter() - t0
    print(f"[GEN] Done in {elapsed:.2f}s — {count/elapsed:,.0f} records/sec")
    return records


# ─────────────────────────────────────────────
# 5. EXPORT
# ─────────────────────────────────────────────

def save_csv(records: list[MedicalBatch], path: Path) -> None:
    fieldnames = list(records[0].to_csv_row().keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(r.to_csv_row() for r in records)
    print(f"[CSV] Saved {len(records):,} records → {path}")


def save_json(records: list[MedicalBatch], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([r.to_dict() for r in records], f, indent=2, default=str)
    print(f"[JSON] Saved {len(records):,} records → {path}")


def print_summary(records: list[MedicalBatch]) -> None:
    from collections import Counter
    status_counts = Counter(r.status for r in records)
    drug_counts   = Counter(r.drug_name for r in records)
    quarantined   = [r for r in records if r.status == BatchStatus.QUARANTINE.value]

    print("\n━━━ Dataset Summary ━━━")
    print(f"  Total records    : {len(records):,}")
    for s, c in sorted(status_counts.items()):
        print(f"  {s:<20}: {c:>5} ({c/len(records)*100:.1f}%)")
    print(f"\n  Top 5 drugs:")
    for drug, cnt in drug_counts.most_common(5):
        print(f"    {drug:<35} {cnt:>4} batches")
    print(f"\n  Quarantined (temp breach): {len(quarantined)}")
    print("━━━━━━━━━━━━━━━━━━━━━━\n")


# ─────────────────────────────────────────────
# 6. API SEEDER (concurrent POST)
# ─────────────────────────────────────────────

def _post_batch(api_url: str, records: list[dict], worker_id: int) -> dict:
    """POST one batch of records to the API. Returns a result summary."""
    url = api_url.rstrip("/") + API_ENDPOINT
    try:
        t0 = time.perf_counter()
        resp = requests.post(
            url,
            json={"records": records},
            timeout=10,
            headers={"Content-Type": "application/json"},
        )
        latency_ms = (time.perf_counter() - t0) * 1000
        return {
            "worker":     worker_id,
            "count":      len(records),
            "status":     resp.status_code,
            "latency_ms": round(latency_ms, 1),
            "ok":         resp.ok,
        }
    except requests.RequestException as e:
        return {
            "worker":     worker_id,
            "count":      len(records),
            "status":     0,
            "latency_ms": -1,
            "ok":         False,
            "error":      str(e),
        }


def seed_api(records: list[MedicalBatch], api_url: str) -> None:
    """
    Split records into batches of POST_BATCH_SIZE and POST them concurrently
    using a ThreadPoolExecutor — same pattern you'll use in the sensor suite.
    """
    print(f"[API] Seeding {len(records):,} records to {api_url} ...")
    print(f"      Batch size: {POST_BATCH_SIZE}  Workers: {POST_WORKERS}")

    payloads = [
        ([r.to_dict() for r in records[i: i + POST_BATCH_SIZE]], i // POST_BATCH_SIZE)
        for i in range(0, len(records), POST_BATCH_SIZE)
    ]

    success, failed, total_latency = 0, 0, 0.0
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=POST_WORKERS) as pool:
        futures = {
            pool.submit(_post_batch, api_url, batch, wid): wid
            for batch, wid in payloads
        }
        for future in as_completed(futures):
            result = future.result()
            if result["ok"]:
                success += result["count"]
                total_latency += result["latency_ms"]
            else:
                failed += result["count"]
                err = result.get("error", f"HTTP {result['status']}")
                print(f"  [WARN] Worker {result['worker']} failed: {err}")

    elapsed = time.perf_counter() - t0
    avg_latency = total_latency / len(payloads) if payloads else 0

    print(f"\n[API] Seeding complete in {elapsed:.2f}s")
    print(f"      Inserted : {success:,}")
    print(f"      Failed   : {failed:,}")
    print(f"      Avg POST latency: {avg_latency:.1f} ms per batch")


# ─────────────────────────────────────────────
# 7. ENTRY POINT
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BioTrack Synthetic Data Generator (Satyam W.)"
    )
    p.add_argument("--count",     type=int,  default=TOTAL_RECORDS,
                   help="Number of records to generate (default: 5000)")
    p.add_argument("--seed-api",  action="store_true",
                   help="POST records to the BioTrack API after generation")
    p.add_argument("--api-url",   type=str,  default="http://localhost:5000",
                   help="Base URL of the BioTrack API")
    p.add_argument("--no-csv",    action="store_true",
                   help="Skip saving CSV output")
    p.add_argument("--no-json",   action="store_true",
                   help="Skip saving JSON output")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # — Generate —
    records = generate_dataset(args.count)
    print_summary(records)

    # — Export —
    if not args.no_csv:
        save_csv(records, OUTPUT_DIR / "medical_inventory.csv")
    if not args.no_json:
        save_json(records, OUTPUT_DIR / "medical_inventory.json")

    # — Seed API —
    if args.seed_api:
        seed_api(records, args.api_url)
    else:
        print("[INFO] Skipping API seed. Run with --seed-api to POST records.")


if __name__ == "__main__":
    main()
