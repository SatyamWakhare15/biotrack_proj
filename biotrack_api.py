"""
BioTrack API Server
FastAPI-based REST API for BioTrack medical inventory system.

Endpoints:
- POST /api/inventory/batch: Bulk insert inventory records
- GET /api/inventory: Retrieve all inventory records
- POST /api/sensor/ping: Receive sensor ping data
- POST /api/audit/log: Receive audit/alert logs
- GET /api/sensor/pings: Retrieve sensor ping history
- GET /api/audit/logs: Retrieve audit logs

Run with: uvicorn biotrack_api:app --reload --host 0.0.0.0 --port 5000
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import json

app = FastAPI(title="BioTrack API", version="1.0.0")

# In-memory storage (replace with database in production)
inventory = []
sensor_pings = []
audit_logs = []

# Pydantic models
class InventoryRecord(BaseModel):
    batch_id: str
    drug_name: str
    quantity: int
    unit: str
    mfg_date: str
    expiry_date: str
    storage_temp_c: float
    min_temp_c: float
    max_temp_c: float
    storage_unit_id: str
    supplier: str
    lot_number: str
    status: str
    created_at: str
    notes: Optional[str] = ""

class SensorPing(BaseModel):
    sensor_id: str
    storage_unit_id: str
    temperature_c: float
    humidity_percent: Optional[float] = None
    timestamp: str
    status: str

class AuditLog(BaseModel):
    sensor_id: str
    event_type: str
    message: str
    timestamp: str
    severity: str

class InventoryBatch(BaseModel):
    records: List[dict]

# Inventory endpoints
@app.post("/api/inventory/batch")
async def create_inventory_batch(data: dict):
    """Bulk insert inventory records."""
    records = data.get("records", [])
    inventory.extend(records)
    return {"message": f"Inserted {len(records)} inventory records", "total": len(inventory)}

@app.get("/api/inventory")
async def get_inventory():
    """Retrieve all inventory records."""
    return {"inventory": inventory, "count": len(inventory)}

# Sensor endpoints
@app.post("/api/sensor/ping")
async def sensor_ping(ping: SensorPing):
    """Receive sensor ping data."""
    sensor_pings.append(ping.dict())
    return {"message": "Ping received", "sensor_id": ping.sensor_id}

@app.get("/api/sensor/pings")
async def get_sensor_pings():
    """Retrieve sensor ping history."""
    return {"pings": sensor_pings, "count": len(sensor_pings)}

# Audit endpoints
@app.post("/api/audit/log")
async def audit_log(log: AuditLog):
    """Receive audit/alert logs."""
    audit_logs.append(log.dict())
    return {"message": "Log received", "event_type": log.event_type}

@app.get("/api/audit/logs")
async def get_audit_logs():
    """Retrieve audit logs."""
    return {"logs": audit_logs, "count": len(audit_logs)}

# Health check
@app.get("/health")
async def health_check():
    """API health check."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)