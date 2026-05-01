#!/usr/bin/env python3
""",
ATR Auditor Snapshot Service
Periodically captures the state of governance boards to maintain
an immutable audit history over time.
""",

import os
import sys
import time
import uuid
import json
import asyncio
import logging
from datetime import datetime, timezone
import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
log = logging.getLogger("atr_auditor_snapshot")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://trade_user:trade_password@scanner-postgres:5432/trade")
SNAPSHOT_INTERVAL_SEC = int(os.getenv("ATR_AUDITOR_SNAPSHOT_INTERVAL", "3600")) # Defaults to 1 hour

VIEWS = {
    "release_board": "v_governance_current_state",
    "incident_board": "v_governance_incident_board",
    "postmortem_board": "v_governance_postmortem_board",
    "runtime_health": "v_governance_runtime_health"
}

async def capture_snapshot(pool: asyncpg.Pool, kind: str):
    """Fetches current tabular state of a board and saves it as a JSON snapshot."""
    view_name = VIEWS.get(kind)
    if not view_name:
        log.error(f"Unknown snapshot kind: {kind}")
        return

    try:
        async with pool.acquire() as conn:
            # Note: We fetch all rows for a full board snapshot. 
            # In massive installations, this might need time-bounding, but 
            # active governance states are typically bounded.
            rows = await conn.fetch(f"SELECT * FROM {view_name} LIMIT 1000")
            data = [dict(row) for row in rows]
            
            # Serialize dt/date to isoformat
            def json_default(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return str(obj)

            json_val = json.dumps(data, default=json_default)
            snapshot_id = f"snap_{kind}_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"

            await conn.execute(
                """,
                INSERT INTO atr_auditor_snapshots 
                (snapshot_id, snapshot_kind, snapshot_json, created_at)
                VALUES ($1, $2, $3, $4)
                """,
                snapshot_id, kind, json_val, datetime.now(timezone.utc)
            )
            log.info(f"Captured {kind} snapshot: {snapshot_id} ({len(data)} rows)")

    except asyncpg.exceptions.UndefinedTableError:
        log.warning(f"View {view_name} does not exist yet. Skip capturing.")
    except Exception as e:
        log.error(f"Failed to capture snapshot {kind}: {e}")

async def main_loop():
    log.info(f"Starting ATR Auditor Snapshot Service. Interval: {SNAPSHOT_INTERVAL_SEC}s")
    
    # Simple retry for initial DB connection
    pool = None
    while pool is None:
        try:
            pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=5)
            log.info("Connected to PostgreSQL")
        except Exception as e:
            log.error(f"Cannot connect to DB: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

    try:
        while True:
            # Perform snapshots
            for kind in VIEWS.keys():
                await capture_snapshot(pool, kind)
            
            # Wait for next interval
            await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)
    except asyncio.CancelledError:
        log.info("Service shutting down...")
    finally:
        await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        sys.exit(0)
