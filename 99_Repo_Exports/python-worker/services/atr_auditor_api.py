#!/usr/bin/env python3
"""
ATR Auditor API Service
Read-only governance surface for Phase 6.7.
Provides unified state without allowing state mutations.
"""

import os
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import asyncpg
import json

DEFAULT_PORT = int(os.getenv("ATR_AUDITOR_API_PORT", "8093"))
DEFAULT_HOST = os.getenv("ATR_AUDITOR_API_HOST", "0.0.0.0")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://trade_user:trade_password@scanner-postgres:5432/trade")


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Startup
    try:
        application.state.db_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=10)
    except Exception:
        # Reconnects are handled lazily in get_db_pool()
        application.state.db_pool = None
    yield
    # Shutdown
    pool = getattr(application.state, "db_pool", None)
    if pool is not None:
        await pool.close()


app = FastAPI(
    title="ATR Auditor API",
    description="Read-only governance surface for ATR operators, compliance and research",
    version="1.0.0",
    lifespan=lifespan,
)

# -----------------------------------------------------------------------------
# Dependency: Database Pool
# -----------------------------------------------------------------------------
async def get_db_pool() -> asyncpg.Pool:
    if not hasattr(app.state, "db_pool") or app.state.db_pool is None:
        try:
            app.state.db_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=10)
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Database connection failed: {e}")
    return app.state.db_pool



# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "atr-auditor-api"}

@app.get("/auditor/release-board")
async def get_release_board():
    """Returns the unified change governance release board."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM v_governance_current_state ORDER BY change_id DESC LIMIT 200")
        return [dict(row) for row in rows]

@app.get("/auditor/change/{change_id}")
async def get_change_details(change_id: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM v_governance_current_state WHERE change_id = $1", change_id)
        if not row:
            raise HTTPException(status_code=404, detail="Change not found")
        # Fetch related artifacts
        artifacts = await conn.fetch("SELECT id, artifact_kind, created_at FROM atr_change_artifacts WHERE change_id = $1 ORDER BY created_at DESC", change_id)
        result = dict(row)
        result["artifacts"] = [dict(a) for a in artifacts]
        return result

@app.get("/auditor/incidents")
async def get_incident_board(
    severity: Optional[str] = None,
    symbol: Optional[str] = None,
    venue: Optional[str] = None
):
    """Returns the current open incidents from the incident board."""
    pool = await get_db_pool()
    query = "SELECT * FROM v_governance_incident_board WHERE 1=1"
    args = []
    
    if severity:
        args.append(severity)
        query += f" AND severity = ${len(args)}"
    if symbol:
        args.append(symbol)
        query += f" AND symbol = ${len(args)}"
    if venue:
        args.append(venue)
        query += f" AND venue = ${len(args)}"
        
    query += " LIMIT 200"
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
        return [dict(row) for row in rows]

@app.get("/auditor/incident/{incident_id}")
async def get_incident_details(incident_id: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM atr_incidents WHERE incident_id = $1", incident_id)
        if not row:
            raise HTTPException(status_code=404, detail="Incident not found")
        return dict(row)

@app.get("/auditor/postmortems")
async def get_postmortem_board():
    """Returns the postmortem hygiene board."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM v_governance_postmortem_board ORDER BY postmortem_id DESC LIMIT 200")
        return [dict(row) for row in rows]

@app.get("/auditor/postmortem/{postmortem_id}")
async def get_postmortem_details(postmortem_id: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        pm_row = await conn.fetchrow("SELECT * FROM atr_postmortems WHERE postmortem_id = $1", postmortem_id)
        if not pm_row:
            raise HTTPException(status_code=404, detail="Postmortem not found")
            
        actions = await conn.fetch("SELECT * FROM atr_corrective_actions WHERE postmortem_id = $1 ORDER BY due_at_ms ASC", postmortem_id)
        
        result = dict(pm_row)
        result["actions"] = [dict(a) for a in actions]
        return result

@app.get("/auditor/runtime-health")
async def get_runtime_health(
    scope_kind: Optional[str] = None
):
    """Returns the current runtime governance health board."""
    pool = await get_db_pool()
    query = "SELECT * FROM v_governance_runtime_health WHERE 1=1"
    args = []
    if scope_kind:
        args.append(scope_kind)
        query += f" AND scope_kind = ${len(args)}"
        
    query += " ORDER BY updated_at_ms DESC LIMIT 300"
    
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(query, *args)
        except asyncpg.exceptions.UndefinedTableError:
            # For backward compat or if migration wasn't perfectly applied
            return []
            
        return [dict(row) for row in rows]

@app.get("/auditor/evidence/{artifact_kind}/{id}")
async def get_evidence(artifact_kind: str, id: str):
    """Retrieve an immutable evidence artifact."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        if artifact_kind == 'change_artifact':
            row = await conn.fetchrow("SELECT * FROM atr_change_artifacts WHERE id = $1", int(id))
            if not row:
                raise HTTPException(status_code=404, detail="Artifact not found")
            return dict(row)
        # We can add explicit fetching for scorecards/rollback_manifests if needed directly,
        # but change_artifacts typically wrap them or they are referenced.
        raise HTTPException(status_code=400, detail=f"Unsupported artifact kind: {artifact_kind}")

@app.get("/auditor/snapshot/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    """Fetch an immutable point-in-time snapshot."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM atr_auditor_snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        # Snapshot JSON might be stored as string or asyncpg handles it as dict.
        result = dict(row)
        if isinstance(result['snapshot_json'], str):
            result['snapshot_json'] = json.loads(result['snapshot_json'])
        return result

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Starting ATR Auditor API Service on {DEFAULT_HOST}:{DEFAULT_PORT}")
    uvicorn.run(
        app,
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        reload=False,
        log_level="info"
    )
