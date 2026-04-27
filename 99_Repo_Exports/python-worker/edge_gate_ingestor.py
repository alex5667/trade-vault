#!/usr/bin/env python3
"""
Edge Gate Ingestion Service (D1-A).
Consumes EdgeGateEvents from Redis Stream and batch inserts into PostgreSQL.
"""
import os
import time
import json
import math
import logging
import signal
import sys
from typing import List, Dict, Any, Tuple

import redis
import psycopg2
from psycopg2 import pool, extras

# Config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("EdgeGateIngestor")

REDIS_HOST = os.getenv("REDIS_HOST", "redis-worker-1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_DSN = os.getenv("EDGE_GATE_DB_DSN", os.getenv("TRADES_DB_DSN", ""))

STREAM_KEY = os.getenv("EDGE_GATE_EVENTS_STREAM", "stream:diag:edge_gate_events")
GROUP_NAME = os.getenv("EDGE_GATE_CONSUMER_GROUP", "edge_gate_pg")
CONSUMER_NAME = os.getenv("EDGE_GATE_CONSUMER_NAME", f"ingestor-{os.getpid()}")
DLQ_STREAM = os.getenv("EDGE_GATE_DLQ_STREAM", "stream:dlq:edge_gate_events")

BATCH_SIZE = int(os.getenv("EDGE_GATE_BATCH_SIZE", "500"))
FLUSH_MS = int(os.getenv("EDGE_GATE_FLUSH_MS", "200"))
MAX_RETRIES = 3

# Parsing / validation config
EPS_BPS = float(os.getenv("EDGE_GATE_EPS_BPS", "0.05"))  # tolerance for recompute flags
RATIO_CAP = float(os.getenv("EDGE_GATE_RATIO_CAP", "1000000.0"))  # cap huge ratios
HARD_CONSISTENCY = os.getenv("EDGE_GATE_HARD_CONSISTENCY", "1").strip().lower() in {"1","true","on","yes"}

def _isfinite(x: float) -> bool:
    """Check if value is finite (not NaN/Inf)."""
    return isinstance(x, (int, float)) and math.isfinite(float(x))

def _safe_int(v: Any, field: str) -> int:
    """Parse int with validation."""
    if v is None:
        raise ValueError(f"Missing {field}")
    if isinstance(v, str) and v.strip() == "":
        raise ValueError(f"Empty {field}")
    try:
        return int(v)
    except Exception:
        raise ValueError(f"Bad int {field}={v!r}")

def _safe_float(v: Any, field: str, default: float = None) -> float:
    """Parse float with finite check."""
    if v is None or (isinstance(v, str) and v.strip() == ""):
        if default is None:
            raise ValueError(f"Missing {field}")
        return float(default)
    try:
        vv = float(v)
    except Exception:
        raise ValueError(f"Bad float {field}={v!r}")
    if not _isfinite(vv):
        raise ValueError(f"Non-finite {field}={vv}")
    return vv

def _safe_bool01(v: Any, field: str, default: bool = False) -> bool:
    """Parse bool from 0/1/true/false."""
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Bad bool {field}={v!r}")

# Global shutdown flag
RUNNING = True

def handle_signal(signum, frame):
    global RUNNING
    logger.info("Received signal %s, shutting down...", signum)
    RUNNING = False

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

class PostgresWriter:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        self._connect()

    def _connect(self):
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(1, 4, dsn=self.dsn)
            logger.info("Connected to Postgres")
        except Exception as e:
            logger.fatal("Failed to connect to Postgres: %s", e)
            sys.exit(1)

    def write_batch(self, events: List[Dict[str, Any]]) -> int:
        if not events:
            return 0
        
        query = """
            INSERT INTO edge_gate_events (
                signal_id, symbol, gate_name, gate_version, stage,
                ts_ms, passed, veto_code, edge_source,
                exp_bps, req_bps, margin_bps, edge_ratio,
                k, fees_bps, slip_bps, buf_bps, total_costs_bps, ctx
            ) VALUES %s
            ON CONFLICT (signal_id, gate_name, stage, gate_version) DO NOTHING
        """
        
        data = []
        for e in events:
            data.append((
                e["signal_id"], e["symbol"], e["gate_name"], e["gate_version"], e["stage"],
                e["ts_ms"], e["passed"], e["veto_code"], e["edge_source"],
                e["exp_bps"], e["req_bps"], e["margin_bps"], e["edge_ratio"],
                e["k"], e["fees_bps"], e["slip_bps"], e["buf_bps"], e["total_costs_bps"],
                json.dumps(e.get("ctx_obj") or {})
            ))

        conn = None
        try:
            conn = self.pool.getconn()
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data)
                conn.commit()
            return len(data)
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error("Batch write failed: %s", e)
            raise e
        finally:
            if conn:
                self.pool.putconn(conn)

    def close(self):
        if self.pool:
            self.pool.closeall()

def main():
    if not DB_DSN:
        logger.fatal("EDGE_GATE_DB_DSN or TRADES_DB_DSN must be set")
        sys.exit(1)

    # Redis init
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
    else:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    for i in range(20):
        try:
            r.ping()
            # Create group idempotent
            try:
                r.xgroup_create(STREAM_KEY, GROUP_NAME, mkstream=True)
                logger.info("Consumer Group '%s' created for stream '%s'", GROUP_NAME, STREAM_KEY)
            except redis.exceptions.ResponseError as e:
                err_str = str(e)
                if "BUSYGROUP" in err_str:
                    pass
                elif "loading the dataset in memory" in err_str.lower() or "busyloading" in err_str.lower():
                    logger.warning("Redis is loading dataset in memory, wait 5s... (%d/20)", i+1)
                    time.sleep(5)
                    continue
                else:
                    raise e
            break
        except Exception as e:
            err_str = str(e).lower()
            if "loading the dataset in memory" in err_str or "busyloading" in err_str:
                logger.warning("Redis is loading dataset in memory, wait 5s... (%d/20)", i+1)
                time.sleep(5)
                continue
            logger.fatal("Redis Init Failed: %s", e)
            sys.exit(1)
    else:
        logger.fatal("Timeout waiting for Redis dataset to load in memory")
        sys.exit(1)

    # Postgres init
    pg = PostgresWriter(DB_DSN)
    
    logger.info("Ingestor started. Batch=%d Flush=%dms", BATCH_SIZE, FLUSH_MS)
    
    total_processed = 0

    while RUNNING:
        try:
            # Read from group
            # XREADGROUP GROUP group consumer COUNT batch BLOCK 2000 STREAMS key >
            try:
                resp = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=BATCH_SIZE, block=2000)
            except redis.exceptions.ResponseError as e:
                err_str = str(e).lower()
                if "nogroup" in err_str:
                    logger.warning("Consumer group missing (NOGROUP), attempting to recreate...")
                    try:
                        r.xgroup_create(STREAM_KEY, GROUP_NAME, mkstream=True)
                        logger.info("Consumer Group '%s' recreated", GROUP_NAME)
                        continue
                    except Exception as create_err:
                        logger.error("Failed to recreate group: %s", create_err)
                        time.sleep(1)
                        continue
                elif "loading the dataset in memory" in err_str or "busyloading" in err_str:
                    logger.warning("Redis is loading the dataset in memory. Waiting 5s...")
                    time.sleep(5)
                    continue
                else:
                    raise e
            
            if not resp:
                continue
                
            stream_data = resp[0][1] # [(id, fields), ...]
            if not stream_data:
                continue

            valid_events = []
            parsed_ids = []
            failed_ids = []

            for msg_id, fields in stream_data:
                try:
                    # Parse & Validate (strict)
                    signal_id = (fields.get("signal_id") or "").strip()
                    symbol = (fields.get("symbol") or "").strip().upper()
                    if not signal_id or not symbol:
                        raise ValueError("Missing ID or Symbol")
                    
                    ts_ms = _safe_int(fields.get("ts_ms", None), "ts_ms")
                    if ts_ms <= 0:
                        raise ValueError(f"Invalid ts_ms={ts_ms}")
                    
                    gate_name = (fields.get("gate_name") or "edge_cost").strip() or "edge_cost"
                    gate_version = _safe_int(fields.get("gate_version", 3), "gate_version")
                    stage = (fields.get("stage") or "pre_emit").strip() or "pre_emit"
                    
                    passed = _safe_bool01(fields.get("passed", "0"), "passed", default=False)
                    veto_code = (fields.get("veto_code") or "").strip() or None
                    edge_source = (fields.get("edge_source") or "none").strip() or "none"
                    
                    # Numerics (finite)
                    exp_bps = _safe_float(fields.get("exp_bps", None), "exp_bps", default=0.0)
                    req_bps = _safe_float(fields.get("req_bps", None), "req_bps", default=0.0)
                    
                    fees_bps = _safe_float(fields.get("fees_bps", None), "fees_bps", default=0.0)
                    slip_bps = _safe_float(fields.get("slip_bps", None), "slip_bps", default=0.0)
                    buf_bps  = _safe_float(fields.get("buf_bps", None),  "buf_bps",  default=0.0)
                    k        = _safe_float(fields.get("k", None),        "k",        default=0.0)
                    
                    # Derived: recompute margin & ratio deterministically
                    margin_bps_calc = exp_bps - req_bps
                    ratio_calc = 0.0
                    if req_bps > 0:
                        ratio_calc = exp_bps / req_bps
                        if ratio_calc > RATIO_CAP:
                            ratio_calc = RATIO_CAP
                    else:
                        ratio_calc = 0.0
                    
                    # total_costs: prefer recompute from components if mismatch is significant
                    total_costs_in = _safe_float(fields.get("total_costs_bps", None), "total_costs_bps", 
                                                default=(fees_bps + slip_bps + buf_bps))
                    total_costs_calc = fees_bps + slip_bps + buf_bps
                    total_costs_bps = total_costs_in
                    costs_recomputed = False
                    if abs(total_costs_in - total_costs_calc) > EPS_BPS:
                        total_costs_bps = total_costs_calc
                        costs_recomputed = True
                    
                    # Guardrail: detect broken producer (all zeros)
                    if (
                        exp_bps == 0.0 and req_bps == 0.0 and k == 0.0 and
                        fees_bps == 0.0 and slip_bps == 0.0 and buf_bps == 0.0 and
                        total_costs_bps == 0.0
                    ):
                        raise ValueError("all_zero_metrics")
                    
                    # ctx json (optional)
                    ctx_obj = None
                    raw_ctx = fields.get("ctx")
                    if raw_ctx:
                        try:
                            ctx_obj = json.loads(raw_ctx)
                            if not isinstance(ctx_obj, dict):
                                ctx_obj = {"ctx": ctx_obj}
                        except Exception:
                            ctx_obj = None
                    
                    # --- HARD CONSISTENCY MODE ---
                    # Optionally enforce: req_bps = k * total_costs_bps
                    req_recomputed = False
                    if HARD_CONSISTENCY and k > 0:
                        req_calc = k * total_costs_bps
                        if abs(req_bps - req_calc) > EPS_BPS:
                            if ctx_obj is None:
                                ctx_obj = {}
                            ctx_obj["req_recomputed"] = True
                            ctx_obj["req_in"] = req_bps
                            ctx_obj["req_calc"] = req_calc
                            req_bps = req_calc
                            req_recomputed = True
                            # Recompute margin and ratio with new req_bps
                            margin_bps_calc = exp_bps - req_bps
                            if req_bps > 0:
                                ratio_calc = exp_bps / req_bps
                                if ratio_calc > RATIO_CAP:
                                    ratio_calc = RATIO_CAP
                            else:
                                ratio_calc = 0.0
                    
                    # Attach recompute flags
                    if costs_recomputed:
                        if ctx_obj is None:
                            ctx_obj = {}
                        ctx_obj["costs_recomputed"] = True
                        ctx_obj["total_costs_in"] = total_costs_in
                        ctx_obj["total_costs_calc"] = total_costs_calc
                    
                    evt = {
                        "signal_id": signal_id,
                        "symbol": symbol,
                        "ts_ms": ts_ms,
                        "gate_name": gate_name,
                        "gate_version": gate_version,
                        "stage": stage,
                        "passed": passed,
                        "veto_code": veto_code,
                        "edge_source": edge_source,
                        
                        "exp_bps": exp_bps,
                        "req_bps": req_bps,
                        "margin_bps": margin_bps_calc,
                        "edge_ratio": ratio_calc,
                        
                        "k": k,
                        "fees_bps": fees_bps,
                        "slip_bps": slip_bps,
                        "buf_bps": buf_bps,
                        "total_costs_bps": total_costs_bps,
                        
                        "ctx_obj": ctx_obj if ctx_obj else None
                    }


                    valid_events.append(evt)
                    parsed_ids.append(msg_id)

                except Exception as e:
                    # Send to DLQ
                    try:
                        r.xadd(DLQ_STREAM, {"original_id": msg_id, "error": str(e), "raw": json.dumps(fields)}, maxlen=200000)
                    except:
                        pass
                    failed_ids.append(msg_id)

            # Write batch
            if valid_events:
                try:
                    pg.write_batch(valid_events)
                    total_processed += len(valid_events)
                except Exception:
                    # Critical DB failure - exit loop to restart
                    logger.error("DB Write Failed - Crashing to retry")
                    sys.exit(1)

            # Ack all (both valid and failed processed/DLQ'd)
            all_ids = parsed_ids + failed_ids
            if all_ids:
                r.xack(STREAM_KEY, GROUP_NAME, *all_ids)
                
            if total_processed % 1000 == 0 and total_processed > 0:
                logger.info("Processed %d events total", total_processed)

        except KeyboardInterrupt:
            break
        except Exception as e:
            err_str = str(e).lower()
            if "loading the dataset in memory" in err_str or "busyloading" in err_str or "BusyLoading" in type(e).__name__:
                logger.warning("Redis is loading the dataset in memory. Waiting 5s...")
                time.sleep(5)
            else:
                logger.error("Loop Error: %s", e)
                time.sleep(1)

    pg.close()
    logger.info("Shutdown complete")

if __name__ == "__main__":
    main()
