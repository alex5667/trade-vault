#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_trade_closed_ndjson.py

Exports POSITION_CLOSED events to NDJSON for offline analysis / autopilot.

Default source: Redis Stream events:trades (TRADE_EVENTS_STREAM).
Why stream (not DB):
  - deterministic, append-only
  - works even without analytics DB access
  - includes AB/meta fields expanded into root by TradeEventsLogger

Usage:
  cd python-worker
  PYTHONPATH=".:.." python tools/export_trade_closed_ndjson.py --since-hours 168 --out /tmp/closed_7d.ndjson
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict, List, Tuple, Optional, Union

import redis


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _loads_maybe_json(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    s = str(x)
    if not s:
        return None
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return s
    return s


def _event_ts_ms(fields: Dict[str, Any]) -> int:
    return _safe_int(fields.get("ts_ms") or fields.get("ts") or fields.get("timestamp") or 0)


def _normalize_sid(raw_sid: Any, *, symbol: str, ts_ms: int) -> str:
    """Normalize sid to canonical: crypto-of:{SYMBOL}:{TS_MS}."""
    sym = str(symbol or "").upper() or "NA"
    try:
        ts = int(ts_ms)
    except Exception:
        ts = 0
    s = str(raw_sid or "")
    if s.startswith("crypto-of:"):
        head = s.split("|", 1)[0]
        parts = head.split(":", 2)
        if len(parts) == 3:
            sym2 = (parts[1] or sym).upper()
            try:
                ts2 = int(float(parts[2]))
            except Exception:
                ts2 = ts
            return f"crypto-of:{sym2}:{ts2}"
        return f"crypto-of:{sym}:{ts}"
    if "|" in s:
        try:
            p = s.split("|")
            sym2 = (p[0] or sym).upper()
            ts2 = int(float(p[1])) if len(p) > 1 else ts
            return f"crypto-of:{sym2}:{ts2}"
        except Exception:
            return f"crypto-of:{sym}:{ts}"
    if not s:
        return f"crypto-of:{sym}:{ts}"
    return s


def _is_position_closed(fields: Dict[str, Any]) -> bool:
    """
    Check if event is a position closed event.
    Supports both explicit event_type and implicit (trades:closed stream may not have event_type).
    """
    et = str(fields.get("event_type") or fields.get("event") or "").upper()
    closed_types = {"POSITION_CLOSED", "CLOSE", "MANUAL_EXIT", "STOP_HIT", "TP_HIT"}
    if et in closed_types:
        return True
    # If no event_type but has exit_ts_ms and pnl fields, assume it's a closed trade
    # (trades:closed stream may store "clean" records without event_type)
    if not et and fields.get("exit_ts_ms") and ("pnl" in fields or "pnl_net" in fields):
        return True
    return False


def export_stream(
    *,
    r: redis.Redis,
    stream: str,
    since_ms: int,
    out_path: str,
    max_scan: int = 500_000,
) -> Tuple[int, int]:
    """
    Reads stream backwards and writes NDJSON in chronological order.
    Returns (written, scanned).
    """
    scanned = 0
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        
        # xrevrange returns filtered items including the max ID (last_id).
        # If the batch only contains the last_id we already processed, we are done.
        is_stuck = True
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            is_stuck = False
            last_id = msg_id
            if not isinstance(fields, dict):
                continue
            ts_ms = _event_ts_ms(fields)
            if ts_ms and ts_ms < since_ms:
                # we reached the boundary; stop (stream is time-ordered enough)
                scanned = max_scan
                break
            if not _is_position_closed(fields):
                continue

            # Normalize minimal schema for tuner
            meta = _loads_maybe_json(fields.get("meta") or fields.get("metadata"))
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            if not isinstance(meta, dict):
                meta = {}

            # Normalize sid to canonical format: crypto-of:{symbol}:{ts_ms}
            symbol_str = str(fields.get("symbol") or "").upper()
            raw_sid = str(fields.get("sid") or "")
            normalized_sid = _normalize_sid(raw_sid, symbol=symbol_str, ts_ms=ts_ms)
            
            row = {
                "ts_ms": ts_ms,
                "exit_ts_ms": ts_ms,  # Alias for compatibility
                "symbol": symbol_str,
                "sid": normalized_sid,  # Normalized canonical sid
                "event_id": str(fields.get("event_id") or ""),
                "pnl": _safe_float(fields.get("pnl") or fields.get("pnl_net") or 0.0),
                "risk_usd": _safe_float(fields.get("risk_usd") or 0.0),
                "r_mult": _safe_float(fields.get("r_mult") or fields.get("r_multiple") or 0.0),
                "regime": str(fields.get("regime") or meta.get("regime") or "na").lower(),
                "regime_group": str(fields.get("regime_group") or meta.get("regime_group") or fields.get("regime") or meta.get("regime") or "na").lower(),  # For stratified DiD
                "scenario": str(fields.get("scenario") or meta.get("scenario") or "").lower(),
                "scenario_v4": str(fields.get("scenario_v4") or meta.get("scenario_v4") or "").lower(),  # For additional stratification
                "ab_arm": str(fields.get("ab_arm") or meta.get("ab_arm") or "A").upper(),
                "ab_group": str(fields.get("ab_group") or meta.get("ab_group") or "default").lower(),
                "arm_ver": _safe_int(fields.get("arm_ver") or meta.get("arm_ver") or 0),
                # optional quality fields (pass-through)
                "book_health_ok": _safe_int(fields.get("book_health_ok") or 0),
                "dn_tier": _safe_int(fields.get("dn_tier") or 0),
                "abs_lvl_tier": _safe_int(fields.get("abs_lvl_tier") or 0),
                "of_confirm_ok": _safe_int(fields.get("of_confirm_ok") or 0),
                "of_confirm_ok_soft": _safe_int(fields.get("of_confirm_ok_soft") or 0),
                # meta enforce fields (for ramp evaluation and Stage2 optimization)
                "meta_enforce_applied": _safe_int(fields.get("meta_enforce_applied") or None),
                "meta_veto": _safe_int(fields.get("meta_veto") or meta.get("meta_veto") or 0),
                "meta_enforce_key": str(fields.get("meta_enforce_key") or meta.get("meta_enforce_key") or ""),
                "meta_enforce_salt": str(fields.get("meta_enforce_salt") or meta.get("meta_enforce_salt") or "enf_v1"),
            }
            rows.append(row)
        
        if is_stuck:
            break

    rows.sort(key=lambda x: int(x.get("ts_ms") or 0))
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    return len(rows), scanned


def export_ndjson(
    *,
    r: Optional[redis.Redis] = None,
    redis_url: Optional[str] = None,
    stream: Optional[str] = None,
    since_ts_ms: Optional[int] = None,
    since_hours: Optional[float] = None,
    out_path: Optional[str] = None,
    batch: int = 1000,
    max_scan: int = 500_000,
) -> int:
    """
    Flexible export function that supports multiple call patterns.
    
    Args:
        r: Redis client (if provided, redis_url is ignored)
        redis_url: Redis URL string (used if r is None)
        stream: Stream name (default: TRADE_EVENTS_STREAM env or "events:trades")
        since_ts_ms: Start timestamp in milliseconds
        since_hours: Hours ago to start from (used if since_ts_ms is None)
        out_path: Output file path (required)
        batch: Batch size (unused, kept for compatibility)
        max_scan: Maximum messages to scan (default: 500_000)
    
    Returns:
        Number of rows written
    """
    if out_path is None:
        raise ValueError("out_path is required")
    
    if r is None:
        url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = redis.from_url(url, decode_responses=True)
    
    # Default stream: trades:closed (or TRADES_CLOSED_STREAM env, fallback to events:trades for backward compat)
    stream_name = stream or os.getenv("TRADES_CLOSED_STREAM") or os.getenv("TRADE_EVENTS_STREAM", "trades:closed")
    
    if since_ts_ms is None:
        if since_hours is not None:
            since_ts_ms = _now_ms() - int(float(since_hours) * 3600.0 * 1000.0)
        else:
            since_ts_ms = _now_ms() - int(168.0 * 3600.0 * 1000.0)  # default 7 days
    
    n, scanned = export_stream(
        r=r,
        stream=stream_name,
        since_ms=since_ts_ms,
        out_path=out_path,
        max_scan=max_scan,
    )
    return n


def export_from_postgres(*, pg_dsn: str, since_ms: int, out_path: str) -> int:
    """
    Fetch trades from Postgres and write to NDJSON.
    Returns number of rows written.
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        raise RuntimeError("psycopg2 not available for Postgres export")
    
    # Convert ms to timestamp for Postgres query
    since_ts_sec = since_ms / 1000.0
    
    sql = """
        SELECT 
            order_id, sid, symbol, direction,
            exit_ts_ms,
            pnl_net as pnl,
            one_r_money as risk_usd,
            r_multiple as r_mult,
            config_json
        FROM trades_closed
        WHERE exit_ts_ms >= %s
        ORDER BY exit_ts_ms ASC
    """
    
    rows = []
    with psycopg2.connect(pg_dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (since_ms,))
        for row in cur:
            # Parse config_json to extract regime/scenario if available
            config = {}
            if row.get("config_json"):
                try:
                    import json
                    config = json.loads(row["config_json"]) if isinstance(row["config_json"], str) else row["config_json"]
                except Exception:
                    pass
            
            # Normalize sid to canonical format
            symbol_str = str(row.get("symbol") or "").upper()
            raw_sid = str(row.get("sid") or "")
            exit_ts_ms = int(row.get("exit_ts_ms") or 0)
            normalized_sid = _normalize_sid(raw_sid, symbol=symbol_str, ts_ms=exit_ts_ms)
            
            # Build normalized row matching stream export format
            out_row = {
                "ts_ms": exit_ts_ms,
                "exit_ts_ms": exit_ts_ms,
                "symbol": symbol_str,
                "sid": normalized_sid,  # Normalized canonical sid
                "event_id": str(row.get("order_id") or ""),
                "pnl": _safe_float(row.get("pnl"), 0.0),
                "risk_usd": _safe_float(row.get("risk_usd"), 0.0),
                "r_mult": _safe_float(row.get("r_mult"), 0.0),
                "regime": str(config.get("regime") or "na").lower(),
                "regime_group": str(config.get("regime_group") or config.get("regime") or "na").lower(),
                "scenario": str(config.get("scenario") or "").lower(),
                "scenario_v4": str(config.get("scenario_v4") or "").lower(),
                "ab_arm": "A",  # Default, not stored in trades_closed
                "ab_group": "default",
                "arm_ver": 0,
                "book_health_ok": 0,
                "dn_tier": 0,
                "abs_lvl_tier": 0,
                "of_confirm_ok": 0,
                "of_confirm_ok_soft": 0,
                "meta_enforce_applied": None,
                "meta_veto": 0,
                "meta_enforce_key": "",
                "meta_enforce_salt": "enf_v1",
            }
            rows.append(out_row)
    
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=168.0)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--stream", type=str, default=os.getenv("TRADES_CLOSED_STREAM") or os.getenv("TRADE_EVENTS_STREAM", "trades:closed"))
    ap.add_argument("--redis-url", type=str, default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=500_000)
    ap.add_argument("--pg-dsn", type=str, default=os.getenv("ANALYTICS_DB_DSN", ""))
    ap.add_argument("--pg-fallback", type=int, default=1, help="Use Postgres if Redis yields < this many trades")
    args = ap.parse_args()

    since_ms = _now_ms() - int(float(args.since_hours) * 3600.0 * 1000.0)
    r = redis.from_url(args.redis_url, decode_responses=True)
    n, scanned = export_stream(r=r, stream=args.stream, since_ms=since_ms, out_path=args.out, max_scan=int(args.max_scan))
    
    # Fallback to Postgres if Redis returned insufficient data
    if n < args.pg_fallback and args.pg_dsn:
        print(f"Redis returned {n} trades, falling back to Postgres (dsn={args.pg_dsn[:30]}...)")
        n = export_from_postgres(pg_dsn=args.pg_dsn, since_ms=since_ms, out_path=args.out)
        print(f"Postgres: written={n} out={args.out}")
    else:
        print(f"written={n} scanned={scanned} out={args.out}")


if __name__ == "__main__":
    main()
