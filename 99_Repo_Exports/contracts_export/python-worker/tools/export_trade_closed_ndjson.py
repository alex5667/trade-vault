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

import argparse
import json
import os
import time
from typing import Any, Dict, List, Tuple, Optional, Union

import redis


def _now_ms() -> int:
    return int(time.time() * 1000)


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


def _is_position_closed(fields: Dict[str, Any]) -> bool:
    et = str(fields.get("event_type") or fields.get("event") or "").upper()
    return et == "POSITION_CLOSED" or et == "CLOSE"


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
        # xrevrange returns (id, fields) newest->older
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
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

            row = {
                "ts_ms": ts_ms,
                "exit_ts_ms": ts_ms,  # Alias for compatibility
                "symbol": str(fields.get("symbol") or "").upper(),
                "sid": str(fields.get("sid") or ""),
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
                # meta enforce fields (for ramp evaluation and Stage2 optimization)
                "meta_enforce_applied": _safe_int(fields.get("meta_enforce_applied") or None),
                "meta_veto": _safe_int(fields.get("meta_veto") or meta.get("meta_veto") or 0),
                "meta_enforce_key": str(fields.get("meta_enforce_key") or meta.get("meta_enforce_key") or ""),
                "meta_enforce_salt": str(fields.get("meta_enforce_salt") or meta.get("meta_enforce_salt") or "enf_v1"),
            }
            rows.append(row)

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
    
    stream_name = stream or os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=168.0)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--stream", type=str, default=os.getenv("TRADE_EVENTS_STREAM", "events:trades"))
    ap.add_argument("--redis-url", type=str, default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=500_000)
    args = ap.parse_args()

    since_ms = _now_ms() - int(float(args.since_hours) * 3600.0 * 1000.0)
    r = redis.from_url(args.redis_url, decode_responses=True)
    n, scanned = export_stream(r=r, stream=args.stream, since_ms=since_ms, out_path=args.out, max_scan=int(args.max_scan))
    print(f"written={n} scanned={scanned} out={args.out}")


if __name__ == "__main__":
    main()
