from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

# -*- coding: utf-8 -*-
"""
Export POSITION_CLOSED events from Redis stream (events:trades) into NDJSON.

Why:
  - TradeMonitor + TradeEventsLogger are the "single source" for closed outcomes.
  - NDJSON is reproducible, append-friendly, and CI-friendly for offline tuning.

Output schema (per line):
  {
    "ts_ms": int,
    "sid": str,
    "symbol": str,
    "regime": str,
    "scenario": str,          # "continuation"|"reversal"|"na"
    "abs_lvl_tier": int,      # -1 if unknown
    "of_confirm_ok": int,     # -1 if unknown
    "ab_arm": str,
    "ab_group": str,
    "arm_ver": int,
    "risk_usd": float,
    "pnl": float,             # pnl_net (as written by events logger)
    "r_mult": float,          # pnl / risk_usd if available, else 0
    "close_reason": str
  }

Usage:
  python tools/export_closed_trades_ndjson.py \
    --redis redis://redis-worker-1:6379/0 \
    --stream events:trades \
    --since-ms 0 \
    --max 200000 \
    --out /tmp/closed_trades.ndjson
"""

import argparse
import json
import os
import time
from collections.abc import Iterable
from typing import Any

import redis


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(v: Any, default: str = "") -> str:
    try:
        if v is None:
            return default
        return str(v)
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _lower(v: str) -> str:
    return (v or "").strip().lower()


def _parse_meta_json(v: Any) -> dict[str, Any]:
    # meta/metadata in stream is stored as JSON string; fail-open.
    try:
        if not v:
            return {}
        if isinstance(v, dict):
            return v
        return json.loads(str(v))
    except Exception:
        return {}


def _iter_xread(redis_url: str, stream: str, start_id: str, count: int) -> Iterable[tuple[str, dict[str, str]]]:
    """
    Yields (msg_id, fields) from XREAD in a loop until no more messages.
    """
    last = start_id
    r = redis.from_url(redis_url, decode_responses=True)
    retry_count = 0
    max_retries = 3

    while True:
        try:
            out = r.xread({stream: last}, count=count)
            retry_count = 0  # Reset on success
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            if retry_count >= max_retries:
                raise
            retry_count += 1
            print(f"WARN: Redis connection lost ({e}). Retrying {retry_count}/{max_retries} in 5s...")
            time.sleep(5)
            r = redis.from_url(redis_url, decode_responses=True)  # Re-resolve IP
            continue

        if not out:
            break
        # out: [(b'stream', [(b'id', {b'k': b'v'})...])]
        _sname, entries = out[0]
        if not entries:
            break
        for mid, fields in entries:
            msg_id = mid.decode() if isinstance(mid, (bytes, bytearray)) else str(mid)
            d: dict[str, str] = {}
            for k, v in (fields or {}).items():
                kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
                vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
                d[kk] = vv
            yield msg_id, d
            last = msg_id


def _is_position_closed(fields: dict[str, str]) -> bool:
    et = _s(fields.get("event_type") or fields.get("event") or "").upper()
    return et == "POSITION_CLOSED"


def _extract_closed(fields: dict[str, str]) -> dict[str, Any] | None:
    """
    Convert a stream record (flat string map) into normalized closed-trade dict.
    Fail-open: returns None if not enough data.
    """
    if not _is_position_closed(fields):
        return None
    sid = _s(fields.get("sid") or "")
    symbol = _s(fields.get("symbol") or "").upper()
    ts_ms = _i(fields.get("ts") or fields.get("ts_ms") or fields.get("timestamp") or 0, 0)
    pnl = _f(fields.get("pnl") or 0.0, 0.0)
    risk_usd = _f(fields.get("risk_usd") or 0.0, 0.0)
    r_mult = _f(fields.get("r_mult") or 0.0, 0.0)
    if r_mult == 0.0 and risk_usd > 0:
        r_mult = pnl / risk_usd

    scenario = _lower(fields.get("scenario") or "na")
    if scenario not in ("continuation", "reversal"):
        scenario = "na"

    # Prefer payload-root fields (already expanded by TradeEventsLogger).
    regime = _lower(fields.get("regime") or "na")
    abs_lvl_tier = _i(fields.get("abs_lvl_tier"), -1)
    of_confirm_ok = _i(fields.get("of_confirm_ok"), -1)
    ab_arm = _s(fields.get("ab_arm") or "").upper()
    ab_group = _lower(fields.get("ab_group") or "default")
    ab_key = _s(fields.get("ab_key") or "")
    arm_ver = _i(fields.get("arm_ver"), 0)

    md = _parse_meta_json(fields.get("meta") or fields.get("metadata"))
    close_reason = _s(fields.get("close_reason") or md.get("close_reason") or "")

    if not symbol or not sid:
        # We still can export, but it will be unusable for grouping.
        # Fail-open: drop.
        return None

    return {
        "ts_ms": int(ts_ms or 0),
        "sid": sid,
        "symbol": symbol,
        "regime": regime or "na",
        "scenario": scenario,
        "abs_lvl_tier": int(abs_lvl_tier),
        "of_confirm_ok": int(of_confirm_ok),
        "ab_arm": ab_arm or "",
        "ab_group": ab_group or "default",
        "ab_key": ab_key,
        "arm_ver": int(arm_ver),
        "risk_usd": float(risk_usd),
        "pnl": float(pnl),
        "r_mult": float(r_mult),
        "close_reason": close_reason,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES))
    ap.add_argument("--since-id", default="0-0", help="Redis stream id (default 0-0)")
    ap.add_argument("--count", type=int, default=2000, help="XREAD batch size")
    ap.add_argument("--max", type=int, default=200000, help="max records to scan")
    ap.add_argument("--out", required=True, help="Output NDJSON file path")
    args = ap.parse_args()

    n = 0
    out_lines: list[str] = []
    for msg_id, fields in _iter_xread(args.redis, args.stream, args.since_id, args.count):
        if n >= int(args.max):
            break
        row = _extract_closed(fields)
        if row is None:
            continue
        out_lines.append(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        n += 1

    with open(args.out, "w", encoding="utf-8") as f:
        for ln in out_lines:
            f.write(ln + "\n")

    print(f"OK exported={n} to={args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
