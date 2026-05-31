#!/usr/bin/env python3
from __future__ import annotations

"""
Smoke tool: Tick-side quality & time sanity

Goal
- Quickly validate that UNKNOWN side handling is sane after Step7/Step8/Step10+
- Quantify: side_conf distribution, UNKNOWN share, ts_source distribution, time skew/lag
- Optionally sample quarantine stream counts (Step8)

Usage
  export REDIS_URL=redis://redis-worker-1:6379/0
  python -m tools.smoke_tick_side_quality --ticks-stream ticks --hours 1 --limit 20000
  python -m tools.smoke_tick_side_quality --ticks-stream ticks --quarantine-stream stream:tick_side:quarantine --hours 6

Notes
- Works with Redis Streams (XREVRANGE) and expects tick fields similar to Step7:
  symbol, side, side_conf, event_ts_ms, ts_source, stream_id/stream_ms (optional)
"""

import argparse
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _b2s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return repr(x)
    return str(x)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        s = _b2s(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        s = _b2s(x).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _percentile(sorted_vals: list[float], q: float) -> float:
    # q in [0,1]
    if not sorted_vals:
        return 0.0
    if q <= 0:
        return float(sorted_vals[0])
    if q >= 1:
        return float(sorted_vals[-1])
    # nearest-rank linear interpolation
    n = len(sorted_vals)
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    w = pos - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


@dataclass(frozen=True)
class TickSample:
    symbol: str
    side: str
    side_conf: str
    ts_source: str
    event_ts_ms: int
    stream_ms: int
    now_ms: int


def parse_tick_fields(fields: dict[Any, Any], *, msg_id: str | None = None, now_ms: int | None = None) -> TickSample:
    now = int(now_ms if now_ms is not None else get_ny_time_millis())

    def _get_val(k_base: str):
        # try string key, then byte key
        return fields.get(k_base) or fields.get(k_base.encode("utf-8"))

    symbol = _b2s(_get_val("symbol") or _get_val("s") or "").upper()

    side = _b2s(_get_val("side") or "").upper()
    side_conf = _b2s(_get_val("side_conf") or "").lower()
    ts_source = _b2s(_get_val("ts_source") or "").lower()

    event_ts_ms = _safe_int(_get_val("event_ts_ms") or _get_val("ts_ms") or _get_val("E") or 0, 0)

    # stream_ms priority: explicit field or derive from msg_id
    stream_ms = _safe_int(_get_val("stream_ms") or 0, 0)
    if stream_ms <= 0 and msg_id:
        try:
            stream_ms = int(str(msg_id).split("-")[0])
        except Exception:
            stream_ms = 0

    return TickSample(
        symbol=symbol,
        side=side or "",
        side_conf=side_conf or "",
        ts_source=ts_source or "",
        event_ts_ms=int(event_ts_ms),
        stream_ms=int(stream_ms),
        now_ms=now,
    )


def summarize_ticks(samples: Iterable[TickSample], *, max_ts_skew_ms: int = 60_000) -> dict[str, Any]:
    """
    Returns dict suitable for JSON output.
    max_ts_skew_ms: threshold to flag large skew between event_ts_ms and stream_ms.
    """
    total = 0
    by_side_conf: dict[str, int] = {}
    by_ts_source: dict[str, int] = {}
    by_side: dict[str, int] = {}
    by_symbol: dict[str, int] = {}

    abs_event_stream: list[float] = []
    abs_now_event: list[float] = []
    skew_gt: int = 0
    missing_event_ts: int = 0

    for s in samples:
        total += 1
        by_symbol[s.symbol] = by_symbol.get(s.symbol, 0) + 1

        sc = s.side_conf or "missing"
        by_side_conf[sc] = by_side_conf.get(sc, 0) + 1

        ts = s.ts_source or "missing"
        by_ts_source[ts] = by_ts_source.get(ts, 0) + 1

        side = s.side or "missing"
        by_side[side] = by_side.get(side, 0) + 1

        if s.event_ts_ms <= 0:
            missing_event_ts += 1
            continue

        if s.stream_ms > 0:
            d = abs(float(s.event_ts_ms - s.stream_ms))
            abs_event_stream.append(d)
            if d > float(max_ts_skew_ms):
                skew_gt += 1

        abs_now_event.append(abs(float(s.now_ms - s.event_ts_ms)))

    abs_event_stream.sort()
    abs_now_event.sort()

    def _stats(vals: list[float]) -> dict[str, Any]:
        if not vals:
            return {"n": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
        return {
            "n": len(vals),
            "p50_ms": _percentile(vals, 0.50),
            "p95_ms": _percentile(vals, 0.95),
            "p99_ms": _percentile(vals, 0.99),
            "max_ms": float(vals[-1]),
        }

    return {
        "n": total,
        "by_symbol_top": dict(sorted(by_symbol.items(), key=lambda kv: kv[1], reverse=True)[:20]),
        "by_side_conf": dict(sorted(by_side_conf.items(), key=lambda kv: kv[1], reverse=True)),
        "by_ts_source": dict(sorted(by_ts_source.items(), key=lambda kv: kv[1], reverse=True)),
        "by_side": dict(sorted(by_side.items(), key=lambda kv: kv[1], reverse=True)),
        "missing_event_ts": missing_event_ts,
        "abs_event_stream_skew": _stats(abs_event_stream),
        "abs_now_event_lag": _stats(abs_now_event),
        "event_stream_skew_gt_threshold": {"threshold_ms": int(max_ts_skew_ms), "count": skew_gt},
    }


def _redis_connect(redis_url: str):
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError("redis library is not installed in this environment") from e
    return redis.Redis.from_url(redis_url, decode_responses=False)


def _xrevrange(r, stream: str, *, min_id: str, max_id: str, count: int) -> list[tuple[str, dict[Any, Any]]]:
    # compat wrapper
    return r.xrevrange(stream, max=max_id, min=min_id, count=count)


def _ms_id(ms: int) -> str:
    return f"{int(ms)}-0"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Smoke: tick side quality & time sanity from Redis Streams")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""), help="Redis URL (default: env REDIS_URL)")
    ap.add_argument("--ticks-stream", default=os.getenv("TICKS_STREAM", "ticks"), help="Ticks stream name")
    ap.add_argument("--quarantine-stream", default=os.getenv("TICK_SIDE_QUARANTINE_STREAM", ""), help="Quarantine stream name (optional)")
    ap.add_argument("--hours", type=float, default=1.0, help="Lookback window in hours (default 1)")
    ap.add_argument("--limit", type=int, default=20000, help="Max entries to read from each stream (default 20000)")
    ap.add_argument("--max-ts-skew-ms", type=int, default=int(os.getenv("CRYPTO_OF_MAX_TS_SKEW_MS", "60000")), help="Skew threshold")
    ap.add_argument("--output", choices=["json", "pretty"], default="pretty", help="Output format")
    args = ap.parse_args(argv)

    if not args.redis_url:
        print("ERROR: missing --redis-url (or REDIS_URL env)", file=sys.stderr)
        return 2

    now_ms = int(get_ny_time_millis())
    min_ms = int(now_ms - args.hours * 3600.0 * 1000.0)

    r = _redis_connect(args.redis_url)

    ticks: list[TickSample] = []
    try:
        entries = _xrevrange(r, args.ticks_stream, min_id=_ms_id(min_ms), max_id="+", count=int(args.limit))
        for msg_id, fields in entries:
            ticks.append(parse_tick_fields(fields, msg_id=msg_id, now_ms=now_ms))
    except Exception as e:
        print(f"ERROR reading ticks stream '{args.ticks_stream}': {e}", file=sys.stderr)
        return 1

    out: dict[str, Any] = {
        "window": {"hours": float(args.hours), "min_ms": min_ms, "now_ms": now_ms},
        "ticks_stream": args.ticks_stream,
        "ticks": summarize_ticks(ticks, max_ts_skew_ms=int(args.max_ts_skew_ms)),
    }

    if args.quarantine_stream:
        try:
            q_entries = _xrevrange(r, args.quarantine_stream, min_id=_ms_id(min_ms), max_id="+", count=int(args.limit))
            out["quarantine_stream"] = args.quarantine_stream
            out["quarantine_count"] = len(q_entries)
            # quick reason distribution
            by_reason: dict[str, int] = {}
            for _id, f in q_entries:
                reason = _b2s(f.get("reason") or "missing")
                by_reason[reason] = by_reason.get(reason, 0) + 1
            out["quarantine_by_reason"] = dict(sorted(by_reason.items(), key=lambda kv: kv[1], reverse=True))
        except Exception as e:
            out["quarantine_error"] = str(e)

    if args.output == "json":
        sys.stdout.write(json.dumps(out, ensure_ascii=False))
        sys.stdout.write("\n")
    else:
        sys.stdout.write(json.dumps(out, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
