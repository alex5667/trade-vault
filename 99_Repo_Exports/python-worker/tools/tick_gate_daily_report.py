#!/usr/bin/env python3
from __future__ import annotations
"""Daily report for tick-quality gate outcomes stored in Redis Streams.

Reads ops:tick_quality_gate (or configured stream) and produces a compact summary:
 - PASS/FAIL/INSUFFICIENT/ERROR counts for a window (default 24h)
 - top failing metrics / reasons
 - optional by-symbol breakdown (if symbol is present in payload)

Designed to be robust to schema variations:
 - If stream entry contains a field 'json' (or 'payload') with JSON, it is parsed.
 - Otherwise, the entry fields are treated as a flat dict.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list, int, float, bool)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return None
    if not isinstance(v, str):
        try:
            v = str(v)
        except Exception:
            return None
    s = v.strip()
    if not s:
        return None
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return v
    return v


def _norm_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return default
    try:
        s = str(v).strip()
    except Exception:
        return default
    return s if s else default


def _safe_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return default
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _now_ms() -> int:
    return get_ny_time_millis()


def _parse_msg_id_ms(msg_id: str) -> int:
    # Redis stream id: "<ms>-<seq>"
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


@dataclass(frozen=True)
class GateEvent:
    msg_id: str
    ts_ms: int
    status: str  # PASS|FAIL|INSUFFICIENT_DATA|ERROR|UNKNOWN
    return_code: int
    symbol: str
    failures: List[Dict[str, Any]]
    raw: Dict[str, Any]


def _extract_event_from_fields(msg_id: str, fields: Dict[str, Any]) -> GateEvent:
    # Prefer JSON payload if present
    payload = None
    for k in ("json", "payload", "report", "gate", "result"):
        if k in fields:
            payload = _loads_maybe_json(fields.get(k))
            if isinstance(payload, dict):
                break
    if not isinstance(payload, dict):
        payload = {}

    merged: Dict[str, Any] = {}
    # Merge: JSON payload first, then flat fields override
    merged.update(payload)
    merged.update(fields)

    ts_ms = _safe_int(merged.get("ts_ms") or merged.get("time_ms") or merged.get("timestamp_ms"), 0)
    if ts_ms <= 0:
        ts_ms = _parse_msg_id_ms(msg_id)

    status = _norm_str(merged.get("status") or merged.get("state"), "UNKNOWN").upper()
    # Normalize common variants
    if status in ("OK", "PASSING", "SUCCESS"):
        status = "PASS"
    if status in ("FAILED", "FAILURE"):
        status = "FAIL"
    if status in ("INSUFFICIENT", "NO_DATA", "INSUFFICIENTDATA"):
        status = "INSUFFICIENT_DATA"
    if status not in ("PASS", "FAIL", "INSUFFICIENT_DATA", "ERROR"):
        # Fallback from return_code if present
        rc = _safe_int(merged.get("return_code") or merged.get("code"), 0)
        if rc == 0:
            status = "PASS"
        elif rc in (2, 20):
            status = "FAIL"
        elif rc in (1, 21):
            status = "INSUFFICIENT_DATA"
        elif rc:
            status = "ERROR"

    return_code = _safe_int(merged.get("return_code") or merged.get("code"), 0)
    symbol = _norm_str(merged.get("symbol") or merged.get("sym"), "")

    failures_raw = merged.get("failures") or merged.get("failed") or merged.get("reasons") or merged.get("violations")
    failures: List[Dict[str, Any]] = []
    fr = _loads_maybe_json(failures_raw)
    if isinstance(fr, list):
        for x in fr:
            if isinstance(x, dict):
                failures.append(x)
            else:
                failures.append({"reason": _norm_str(x)})
    elif isinstance(fr, dict):
        # Sometimes failures is a dict of metric->info
        for mk, mv in fr.items():
            if isinstance(mv, dict):
                d = dict(mv)
                d.setdefault("metric", mk)
                failures.append(d)
            else:
                failures.append({"metric": _norm_str(mk), "value": mv})

    return GateEvent(
        msg_id=str(msg_id),
        ts_ms=int(ts_ms),
        status=status,
        return_code=int(return_code),
        symbol=symbol,
        failures=failures,
        raw=merged,
    )


def _within_window(ev: GateEvent, *, start_ms: int) -> bool:
    return ev.ts_ms >= start_ms


def _metric_name_from_failure(f: Dict[str, Any]) -> str:
    # Prefer explicit key, fallback to reason string parsing
    metric = _norm_str(f.get("metric") or f.get("name"), "")
    if metric:
        return metric
    reason = _norm_str(f.get("reason") or f.get("msg") or f.get("message"), "")
    if reason:
        return reason[:96]
    return "unknown"


def aggregate_events(events: Iterable[GateEvent]) -> Dict[str, Any]:
    counts = {"PASS": 0, "FAIL": 0, "INSUFFICIENT_DATA": 0, "ERROR": 0, "UNKNOWN": 0}
    fail_metrics: Dict[str, int] = {}
    by_symbol: Dict[str, Dict[str, int]] = {}
    latest_ms = 0

    for ev in events:
        st = ev.status if ev.status in counts else "UNKNOWN"
        counts[st] = counts.get(st, 0) + 1
        latest_ms = max(latest_ms, ev.ts_ms or 0)

        sym = ev.symbol
        if sym:
            bucket = by_symbol.setdefault(sym, {"PASS": 0, "FAIL": 0, "INSUFFICIENT_DATA": 0, "ERROR": 0, "UNKNOWN": 0})
            bucket[st] = bucket.get(st, 0) + 1

        if st == "FAIL":
            if ev.failures:
                for f in ev.failures:
                    mk = _metric_name_from_failure(f)
                    fail_metrics[mk] = fail_metrics.get(mk, 0) + 1
            else:
                fail_metrics["unknown"] = fail_metrics.get("unknown", 0) + 1

    top_fail_metrics = sorted(fail_metrics.items(), key=lambda kv: (-kv[1], kv[0]))[:25]
    top_symbols = sorted(by_symbol.items(), key=lambda kv: (-kv[1].get("FAIL", 0), -sum(kv[1].values()), kv[0]))[:25]

    return {
        "counts": counts,
        "top_fail_metrics": [{"metric": k, "count": v} for k, v in top_fail_metrics],
        "top_symbols": [{"symbol": s, **c} for s, c in top_symbols],
        "latest_ts_ms": latest_ms,
    }


def _format_text(report: Dict[str, Any], *, window_hours: float) -> str:
    counts = report.get("counts") or {}
    total = sum(int(v) for v in counts.values()) if isinstance(counts, dict) else 0
    lines: List[str] = []
    lines.append(f"Tick Quality Gate — window={window_hours:.2f}h, n={total}")
    lines.append(
        "Counts: " + ", ".join(f"{k}={counts.get(k, 0)}" for k in ("PASS", "FAIL", "INSUFFICIENT_DATA", "ERROR", "UNKNOWN"))
    )
    latest = _safe_int(report.get("latest_ts_ms"), 0)
    if latest > 0:
        lines.append(f"Latest: ts_ms={latest}")
    tfm = report.get("top_fail_metrics") or []
    if tfm:
        lines.append("Top FAIL reasons:")
        for row in tfm[:10]:
            lines.append(f"  - {row.get('metric')}: {row.get('count')}")
    ts = report.get("top_symbols") or []
    if ts:
        lines.append("Top symbols by FAIL:")
        for row in ts[:10]:
            lines.append(
                f"  - {row.get('symbol')}: FAIL={row.get('FAIL', 0)} PASS={row.get('PASS', 0)} INSUFF={row.get('INSUFFICIENT_DATA', 0)}"
            )
    return "\n".join(lines)


def _read_redis_events(
    *,
    redis_url: str,
    stream: str,
    start_ms: int,
    limit: int,
) -> List[GateEvent]:
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError("redis-py is required to read gate stream") from e

    r = redis.Redis.from_url(redis_url, decode_responses=False)
    # Read newest first and filter by timestamp
    raw = r.xrevrange(stream, max="+", min="-", count=int(limit))
    events: List[GateEvent] = []
    for msg_id_b, fields in raw:
        msg_id = msg_id_b.decode("utf-8", errors="replace") if isinstance(msg_id_b, (bytes, bytearray)) else str(msg_id_b)
        # normalize fields to str->Any
        f2: Dict[str, Any] = {}
        for k, v in (fields or {}).items():
            kk = k.decode("utf-8", errors="replace") if isinstance(k, (bytes, bytearray)) else str(k)
            f2[kk] = v
        ev = _extract_event_from_fields(msg_id, f2)
        if ev.ts_ms < start_ms:
            # since xrevrange returns newest->oldest, we can stop once we are below window
            break
        events.append(ev)
    return events


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--stream", default=os.getenv("TICK_GATE_REDIS_STREAM", "ops:tick_quality_gate"))
    p.add_argument("--hours", type=float, default=float(os.getenv("TICK_GATE_REPORT_HOURS", "24")))
    p.add_argument("--limit", type=int, default=int(os.getenv("TICK_GATE_REPORT_LIMIT", "20000")))
    p.add_argument("--format", choices=["text", "json"], default="text")
    args = p.parse_args(argv)

    start_ms = _now_ms() - int(args.hours * 3600 * 1000)
    try:
        events = _read_redis_events(redis_url=args.redis_url, stream=args.stream, start_ms=start_ms, limit=args.limit)
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        return 2

    report = aggregate_events(events)
    if args.format == "json":
        out = {
            "window_hours": args.hours,
            "stream": args.stream,
            **report,
        }
        sys.stdout.write(json.dumps(out, ensure_ascii=False, sort_keys=True) + "\n")
    else:
        sys.stdout.write(_format_text(report, window_hours=args.hours) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
