#!/usr/bin/env python3
from __future__ import annotations

"""
check_ml_confirm_stream_health.py

Health-check for Redis stream metrics:ml_confirm *content* (not just length).
Exit codes:
  0 = OK
  2 = FAIL (action required)
Prints JSON by default.

Designed to be safe in prod:
- bounded XREVRANGE
- tolerant to different payload layouts: {payload: json}, or flat fields.
- does not require any project imports.
"""
import argparse
import json
import os
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        return v
    return v


def _parse_entry(fields: dict[bytes, bytes]) -> dict[str, Any]:
    # Convert bytes->str and try to parse payload if present
    out: dict[str, Any] = {}
    payload_obj: dict[str, Any] | None = None
    for kb, vb in fields.items():
        k = kb.decode("utf-8", "replace")
        out[k] = _loads_maybe_json(vb)
    if "payload" in out and isinstance(out["payload"], dict):
        payload_obj = out["payload"]
    elif "json" in out and isinstance(out["json"], dict):
        payload_obj = out["json"]
    # If payload exists, merge it (payload keys win to reflect actual schema)
    if payload_obj:
        merged = dict(out)
        merged.update(payload_obj)
        return merged
    return out


def _get_float(d: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for k in keys:
        if k in d:
            try:
                return float(d[k])
            except Exception:
                continue
    return default


def _get_int(d: dict[str, Any], *keys: str, default: int = 0) -> int:
    for k in keys:
        if k in d:
            try:
                return int(float(d[k]))
            except Exception:
                continue
    return default


def compute_health(samples: list[dict[str, Any]], now_ms: int, max_stale_ms: int) -> tuple[bool, dict[str, Any]]:
    n = len(samples)
    if n == 0:
        return False, {
            "ok": False,
            "reason": "empty_stream",
            "n": 0,
        }

    # Determine "ts_ms" for staleness
    last = samples[0]
    ts_ms = _get_int(last, "ts_ms", "ts", "t_ms", default=0)
    if ts_ms <= 0:
        # best-effort fallback: some streams carry unix seconds
        ts_s = _get_int(last, "ts_s", default=0)
        if ts_s > 0:
            ts_ms = ts_s * 1000
    stale_ms = (now_ms - ts_ms) if ts_ms > 0 else None

    # Required keys (soft): status/p_edge/conf/latency/missing_n
    miss_required = 0
    zero_p_edge = 0
    err_count = 0
    abstain_count = 0
    allow_count = 0
    status_counts: dict[str, int] = {}

    for s in samples:
        status = str(s.get("status") or s.get("st") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        if status.lower() in ("err", "error", "fail"):
            err_count += 1

        p_edge = _get_float(s, "p_edge", "p", default=float("nan"))
        conf = _get_float(s, "conf", "confidence", default=float("nan"))
        lat_ms = _get_float(s, "lat_ms", "latency_ms", "latency", default=float("nan"))
        missing_n = _get_int(s, "missing_n", default=-1)

        # missing required: any of these unavailable
        if not (p_edge == p_edge) or not (conf == conf) or not (lat_ms == lat_ms) or missing_n < 0:
            miss_required += 1

        if p_edge == 0.0:
            zero_p_edge += 1

        if _get_int(s, "abstain") == 1 or status.lower() == "abstain":
            abstain_count += 1
        if _get_int(s, "allow") == 1:
            allow_count += 1

    miss_rate = miss_required / max(1, n)
    zero_rate = zero_p_edge / max(1, n)
    err_rate = err_count / max(1, n)
    abstain_rate = abstain_count / max(1, n)
    allow_rate = allow_count / max(1, n)

    ok = True
    reasons = []
    if stale_ms is not None and stale_ms > max_stale_ms:
        ok = False
        reasons.append(f"stale_stream_ms>{max_stale_ms}")
    if miss_rate > 0.30:
        ok = False
        reasons.append("missing_required_rate>0.30")
    if zero_rate > 0.95:
        ok = False
        reasons.append("p_edge_zero_rate>0.95")
    if err_rate > 0.20:
        ok = False
        reasons.append("err_rate>0.20")

    return ok, {
        "ok": ok,
        "reason": "ok" if ok else ";".join(reasons),
        "n": n,
        "stale_ms": stale_ms,
        "missing_required_rate": round(miss_rate, 4),
        "p_edge_zero_rate": round(zero_rate, 4),
        "err_rate": round(err_rate, 4),
        "abstain_rate": round(abstain_rate, 4),
        "allow_rate": round(allow_rate, 4),
        "status_counts": status_counts,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL") or os.getenv("TB_REDIS_URL") or "redis://localhost:6379/0")
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM") or "metrics:ml_confirm")
    ap.add_argument("--count", type=int, default=int(os.getenv("ML_CONFIRM_HEALTH_COUNT") or "500"))
    ap.add_argument("--max_stale_ms", type=int, default=int(os.getenv("ML_CONFIRM_MAX_STALE_MS") or "120000"))
    ap.add_argument("--json", action="store_true", help="print JSON (default true)")
    args = ap.parse_args()

    if redis is None:
        out = {"ok": False, "reason": "redis_python_not_installed"}
        print(json.dumps(out, ensure_ascii=False))
        return 2

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    try:
        entries = r.xrevrange(args.stream, max="+", min="-", count=args.count)
    except Exception as e:
        out = {"ok": False, "reason": f"redis_error:{type(e).__name__}"}
        print(json.dumps(out, ensure_ascii=False))
        return 2

    samples = []
    for _id, fields in entries:
        try:
            parsed = _parse_entry(fields)
            samples.append(parsed)
        except Exception:
            continue

    ok, report = compute_health(samples, _now_ms(), args.max_stale_ms)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
