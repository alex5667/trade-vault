#!/usr/bin/env python3
"""Check whether signals:of:inputs has enough OE-group data to resume v14_of Phase 2/3.

Run from the main host (where Redis worker-1 is accessible):
    python -m tools.check_v14_oe_readiness

Exit codes:
    0 — ready (coverage >= threshold and history >= min_days)
    1 — not ready yet (prints reason)
    2 — Redis unreachable
"""
from __future__ import annotations

import json
import os
import sys
import time

# OE group fields that must be present in signals:of:inputs payload
OE_REQUIRED = [
    "exec_cost_to_tp1_ratio",
    "exec_cost_to_sl_ratio",
    "exec_cost_to_atr_ratio",
    "spread_p95_bps_symbol_kind_session",
    "fill_prob_1s",
    "eta_fill_sec",
    "queue_ahead_qty_5",
]

OE_OPTIONAL = [
    "slippage_p95_bps_symbol_kind_session",
    "fill_prob_3s",
    "fill_prob_5s",
    "queue_ahead_qty_l1",
    "queue_ahead_qty_l5",
]

# Thresholds
MIN_COVERAGE = 0.70      # >= 70% of sampled entries must have at least 1 required OE field
MIN_DAYS = 7             # stream must span at least 7 days
SAMPLE_SIZE = 200        # entries to sample from stream tail


def _parse_payload(data: dict) -> dict:
    raw = data.get("payload") or data.get("data") or ""
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return data


def evaluate_readiness(redis_client=None) -> dict:
    """Library-form readiness check. Returns dict (no sys.exit, no print).

    Callers (e.g. v14_of_auto_promote) gate on `ready` field.

    Returns
    -------
    dict with keys:
        ready (bool)
        coverage (float)
        span_days (float)
        sampled (int)
        reasons (list[str]) — non-empty iff not ready
        field_counts (dict)
        error (str) — set on Redis/stream failure (ready=False)
    """
    out: dict = {
        "ready": False,
        "coverage": 0.0,
        "span_days": 0.0,
        "sampled": 0,
        "reasons": [],
        "field_counts": {},
        "min_coverage": MIN_COVERAGE,
        "min_days": MIN_DAYS,
    }
    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    try:
        r = redis_client or _get_redis_or_raise()
    except Exception as e:
        out["error"] = f"redis_unreachable: {e}"
        out["reasons"].append(out["error"])
        return out

    try:
        entries = r.xrevrange(stream, count=SAMPLE_SIZE)
    except Exception as e:
        out["error"] = f"xrevrange_failed: {e}"
        out["reasons"].append(out["error"])
        return out

    if not entries:
        out["error"] = f"stream_empty: {stream}"
        out["reasons"].append(out["error"])
        return out

    now_ms = int(time.time() * 1000)
    oldest_id = entries[-1][0]
    oldest_ms = int(oldest_id.split("-")[0])
    span_days = (now_ms - oldest_ms) / 86_400_000

    oe_hits = 0
    field_counts: dict[str, int] = {k: 0 for k in OE_REQUIRED + OE_OPTIONAL}
    total = len(entries)
    for _xid, data in entries:
        payload = _parse_payload(data)
        ind = payload.get("indicators", payload)
        if not isinstance(ind, dict):
            continue
        if any(k in ind for k in OE_REQUIRED):
            oe_hits += 1
        for k in OE_REQUIRED + OE_OPTIONAL:
            if k in ind:
                field_counts[k] += 1

    coverage = oe_hits / total if total else 0.0
    out.update({
        "coverage": coverage,
        "span_days": span_days,
        "sampled": total,
        "field_counts": field_counts,
        "oldest_id": oldest_id,
    })
    if coverage < MIN_COVERAGE:
        out["reasons"].append(f"coverage {coverage:.1%} < {MIN_COVERAGE:.0%}")
    if span_days < MIN_DAYS:
        out["reasons"].append(f"span {span_days:.1f}d < {MIN_DAYS}d")
    out["ready"] = not out["reasons"]
    return out


def _get_redis_or_raise():
    import redis
    host = os.getenv("REDIS_WORKER_HOST", "localhost")
    port = int(os.getenv("REDIS_WORKER_PORT", "63791"))
    r = redis.Redis(host=host, port=port, decode_responses=True, socket_connect_timeout=3)
    r.ping()
    return r


def main() -> None:
    try:
        r = _get_redis_or_raise()
    except Exception as e:
        host = os.getenv("REDIS_WORKER_HOST", "localhost")
        port = int(os.getenv("REDIS_WORKER_PORT", "63791"))
        print(f"ERROR: Redis unreachable at {host}:{port} — {e}")
        sys.exit(2)

    res = evaluate_readiness(redis_client=r)
    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    if res.get("error", "").startswith("stream_empty"):
        print(f"FAIL: stream '{stream}' is empty or does not exist")
        sys.exit(1)

    total = res["sampled"]
    print(f"stream:       {stream}")
    print(f"sampled:      {total} entries")
    print(f"span:         {res['span_days']:.1f} days (oldest entry {res.get('oldest_id', '-')})")
    print(f"OE coverage:  {res['coverage']:.1%} (≥1 required OE field)")
    print(f"\nPer-field counts (out of {total}):")
    for k in OE_REQUIRED:
        c = res["field_counts"].get(k, 0)
        print(f"  [{'✓' if c > 0 else '✗'}] {k}: {c}")
    print(f"\nOptional OE fields:")
    for k in OE_OPTIONAL:
        c = res["field_counts"].get(k, 0)
        print(f"  [{'✓' if c > 0 else '–'}] {k}: {c}")

    if res["ready"]:
        print(f"\nREADY: coverage={res['coverage']:.1%} >= {MIN_COVERAGE:.0%}, "
              f"span={res['span_days']:.1f}d >= {MIN_DAYS}d")
        print("Next step: run v14_of Phase 2 training — see memory: project_v14_of_oe_canary_pending.md")
        sys.exit(0)
    print(f"\nNOT READY: {'; '.join(res['reasons'])}")
    remaining = max(1, int(MIN_DAYS - res["span_days"]))
    print(f"Check again in {remaining} day(s)")
    sys.exit(1)


if __name__ == "__main__":
    main()
