#!/usr/bin/env python3
from __future__ import annotations

from domain.evidence_keys import MetaKeys

"""of_gate_metrics_contract_check_v1.py

P40: Producer-side contract check for metrics:of_gate.

Why
----
Meta coverage ops (preflight/rollout/quarantine) depend on metrics:of_gate entries
having required top-level fields:
  - meta_feature_coverage
  - meta_enforce_cov_bucket

This tool is safe to run from SRE timers. It returns:
  0: OK
  2: Soft block (insufficient data / missing fields)
  1: Hard fail (Redis/infrastructure error)
"""


import argparse
import os
import sys
from typing import Any

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _to_bool(x: str) -> bool:
    return str(x).strip().lower() in ("1", "true", "yes", "on")


def _f(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--stream", default=os.getenv("META_COV_SOURCE_STREAM", "metrics:of_gate"))
    p.add_argument("--count", type=int, default=int(os.getenv("OF_GATE_CONTRACT_COUNT", "500")))
    p.add_argument("--min-ok", type=int, default=int(os.getenv("OF_GATE_CONTRACT_MIN_OK", "50")))
    p.add_argument("--strict", type=int, default=int(os.getenv("OF_GATE_CONTRACT_STRICT", "0")))
    args = p.parse_args()

    if redis is None:
        print("redis package not installed", file=sys.stderr)
        return 1

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True, socket_timeout=5.0)

    try:
        # Ping for infra sanity
        r.ping()
    except Exception as e:
        print(f"hard_fail redis_ping: {e}", file=sys.stderr)
        return 1

    try:
        rows: list[tuple[str, dict[str, Any]]] = r.xrevrange(args.stream, count=args.count) or []
    except Exception as e:
        print(f"hard_fail xrevrange stream={args.stream}: {e}", file=sys.stderr)
        return 1

    if len(rows) < args.min_ok:
        print(
            f"soft_block insufficient_data stream={args.stream} n={len(rows)} need>={args.min_ok}",
            file=sys.stderr,
        )
        return 2

    required = ["meta_feature_coverage", "meta_enforce_cov_bucket"]
    missing_any = 0
    bad_bucket = 0
    bad_cov = 0

    for _msg_id, f in rows[: args.min_ok]:
        for k in required:
            if k not in f:
                missing_any += 1
                break

        b = (f.get(MetaKeys.ENFORCE_COV_BUCKET) or "").strip().lower()
        if b and b not in ("trend", "range", "other"):
            bad_bucket += 1

        cov = _f(f.get(MetaKeys.FEATURE_COVERAGE))
        if cov is None or cov < 0.0 or cov > 1.0:
            bad_cov += 1

    ok = (missing_any == 0) and (bad_bucket == 0) and (bad_cov == 0)
    if ok:
        print(
            f"ok stream={args.stream} checked={args.min_ok} missing={missing_any} bad_bucket={bad_bucket} bad_cov={bad_cov}"
        )
        return 0

    msg = (
        f"soft_block stream={args.stream} checked={args.min_ok} "
        f"missing={missing_any} bad_bucket={bad_bucket} bad_cov={bad_cov}"
    )
    if _to_bool(args.strict):
        print("hard_fail " + msg, file=sys.stderr)
        return 1

    print(msg, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
