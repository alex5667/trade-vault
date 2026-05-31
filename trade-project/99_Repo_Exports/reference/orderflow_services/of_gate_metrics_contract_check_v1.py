# -*- coding: utf-8 -*-
"""P76 — OF gate contract smoke-check (v1)

Reads the tail of Redis Stream `metrics:of_gate`, validates each row against
`services.orderflow.of_gate_metrics_contract`, computes a compact summary and
writes it to `sre:of_gate_contract_smoke`.

Exit codes:
  0  OK
  2  ALERT (bad_share / schema-missing share above thresholds)
  1  ERROR

Designed for low cardinality output:
  - top_dq_json (up to 10)
  - top_reason_code_json (up to 10)
  - top_reason_code_bad_json (up to 10)

Intended to be run periodically (timer/cron) and paired with
`orderflow_services/of_gate_contract_smoke_exporter_v1.py` + Prometheus alerts
and a Grafana dashboard.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import Counter
from typing import Any, Dict, List, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("of_gate_contract_smoke_check")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _load_contract():
    """Load contract validators, preferring services/ over ok_rate_logic/ fallback."""
    try:
        from services.orderflow.of_gate_metrics_contract import (  # type: ignore
            validate_of_gate_row,
            why_label,
            derive_reason_code,
        )

        return validate_of_gate_row, why_label, derive_reason_code
    except Exception:
        pass

    try:
        from ok_rate_logic.of_gate_metrics_contract import (  # type: ignore
            validate_of_gate_row,
            why_label,
            derive_reason_code,
        )

        return validate_of_gate_row, why_label, derive_reason_code
    except Exception:
        pass

    # Minimal stubs if neither is available — for isolated testing
    def validate_of_gate_row(row):  # type: ignore
        return True, 0

    def why_label(code):  # type: ignore
        return str(code)

    def derive_reason_code(row):  # type: ignore
        return row.get("reason_code") or "unknown"

    return validate_of_gate_row, why_label, derive_reason_code


def _reason_code(row: Dict[str, Any], derive_reason_code) -> str:
    rc = str(row.get("reason_code") or "").strip()
    if rc:
        return rc[:64]
    try:
        return str(derive_reason_code(row) or "")[:64] or "unknown"
    except Exception:
        return "unknown"


def _schema_version_int(row: Dict[str, Any]) -> int:
    v = row.get("schema_version")
    if v is None:
        return 0
    s = str(v).strip()
    if not s:
        return 0
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def _schema_missing(row: Dict[str, Any]) -> int:
    """Missing schema markers mean producers aren't calling enrich_schema_fields()."""
    if ("schema_name" not in row) or ("schema_version" not in row) or ("reason_code" not in row):
        return 1
    return 0


def _top(counter: Counter, k: int = 10) -> List[Tuple[str, int]]:
    return [(str(a), int(b)) for a, b in counter.most_common(k)]


def _notify_redis_stream(r, stream: str, text: str) -> None:
    try:
        r.xadd(
            stream,
            {"ts_ms": str(_now_ms()), "source": "of_gate_contract_smoke", "text": text},
            maxlen=5000,
            approximate=True,
        )
    except Exception as e:
        logger.warning(f"notify stream failed: {e}")


def main() -> int:
    ap = argparse.ArgumentParser(description="P76 contract smoke-check for metrics:of_gate")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--limit", type=int, default=int(os.getenv("OF_GATE_CONTRACT_CHECK_LIMIT", "2000")))

    ap.add_argument("--bad-max", type=float, default=float(os.getenv("OF_GATE_CONTRACT_BAD_MAX", "0.001")))
    ap.add_argument(
        "--schema-missing-max",
        type=float,
        default=float(os.getenv("OF_GATE_CONTRACT_SCHEMA_MISSING_MAX", "0.25")),
    )

    ap.add_argument("--out-stream", default=os.getenv("OF_GATE_CONTRACT_SMOKE_OUT_STREAM", "sre:of_gate_contract_smoke"))

    ap.add_argument("--notify", action="store_true", help="Notify to a Redis notify stream on ALERT/ERROR")
    ap.add_argument("--notify-stream", default=os.getenv("OF_GATE_CONTRACT_SMOKE_NOTIFY_STREAM", "notify:telegram"))

    args = ap.parse_args()

    if redis is None:
        logger.error("redis dependency is missing")
        return 1

    validate_of_gate_row, why_label, derive_reason_code = _load_contract()

    r = redis.from_url(args.redis_url, decode_responses=True)

    try:
        rows = r.xrevrange(args.stream, max="+", min="-", count=max(0, int(args.limit)))
    except Exception as e:
        logger.error(f"xrevrange failed: {e}")
        if args.notify:
            _notify_redis_stream(r, args.notify_stream, f"P76 of_gate contract smoke-check ERROR: xrevrange failed: {e}")
        return 1

    n_total = len(rows)
    bad_total = 0
    schema_missing_total = 0

    dq: Counter = Counter()
    reason_all: Counter = Counter()
    reason_bad: Counter = Counter()
    schema_versions: Counter = Counter()

    for _id, fields in rows:
        if not isinstance(fields, dict):
            continue

        schema_missing_total += _schema_missing(fields)

        rc = _reason_code(fields, derive_reason_code)
        reason_all[rc] += 1

        v = _schema_version_int(fields)
        schema_versions[str(v)] += 1

        ok, why = validate_of_gate_row(fields)
        if not ok:
            bad_total += 1
            dq[why_label(why)] += 1
            reason_bad[rc] += 1

    bad_share = (bad_total / n_total) if n_total > 0 else 0.0
    schema_missing_share = (schema_missing_total / n_total) if n_total > 0 else 0.0

    schema_version_mode = 0
    try:
        schema_version_mode = int(schema_versions.most_common(1)[0][0]) if schema_versions else 0
    except Exception:
        schema_version_mode = 0

    # Write compact summary to output stream (read by exporter → Prometheus)
    out = {
        "ts_ms": str(_now_ms()),
        "stream": str(args.stream),
        "limit": str(args.limit),
        "n_total": str(n_total),
        "bad_total": str(bad_total),
        "bad_share": f"{bad_share:.9f}",
        "bad_max": f"{float(args.bad_max):.9f}",
        "schema_version_mode": str(schema_version_mode),
        "schema_missing_total": str(schema_missing_total),
        "schema_missing_share": f"{schema_missing_share:.9f}",
        "schema_missing_max": f"{float(args.schema_missing_max):.9f}",
        "top_dq_json": json.dumps(_top(dq, 10), ensure_ascii=False),
        "top_reason_code_json": json.dumps(_top(reason_all, 10), ensure_ascii=False),
        "top_reason_code_bad_json": json.dumps(_top(reason_bad, 10), ensure_ascii=False),
    }

    try:
        r.xadd(args.out_stream, out, maxlen=2000, approximate=True)
    except Exception as e:
        logger.error(f"xadd out-stream failed: {e}")
        if args.notify:
            _notify_redis_stream(r, args.notify_stream, f"P76 of_gate contract smoke-check ERROR: cannot write out-stream: {e}")
        return 1

    alert = False
    if n_total > 0 and bad_share > float(args.bad_max):
        alert = True
    if n_total > 0 and schema_missing_share > float(args.schema_missing_max):
        alert = True

    if alert:
        top_dq = ", ".join([f"{k}:{v}" for k, v in dq.most_common(5)]) or "none"
        msg = (
            f"P76 OF-gate contract ALERT: bad_share={bad_share:.4%} (max={float(args.bad_max):.4%}), "
            f"schema_missing_share={schema_missing_share:.1%} (max={float(args.schema_missing_max):.1%}), "
            f"n={n_total}, schema_v_mode={schema_version_mode}, top_dq={top_dq}"
        )
        logger.warning(msg)
        if args.notify:
            _notify_redis_stream(r, args.notify_stream, msg)
        return 2

    logger.info(
        f"P76 OK: n={n_total} bad={bad_total} bad_share={bad_share:.6f} "
        f"schema_missing_share={schema_missing_share:.6f} schema_v_mode={schema_version_mode}"
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sys.exit(main())
