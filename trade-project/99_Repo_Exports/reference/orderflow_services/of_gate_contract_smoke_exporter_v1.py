# -*- coding: utf-8 -*-
"""P76 — OF gate contract smoke exporter (v1)

Reads the latest record from Redis Stream `sre:of_gate_contract_smoke` and
exports a small set of Prometheus Gauges.

Core:
  - of_gate_contract_smoke_bad_share
  - of_gate_contract_smoke_bad_total
  - of_gate_contract_smoke_n_total
  - of_gate_contract_smoke_schema_version_mode
  - of_gate_contract_smoke_missing_schema_share
  - of_gate_contract_smoke_last_ts_ms

Top breakdowns (top-10 only):
  - of_gate_contract_smoke_dq_bad_total{dq_code}
  - of_gate_contract_smoke_reason_code_total{reason_code}
  - of_gate_contract_smoke_reason_code_bad_total{reason_code}
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any, List, Tuple

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("of_gate_contract_smoke_exporter")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
IN_STREAM = os.getenv("OF_GATE_CONTRACT_SMOKE_OUT_STREAM", "sre:of_gate_contract_smoke")
PORT = int(os.getenv("OF_GATE_CONTRACT_SMOKE_EXPORTER_PORT", "9148"))
INTERVAL_SEC = float(os.getenv("OF_GATE_CONTRACT_SMOKE_EXPORTER_INTERVAL_SEC", "15"))

G_BAD_SHARE = Gauge("of_gate_contract_smoke_bad_share", "Share of invalid rows in metrics:of_gate tail")
G_BAD_TOTAL = Gauge("of_gate_contract_smoke_bad_total", "Invalid rows count in metrics:of_gate tail")
G_N_TOTAL = Gauge("of_gate_contract_smoke_n_total", "Total rows sampled from metrics:of_gate")
G_SCHEMA_V_MODE = Gauge("of_gate_contract_smoke_schema_version_mode", "Mode of schema_version (int) in sampled rows")
G_SCHEMA_MISSING_SHARE = Gauge("of_gate_contract_smoke_missing_schema_share", "Share of rows missing schema markers")
G_LAST_TS_MS = Gauge("of_gate_contract_smoke_last_ts_ms", "Timestamp of the last smoke-check record (ms)")

G_DQ = Gauge("of_gate_contract_smoke_dq_bad_total", "Top DQ codes among invalid rows", ["dq_code"])
G_REASON_ALL = Gauge("of_gate_contract_smoke_reason_code_total", "Top reason_code among all rows", ["reason_code"])
G_REASON_BAD = Gauge("of_gate_contract_smoke_reason_code_bad_total", "Top reason_code among invalid rows", ["reason_code"])


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _parse_top_pairs(s: str) -> List[Tuple[str, int]]:
    try:
        obj = json.loads(s)
        if not isinstance(obj, list):
            return []
        out: List[Tuple[str, int]] = []
        for it in obj:
            if not (isinstance(it, (list, tuple)) and len(it) == 2):
                continue
            k = str(it[0])[:64]
            v = _safe_int(it[1], 0)
            out.append((k, v))
        return out
    except Exception:
        return []


def _set_labeled(g: Gauge, pairs: List[Tuple[str, int]], label: str) -> None:
    for k, v in pairs:
        try:
            g.labels(**{label: k}).set(float(v))
        except Exception:
            continue


def run() -> int:
    if redis is None:
        logger.error("Missing redis dependency")
        return 1

    r = redis.from_url(REDIS_URL, decode_responses=True)

    start_http_server(PORT)
    logger.info(f"P76 OF-gate contract smoke exporter on :{PORT}, stream={IN_STREAM}")

    while True:
        try:
            rows = r.xrevrange(IN_STREAM, max="+", min="-", count=1)
            if not rows:
                G_LAST_TS_MS.set(0)
                time.sleep(INTERVAL_SEC)
                continue

            _id, fields = rows[0]
            if not isinstance(fields, dict):
                time.sleep(INTERVAL_SEC)
                continue

            G_LAST_TS_MS.set(float(_safe_int(fields.get("ts_ms"), 0)))
            G_N_TOTAL.set(float(_safe_int(fields.get("n_total"), 0)))
            G_BAD_TOTAL.set(float(_safe_int(fields.get("bad_total"), 0)))
            G_BAD_SHARE.set(_safe_float(fields.get("bad_share"), 0.0))
            G_SCHEMA_V_MODE.set(float(_safe_int(fields.get("schema_version_mode"), 0)))
            G_SCHEMA_MISSING_SHARE.set(_safe_float(fields.get("schema_missing_share"), 0.0))

            _set_labeled(G_DQ, _parse_top_pairs(str(fields.get("top_dq_json") or "[]")), "dq_code")
            _set_labeled(G_REASON_ALL, _parse_top_pairs(str(fields.get("top_reason_code_json") or "[]")), "reason_code")
            _set_labeled(G_REASON_BAD, _parse_top_pairs(str(fields.get("top_reason_code_bad_json") or "[]")), "reason_code")

        except Exception as e:
            logger.warning(f"collect failed: {e}")

        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sys.exit(run())
