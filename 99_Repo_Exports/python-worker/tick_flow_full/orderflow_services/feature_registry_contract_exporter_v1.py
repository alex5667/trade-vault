#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prometheus exporter for Feature Registry contract status (P94).

Reads Redis hash `metrics:feature_registry_contract:last` and exposes gauges:
  - feature_registry_contract_exporter_up
  - feature_registry_contract_last_success
  - feature_registry_contract_last_updated_ts_ms
  - feature_registry_contract_last_age_seconds
  - feature_registry_contract_last_pins_present
  - feature_registry_contract_last_schema_ver_mismatch
  - feature_registry_contract_last_schema_hash_mismatch
  - feature_registry_contract_last_feature_cols_hash_mismatch

Resilience:
  - if Redis is unavailable -> exporter_up=0 (no crash)
  - if record is stale -> last_success forced to 0

ENV:
  REDIS_URL                                 (default: redis://redis-worker-1:6379/0)
  FEATURE_REGISTRY_CONTRACT_METRICS_KEY     (default: metrics:feature_registry_contract:last)
  FEATURE_REGISTRY_CONTRACT_EXPORTER_PORT   (default: 9817)
  FEATURE_REGISTRY_CONTRACT_EXPORTER_INTERVAL_S (default: 10)
  FEATURE_REGISTRY_CONTRACT_EXPORTER_STALE_S    (default: 21600 = 6h)
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict

import redis  # type: ignore
from prometheus_client import Gauge, start_http_server


def _now_s() -> float:
    return time.time()


def _as_float(v: Any, d: float = 0.0) -> float:
    try:
        if v is None:
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _as_int(v: Any, d: int = 0) -> int:
    try:
        if v is None:
            return int(d)
        return int(float(v))
    except Exception:
        return int(d)


UP = Gauge("feature_registry_contract_exporter_up", "1 if exporter can read Redis metrics")
LAST_SUCCESS = Gauge("feature_registry_contract_last_success", "1 if last contract check is ok")
LAST_UPDATED_MS = Gauge("feature_registry_contract_last_updated_ts_ms", "updated_ts_ms from Redis hash")
AGE_S = Gauge("feature_registry_contract_last_age_seconds", "Age of metrics record in seconds")
PINS_PRESENT = Gauge("feature_registry_contract_last_pins_present", "1 if pins are present in cfg hash")
MM_VER = Gauge("feature_registry_contract_last_schema_ver_mismatch", "1 if schema_ver mismatches pinned")
MM_SCHEMA = Gauge("feature_registry_contract_last_schema_hash_mismatch", "1 if schema_hash mismatches pinned")
MM_COLS = Gauge("feature_registry_contract_last_feature_cols_hash_mismatch", "1 if feature_cols_hash mismatches pinned")


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    metrics_key = os.getenv("FEATURE_REGISTRY_CONTRACT_METRICS_KEY", "metrics:feature_registry_contract:last")
    port = int(os.getenv("FEATURE_REGISTRY_CONTRACT_EXPORTER_PORT", "9817"))
    interval_s = float(os.getenv("FEATURE_REGISTRY_CONTRACT_EXPORTER_INTERVAL_S", "10"))
    stale_s = float(os.getenv("FEATURE_REGISTRY_CONTRACT_EXPORTER_STALE_S", "21600"))  # 6h

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)

    while True:
        try:
            m: Dict[str, Any] = r.hgetall(metrics_key) or {}
            UP.set(1)

            status = str(m.get("status", "") or "")
            success = 1.0 if status == "ok" else 0.0
            LAST_SUCCESS.set(success)

            updated_ms = _as_int(m.get("updated_ts_ms", 0), 0)
            LAST_UPDATED_MS.set(float(updated_ms))
            PINS_PRESENT.set(float(_as_int(m.get("pins_present", 0), 0)))
            MM_VER.set(float(_as_int(m.get("mismatch_schema_ver", 0), 0)))
            MM_SCHEMA.set(float(_as_int(m.get("mismatch_schema_hash", 0), 0)))
            MM_COLS.set(float(_as_int(m.get("mismatch_feature_cols_hash", 0), 0)))

            age = max(0.0, _now_s() - (float(updated_ms) / 1000.0 if updated_ms > 0 else 0.0))
            AGE_S.set(age)

            if age > stale_s:
                LAST_SUCCESS.set(0.0)
        except Exception:
            UP.set(0)
            LAST_SUCCESS.set(0)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
