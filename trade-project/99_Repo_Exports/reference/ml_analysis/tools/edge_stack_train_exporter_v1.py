#!/usr/bin/env python3
"""Prometheus exporter for edge_stack_v1 training bundle status (P59).

Reads Redis hash `metrics:edge_stack_train:last` (or configured key) and exposes gauges:
  - edge_stack_train_last_success        (1 if status == "ok")
  - edge_stack_train_last_updated_ts_ms  (ms timestamp of last run)
  - edge_stack_train_last_joined         (joined count from dataset)
  - edge_stack_train_last_pos_rate       (pos_rate from dataset)
  - edge_stack_train_last_oof_meta_brier (OOF brier score)
  - edge_stack_train_last_oof_meta_ece   (OOF ECE score)
  - edge_stack_train_last_promote_applied (1 if promotion applied)
  - edge_stack_train_last_train_ok       (1 if train validation passed)
  - edge_stack_train_last_age_seconds    (age of record in seconds)
  - edge_stack_train_exporter_up         (1 if Redis is reachable)

This exporter is intentionally low-cardinality and resilient:
  - if Redis is unavailable, `edge_stack_train_exporter_up` = 0 (no crash)
  - stale records (age > EDGE_STACK_TRAIN_EXPORTER_STALE_S) force success=0

ENV:
  REDIS_URL                           (default: redis://redis-worker-1:6379/0)
  EDGE_STACK_TRAIN_METRICS_KEY        (default: metrics:edge_stack_train:last)
  EDGE_STACK_TRAIN_EXPORTER_PORT      (default: 9813)
  EDGE_STACK_TRAIN_EXPORTER_INTERVAL_S (default: 5)
  EDGE_STACK_TRAIN_EXPORTER_STALE_S   (default: 129600 = 36h)
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
    """Safely convert Redis string to float."""
    try:
        if v is None:
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _as_int(v: Any, d: int = 0) -> int:
    """Safely convert Redis string to int via float."""
    try:
        if v is None:
            return int(d)
        return int(float(v))
    except Exception:
        return int(d)


# Prometheus gauges (low-cardinality, no dynamic labels)
UP = Gauge("edge_stack_train_exporter_up", "1 if exporter can read Redis metrics")
LAST_SUCCESS = Gauge("edge_stack_train_last_success", "1 if last bundle status is ok")
LAST_UPDATED_MS = Gauge("edge_stack_train_last_updated_ts_ms", "updated_ts_ms from Redis hash")
LAST_JOINED = Gauge("edge_stack_train_last_joined", "joined count from last bundle")
LAST_POS_RATE = Gauge("edge_stack_train_last_pos_rate", "pos_rate from last bundle")
LAST_BRIER = Gauge("edge_stack_train_last_oof_meta_brier", "OOF meta brier from last bundle")
LAST_ECE = Gauge("edge_stack_train_last_oof_meta_ece", "OOF meta ECE from last bundle")
LAST_PROMOTE = Gauge("edge_stack_train_last_promote_applied", "1 if promotion applied in last bundle")
LAST_TRAIN_OK = Gauge("edge_stack_train_last_train_ok", "1 if train validation passed in last bundle")
AGE_S = Gauge("edge_stack_train_last_age_seconds", "Age of metrics record in seconds")


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    metrics_key = os.getenv("EDGE_STACK_TRAIN_METRICS_KEY", "metrics:edge_stack_train:last")
    port = int(os.getenv("EDGE_STACK_TRAIN_EXPORTER_PORT", "9813"))
    interval_s = float(os.getenv("EDGE_STACK_TRAIN_EXPORTER_INTERVAL_S", "5"))
    # Records older than stale_s are treated as failed (model not trained recently)
    stale_s = float(os.getenv("EDGE_STACK_TRAIN_EXPORTER_STALE_S", "129600"))  # 36h default

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)

    while True:
        try:
            m: Dict[str, Any] = r.hgetall(metrics_key) or {}
            UP.set(1)

            # Derive success from status field
            status = str(m.get("status", "") or "")
            success = 1.0 if status == "ok" else 0.0
            LAST_SUCCESS.set(success)

            updated_ms = _as_int(m.get("updated_ts_ms", 0), 0)
            LAST_UPDATED_MS.set(float(updated_ms))
            LAST_JOINED.set(float(_as_int(m.get("joined", 0), 0)))
            LAST_POS_RATE.set(_as_float(m.get("pos_rate", 0.0), 0.0))
            LAST_BRIER.set(_as_float(m.get("oof_meta_brier", 0.0), 0.0))
            LAST_ECE.set(_as_float(m.get("oof_meta_ece", 0.0), 0.0))
            LAST_PROMOTE.set(float(_as_int(m.get("promote_applied", 0), 0)))
            LAST_TRAIN_OK.set(float(_as_int(m.get("train_ok", 0), 0)))

            age = max(0.0, _now_s() - (float(updated_ms) / 1000.0 if updated_ms > 0 else 0.0))
            AGE_S.set(age)

            # Stale → mark success=0 to trigger Prometheus alert
            if age > stale_s:
                LAST_SUCCESS.set(0.0)
        except Exception:
            UP.set(0)
            LAST_SUCCESS.set(0)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
