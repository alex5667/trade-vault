from __future__ import annotations

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""conf_score_weight_tuning_exporter_v1.py

Prometheus exporter for Phase2 confidence scorer tuning job status.

Reads low-cardinality keys from Redis hash (default settings:dynamic_cfg)
written by nightly_conf_score_weight_tuning_bundle_v1.py.

Exports:
  conf_score_tuning_last_ok (0/1)
  conf_score_tuning_last_age_seconds
  conf_score_tuning_last_exit_code
  conf_score_tuning_last_joined_rows
  conf_score_tuning_last_pos_rate
  conf_score_tuning_last_published (0/1)

Env:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)
  EXPORTER_PORT (default 9117)
  EXPORTER_ADDR (default 0.0.0.0)
""",
import logging
import os
import time
from typing import Any

import redis  # type: ignore
from prometheus_client import Gauge, start_http_server  # type: ignore

from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("conf_score_tuning_exporter")


def _to_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _to_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


g_ok = Gauge("conf_score_tuning_last_ok", "1 if last tuning job run succeeded")
g_age = Gauge("conf_score_tuning_last_age_seconds", "Age (seconds) since last tuning run")
g_exit = Gauge("conf_score_tuning_last_exit_code", "Exit code of last tuning run")
g_rows = Gauge("conf_score_tuning_last_joined_rows", "Joined rows used for tuning")
g_pos_rate = Gauge("conf_score_tuning_last_pos_rate", "Positive label rate in last dataset")
g_pub = Gauge("conf_score_tuning_last_published", "1 if last run published tuning to Redis")


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    dyn_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

    addr = os.getenv("EXPORTER_ADDR", "0.0.0.0")
    port = int(os.getenv("EXPORTER_PORT", "9117"))

    r = redis.Redis.from_url(redis_url, decode_responses=True)

    start_http_server(port, addr=addr)
    logger.info("conf_score_weight_tuning_exporter listening on %s:%s", addr, port)

    while True:
        try:
            d: dict[str, Any] = r.hgetall(dyn_key) or {}

            last_ts_ms = _to_int(d.get("conf_score_tuning_last_ts_ms"), 0)
            ok = _to_int(d.get("conf_score_tuning_last_ok"), 0)
            exit_code = _to_int(d.get("conf_score_tuning_last_exit_code"), 0)
            joined_rows = _to_int(d.get("conf_score_tuning_last_joined_rows"), 0)
            pos_rate = _to_float(d.get("conf_score_tuning_last_pos_rate"), 0.0)
            published = _to_int(d.get("conf_score_tuning_last_published"), 0)

            g_ok.set(float(1 if ok else 0))
            g_exit.set(float(exit_code))
            g_rows.set(float(joined_rows))
            g_pos_rate.set(float(pos_rate))
            g_pub.set(float(1 if published else 0))

            now_ms = get_ny_time_millis()
            age_s = max(0.0, float(now_ms - last_ts_ms) / 1000.0) if last_ts_ms > 0 else 1e9
            g_age.set(age_s)

        except Exception as exc:  # noqa: BLE001
            logger.warning("export loop error: %s", exc)

        time.sleep(float(os.getenv("EXPORTER_POLL_SEC", "5")))


if __name__ == "__main__":
    main()
