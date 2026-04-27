"""P59: Prometheus exporter for edge_stack_v1 training status.

Reads Redis hash: metrics:edge_stack_train:last

Exports:
  - edge_stack_train_last_success (0/1)
  - edge_stack_train_last_updated_ts_ms
  - edge_stack_train_last_joined
  - edge_stack_train_last_pos_rate
  - edge_stack_train_last_oof_meta_brier
  - edge_stack_train_last_oof_meta_ece
  - edge_stack_train_last_oof_meta_precision_top5pct
"""

from __future__ import annotations

import os
import time
from typing import Dict, Any

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


KEY = os.getenv("EDGE_STACK_TRAIN_METRICS_KEY", "metrics:edge_stack_train:last")

g_success = Gauge("edge_stack_train_last_success", "Last edge_stack train status (1=ok)")
g_updated = Gauge("edge_stack_train_last_updated_ts_ms", "Last edge_stack train metrics update time (ms)")
g_joined = Gauge("edge_stack_train_last_joined", "Joined records in last dataset")
g_pos_rate = Gauge("edge_stack_train_last_pos_rate", "Positive rate in last dataset")
g_brier = Gauge("edge_stack_train_last_oof_meta_brier", "OOF meta brier score")
g_ece = Gauge("edge_stack_train_last_oof_meta_ece", "OOF meta ECE")
g_prec = Gauge("edge_stack_train_last_oof_meta_precision_top5pct", "OOF meta precision@top5%")


def _redis_client(url: str, max_attempts: int = 3):
    if redis is None:
        raise RuntimeError("redis-py is required")
    delay = 1.0
    for attempt in range(max_attempts):
        try:
            r = redis.Redis.from_url(url, decode_responses=True)
            r.ping()
            return r
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            print(f"⚠️ Redis not ready (attempt {attempt + 1}/{max_attempts}): {e}. Retry in {delay:.0f}s...")
            time.sleep(delay)
            delay = min(delay * 2, 10.0)


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    port = int(os.getenv("EDGE_STACK_TRAIN_EXPORTER_PORT", "9813"))
    poll_s = int(os.getenv("EDGE_STACK_TRAIN_EXPORTER_POLL_S", "10"))

    start_http_server(port)
    r = _redis_client(redis_url)

    while True:
        try:
            m: Dict[str, Any] = r.hgetall(KEY) or {}
            status = str(m.get("status", ""))

            g_success.set(1.0 if status == "ok" else 0.0)
            g_updated.set(_safe_float(m.get("updated_ts_ms")))
            g_joined.set(_safe_float(m.get("joined")))
            g_pos_rate.set(_safe_float(m.get("pos_rate")))
            g_brier.set(_safe_float(m.get("oof_meta_brier")))
            g_ece.set(_safe_float(m.get("oof_meta_ece")))
            g_prec.set(_safe_float(m.get("oof_meta_precision_top5pct")))
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError) as e:
            print(f"⚠️ Redis connection lost: {e}. Reconnecting...")
            try:
                r = _redis_client(redis_url)
            except Exception:
                pass
        except Exception:
            # keep exporter alive for non-connection errors
            pass
        time.sleep(poll_s)


if __name__ == "__main__":  # pragma: no cover
    main()
