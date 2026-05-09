"""
P60 Edge Stack Shadow Exporter.

Goals:
1. Expose metrics from Redis `metrics:edge_stack_shadow:last` to Prometheus.
2. Port: 9814 (default).

Metrics:
- edge_stack_shadow_last_success (0/1)
- edge_stack_shadow_last_updated_ts_ms
- edge_stack_shadow_last_n
- edge_stack_shadow_champion_brier
- edge_stack_shadow_champion_ece
- edge_stack_shadow_champion_precision_top5pct
- edge_stack_shadow_champion_expectancy_r_top5pct
- edge_stack_shadow_candidate_... (if avail)
- edge_stack_shadow_promoted (0/1)
"""

import logging
import os
import sys
import time

import redis
from prometheus_client import Gauge, start_http_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Constants
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
METRICS_KEY = "metrics:edge_stack_shadow:last"
PORT = int(os.environ.get("EDGE_STACK_SHADOW_EXPORTER_PORT", 9814))
POLL_INTERVAL = 15

# Gauges
g_success = Gauge("edge_stack_shadow_last_success", "1 if last eval was status=ok")
g_updated = Gauge("edge_stack_shadow_last_updated_ts_ms", "Timestamp of last update")
g_n = Gauge("edge_stack_shadow_last_n", "Number of samples in last eval")
g_promoted = Gauge("edge_stack_shadow_promoted", "1 if last run triggered promotion")

# Dynamic metric creation helper
gauges = {}

def get_gauge(name: str, doc: str = ""):
    if name not in gauges:
        gauges[name] = Gauge(name, doc or name)
    return gauges[name]

def main():
    logger.info(f"Starting Edge Stack Shadow Exporter on port {PORT}")
    start_http_server(PORT)

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    while True:
        try:
            data = r.hgetall(METRICS_KEY)
            if not data:
                time.sleep(POLL_INTERVAL)
                continue

            # Status
            status = data.get("status", "unknown")
            g_success.set(1 if status == "ok" else 0)

            # TS
            ts = float(data.get("updated_ts_ms", 0))
            g_updated.set(ts)

            # N
            g_n.set(float(data.get("n_samples", 0)))

            # Promoted
            g_promoted.set(float(data.get("promoted", 0)))

            # Dynamic metrics for champion/candidate
            # We look for keys starting with champion_ or candidate_ and are numeric
            for k, v in data.items():
                if k.startswith("champion_") or k.startswith("candidate_"):
                    # skip strings like check_error, path, status if they are not numbers
                    try:
                        val = float(v)
                        metric_name = f"edge_stack_shadow_{k}"
                        get_gauge(metric_name).set(val)
                    except ValueError:
                        pass

            logger.debug("Metrics updated")

        except Exception as e:
            logger.error(f"Error polling Redis: {e}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
