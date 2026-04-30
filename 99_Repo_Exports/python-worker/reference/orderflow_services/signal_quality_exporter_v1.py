"""P47 — Signal Quality KPIs Exporter (v1)

Exposes metrics from `metrics:signal_quality:24h` as Prometheus Gauge metrics.
Runs an HTTP server on port 9135.
"""

import json
import logging
import os
import time
import sys
from typing import Dict

from prometheus_client import start_http_server, Gauge

try:
    import redis
except ImportError:
    redis = None

logging.basicConfig(
    level=logging.INFO
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("signal_quality_exporter")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
IN_HASH = os.getenv("SIGNAL_QUALITY_OUT_HASH", "metrics:signal_quality:24h")
PORT = int(os.getenv("SIGNAL_QUALITY_EXPORTER_PORT", "9135"))

# --- Metrics ---
# Labels: group_type, group_name
# But our keys are like "global", "symbol:BTCUSDT", "strategy:Foo".
# We should parse these into labels.

def parse_group_key(key: str) -> Dict[str, str]:
    if ":" not in key:
        return {"type": "global", "name": "all"}
    parts = key.split(":", 1)
    return {"type": parts[0], "name": parts[1]}

# Gauges
G_EXPECTANCY = Gauge("signal_quality_expectancy_r_24h", "Expectancy (Mean R)", ["type", "name"])
G_PRECISION = Gauge("signal_quality_precision_top5p_24h", "Precision at Top 5%", ["type", "name"])
G_ECE = Gauge("signal_quality_ece_24h", "Expected Calibration Error", ["type", "name"])
G_N = Gauge("signal_quality_n_24h", "Number of trades in calculation", ["type", "name"])
G_LAST_TS = Gauge("signal_quality_last_ts_ms", "Timestamp of last calculation", ["type", "name"])


class SignalQualityCollector:
    def __init__(self, redis_client):
        self.r = redis_client

    def collect(self):
        # This approach uses Custom Collector, which is better than `while True` loop pushing to gauges.
        # But `Gauge` in python client is usually stateful.
        # To make it stateless (fetch on scrape), we can either use CustomCollector yielding Metrics
        # OR just update the global Gauges inside a loop/callback. 
        # Standard pattern: Update gauges periodically or on scrape.
        # Let's use the simplest pattern: Update gauges right before scrape? 
        # Or just run a loop that updates them every X seconds. 
        # For an exporter, usually "collect()" is called on scrape.
        
        # We will just fetch from Redis and set the gauges.
        try:
            data = self.r.hgetall(IN_HASH)
            if not data:
                return

            # Clear old metrics? 
            # Prometheus client doesn't easily "clear" labels not present anymore unless we recreate gauges or tracking.
            # We can use `set_to_current_time()` style or just overwrite.
            # Limitation: if a group disappears, it might linger. 
            # For now, we assume groups are relatively stable or we tolerate some ghost metrics until restart.
            
            for k_bytes, v_bytes in data.items():
                key = k_bytes.decode("utf-8")
                val_json = v_bytes.decode("utf-8")
                
                try:
                    metrics = json.loads(val_json)
                except (ValueError, json.JSONDecodeError):
                    continue
                
                labels = parse_group_key(key)
                l_type = labels["type"]
                l_name = labels["name"]
                
                if metrics.get("expectancy_r") is not None:
                    G_EXPECTANCY.labels(l_type, l_name).set(metrics["expectancy_r"])
                if metrics.get("precision_top5p") is not None:
                    G_PRECISION.labels(l_type, l_name).set(metrics["precision_top5p"])
                if metrics.get("ece") is not None:
                    G_ECE.labels(l_type, l_name).set(metrics["ece"])
                if metrics.get("n") is not None:
                    G_N.labels(l_type, l_name).set(metrics["n"])
                if metrics.get("ts_calc") is not None:
                    G_LAST_TS.labels(l_type, l_name).set(metrics["ts_calc"])
                    
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            
            
def run():
    if not redis:
        logger.error("Missing redis dependency")
        sys.exit(1)
        
    r = redis.from_url(REDIS_URL)
    
    # We will just start the server and update metrics in a loop or on request?
    # Simpler: Main loop updates metrics every 15s. HTTP server serves current state.
    
    start_http_server(PORT)
    logger.info(f"Signal Quality Exporter running on port {PORT}")
    
    collector = SignalQualityCollector(r)
    
    while True:
        collector.collect()
        time.sleep(15)

if __name__ == "__main__":
    run()
