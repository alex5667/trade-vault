
import os
import sys
import time
import json
import logging
import redis
from prometheus_client import start_http_server, Gauge

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

class DecisionCoverageExporter:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        self.metrics_key = os.getenv("DECISION_COVERAGE_OUT_HASH", "metrics:decision_coverage:24h")
        self.port = int(os.getenv("DECISION_COVERAGE_EXPORTER_PORT", "9815"))
        
        # Define Metrics
        self.g_allow_rate = Gauge("decision_allow_rate_24h", "Fraction of allowed decisions in last 24h")
        self.g_veto_rate = Gauge("decision_veto_rate_24h", "Fraction of vetoed decisions in last 24h")
        self.g_n = Gauge("decision_n_24h", "Total count of decisions in last 24h")
        self.g_last_ts = Gauge("decision_last_ts_ms", "Timestamp of last processed decision")

        # P69: Policy Mode breakdowns
        self.g_policy_mode_share = Gauge("decision_policy_mode_share_24h", "Share of decisions by policy mode", ["mode"])
        self.g_policy_mode_n = Gauge("decision_policy_mode_n_24h", "Count of decisions by policy mode", ["mode"])

    def update_metrics(self):
        try:
            data = self.r.hgetall(self.metrics_key)
            if not data:
                return

            if "decision_allow_rate_24h" in data:
                self.g_allow_rate.set(float(data["decision_allow_rate_24h"]))
            
            if "decision_veto_rate_24h" in data:
                self.g_veto_rate.set(float(data["decision_veto_rate_24h"]))
                
            if "decision_n_24h" in data:
                self.g_n.set(float(data["decision_n_24h"]))
                
            if "decision_last_ts_ms" in data:
                self.g_last_ts.set(float(data["decision_last_ts_ms"]))
                
            # P69: Export policy mode metrics
            # We iterate known modes to avoid dynamic label issues if keys missing
            for mode in ["ok", "warn", "block", "unknown"]:
                # Share
                k_share = f"decision_policy_mode_share_24h_{mode}"
                if k_share in data:
                    self.g_policy_mode_share.labels(mode=mode).set(float(data[k_share]))
                
                # Count
                k_n = f"decision_policy_mode_n_24h_{mode}"
                if k_n in data:
                    self.g_policy_mode_n.labels(mode=mode).set(float(data[k_n]))
                
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")

    def run(self):
        logger.info(f"Starting Decision Coverage Exporter on port {self.port}...")
        start_http_server(self.port)
        
        while True:
            self.update_metrics()
            time.sleep(15)

if __name__ == "__main__":
    exporter = DecisionCoverageExporter()
    exporter.run()
