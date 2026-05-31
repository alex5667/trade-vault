"""
conf_cal_live_status_exporter_v1.py

Exposes metrics from live_status.json (produced by health loop) to Prometheus.
Metrics:
- conf_cal_rollback_total
- conf_cal_live_ece_raw / _cal
- conf_cal_live_brier_raw / _cal
- conf_cal_live_degrade (0/1)
- conf_cal_live_exact_rate
+ status_age
"""

import json
import logging
import os
import time

from prometheus_client import Gauge, start_http_server

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("conf_cal_exporter")

STATUS_PATH = os.getenv("CONF_CAL_LIVE_STATUS_PATH", "/tmp/conf_cal_live_status.json")
EXPORTER_PORT = int(os.getenv("CONF_CAL_LIVE_EXPORTER_PORT", "9134"))

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
# Labels: symbol (usually GLOBAL for this loop, unless we split by symbol)
LABELS = ["symbol"]

ROLLBACK_TOTAL = Gauge("conf_cal_rollback_total", "Total auto-rollbacks triggered", LABELS)
LIVE_ECE_RAW = Gauge("conf_cal_live_ece_raw", "Live ECE (Raw Confidence)", LABELS)
LIVE_ECE_CAL = Gauge("conf_cal_live_ece_cal", "Live ECE (Calibrated Confidence)", LABELS)
LIVE_BRIER_RAW = Gauge("conf_cal_live_brier_raw", "Live Brier Score (Raw)", LABELS)
LIVE_BRIER_CAL = Gauge("conf_cal_live_brier_cal", "Live Brier Score (Calibrated)", LABELS)
LIVE_DEGRADE = Gauge("conf_cal_live_degrade", "Degradation status (1=Bad, 0=OK)", LABELS)
LIVE_EXACT_RATE = Gauge("conf_cal_live_bucket_exact_rate", "Rate of exact bucket hits", LABELS)

STATUS_AGE = Gauge("conf_cal_status_age_seconds", "Age of the status file")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info(f"Starting Exporter on port {EXPORTER_PORT}. Watching {STATUS_PATH}")
    start_http_server(EXPORTER_PORT)

    symbol_label = "GLOBAL" # The health loop aggregates globally (or could be per-symbol input)

    while True:
        try:
            if os.path.exists(STATUS_PATH):
                # Check age
                mtime = os.path.getmtime(STATUS_PATH)
                age = time.time() - mtime
                STATUS_AGE.set(age)

                # Read
                with open(STATUS_PATH) as f:
                    data = json.load(f)

                # Update Metrics
                ROLLBACK_TOTAL.labels(symbol=symbol_label).set(float(data.get("rollback_total", 0)))

                LIVE_ECE_RAW.labels(symbol=symbol_label).set(float(data.get("live_ece_raw", 0)))
                LIVE_ECE_CAL.labels(symbol=symbol_label).set(float(data.get("live_ece_cal", 0)))

                LIVE_BRIER_RAW.labels(symbol=symbol_label).set(float(data.get("live_brier_raw", 0)))
                LIVE_BRIER_CAL.labels(symbol=symbol_label).set(float(data.get("live_brier_cal", 0)))

                is_bad = 1 if data.get("status") == "degraded" else 0
                LIVE_DEGRADE.labels(symbol=symbol_label).set(is_bad)

                LIVE_EXACT_RATE.labels(symbol=symbol_label).set(float(data.get("exact_rate", 0)))

            else:
                STATUS_AGE.set(9999)

        except Exception as e:
            logger.error(f"Export error: {e}")

        time.sleep(5)

if __name__ == "__main__":
    main()
