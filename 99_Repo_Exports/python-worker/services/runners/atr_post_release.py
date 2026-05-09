import logging
import os
import time

from prometheus_client import start_http_server

from services.atr_post_release_observation_service import process_pending_observations

logger = logging.getLogger("atr_post_release_runner")

def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("ATR Post Release Observation Daemon starting in SHADOW MODE (ENFORCE=0).")

    port = int(os.getenv("ATR_POST_RELEASE_METRICS_PORT", "9849"))
    start_http_server(port)
    logger.info(f"Prometheus metrics exposed on port {port}")

    while True:
        try:
            process_pending_observations()
            logger.info("Processed pending post-release observations.")
        except Exception as e:
            logger.error(f"Error in ATR Post Release Observation runner: {e}")
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
