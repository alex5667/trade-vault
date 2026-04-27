import time
import logging
from services.atr_post_release_observation_service import process_pending_observations

logger = logging.getLogger("atr_post_release_runner")

def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("ATR Post Release Observation Daemon starting in SHADOW MODE (ENFORCE=0).")
    
    while True:
        try:
            process_pending_observations()
            logger.info("Processed pending post-release observations.")
        except Exception as e:
            logger.error(f"Error in ATR Post Release Observation runner: {e}")
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
