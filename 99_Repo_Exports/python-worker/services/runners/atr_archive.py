import time
import logging
from services.atr_archive_and_replay_service import ATRArchiveAndReplayService

logger = logging.getLogger("atr_archive_runner")

def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("ATR Archive & Replay Service Daemon starting in SHADOW MODE (ENFORCE=0).")
    srv = ATRArchiveAndReplayService()
    
    while True:
        try:
            # Example operation: purge expired hot data for 'signal'
            # In a real environment, this might be scheduled via schedule/cron
            srv.purge_expired_hot_data("signal", incident_linked=False, archive_ready=True)
            logger.info("Performed routine purge check for 'signal'. Sleeping...")
        except Exception as e:
            logger.error(f"Error in ATR Archive runner: {e}")
        time.sleep(3600)  # Run hourly

if __name__ == "__main__":
    main()
