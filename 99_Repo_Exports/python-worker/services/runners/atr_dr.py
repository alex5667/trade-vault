import time
import logging
from services.atr_disaster_recovery_service import ATRDisasterRecoveryService

logger = logging.getLogger("atr_dr_runner")

def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("ATR Disaster Recovery Daemon starting in SHADOW MODE (ENFORCE=0).")
    
    while True:
        try:
            # The DR service usually operates on manual trigger or Telegram integration
            # This loop just maintains the container and background health checks
            active_blocker = ATRDisasterRecoveryService.is_release_blocked_by_dr("global")
            if active_blocker:
                logger.warning(f"Active DR event detected: {active_blocker}")
        except Exception as e:
            logger.error(f"Error in ATR DR runner: {e}")
        time.sleep(60)

if __name__ == "__main__":
    main()
