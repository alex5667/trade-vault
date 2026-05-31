# tools/verify_p41_contract.py
import os
import json
import redis
from common.log import setup_logger

logger = setup_logger("ContractVerifierP41")

def main():
    # Use localhost:63791 (redis-worker-1) by default if env not set
    # Or try to get it from environment
    redis_url = os.getenv("REDIS_URL", "redis://localhost:63791/0")
    stream_name = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        logger.info(f"Connecting to Redis: {redis_url}")
        
        # Get latest entry
        entries = r.xrevrange(stream_name, count=1)
        if not entries:
            logger.warning(f"Stream {stream_name} is empty.")
            return

        entry_id, payload = entries[0]
        logger.info(f"Checking entry {entry_id} in {stream_name}...")

        # Contract P41 Fields
        p41_fields = ["meta_enforce_cov_bucket", "meta_enforce_applied"]
        
        missing = []
        for f in p41_fields:
            if f not in payload:
                # Check aliases
                alias = f.replace("meta_enforce_", "meta_")
                if alias in payload:
                    logger.info(f"Found alias '{alias}' for '{f}'")
                else:
                    missing.append(f)
        
        if missing:
            logger.error(f"FAIL: Missing P41 fields: {missing}")
            # Dump payload for debug
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        else:
            logger.info("PASS: All P41 fields (or aliases) are present.")
            # Verify types/values if possible
            bucket = payload.get("meta_enforce_cov_bucket") or payload.get("meta_cov_bucket")
            applied = payload.get("meta_enforce_applied") or payload.get("meta_applied")
            
            logger.info(f"meta_enforce_cov_bucket: {bucket}")
            logger.info(f"meta_enforce_applied: {applied}")

    except Exception as e:
        logger.error(f"Error during verification: {e}")

if __name__ == "__main__":
    main()
