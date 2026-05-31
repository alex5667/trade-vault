import os
import redis
import json
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

def bootstrap_specs():
    r = None
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.ConnectionError:
        logger.warning(f"Failed to connect to {REDIS_HOST}:{REDIS_PORT}. Trying redis-worker-1...")
        try:
            r = redis.Redis(host="redis-worker-1", port=6379, db=0, decode_responses=True)
            r.ping()
            logger.info("Connected to redis-worker-1:6379")
        except redis.ConnectionError:
            logger.error("Could not connect to Redis. Please ensure you are in the correct network/env.")
            return

    symbols = ["BTCUSDT", "ETHUSDT"]
    
    # Lenient default specs to unblock trading
    lenient_specs = {
        "trailing_enabled": True,
        "delta_abs_min": 0.5,
        "delta_abs_min_confirm": 0.5,
        "min_confirmations": 1,
        "managed_enabled": True,
        "strategy": "crypto_orderflow"
    }

    # Config overrides (Hash) - strictly controls the filtering
    config_overrides = {
        "delta_abs_min": "0.5",
        "delta_abs_min_confirm": "0.5",
        "min_confirmations": "1"
    }

    for symbol in symbols:
        # 1. Set symbol_specs (JSON)
        key_specs = f"symbol_specs:{symbol}"
        logger.info(f"Setting lenient specs for {symbol}...")
        r.set(key_specs, json.dumps(lenient_specs))
        logger.info(f"Successfully set {key_specs}")
        
        # 2. Set config:orderflow:{symbol} (Hash)
        key_conf = f"config:orderflow:{symbol}"
        logger.info(f"Setting config overrides for {symbol}...")
        # Use hset mapping
        r.hset(key_conf, mapping=config_overrides)
        logger.info(f"Successfully set {key_conf} with {config_overrides}")

    logger.info("Bootstrap complete.")

if __name__ == "__main__":
    bootstrap_specs()
