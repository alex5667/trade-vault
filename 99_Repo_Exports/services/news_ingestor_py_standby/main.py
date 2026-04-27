#!/usr/bin/env python3
import os
import logging
import redis

# Setup logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)

# Import and run standby ingestor
from news_pipeline.standby_ingestor import run

def get_redis():
    """Get Redis client - reuse your existing pattern"""
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Отключаем CLIENT SETINFO для совместимости со старыми версиями Redis
    import redis.connection
    redis.connection.Connection.lib_name = None
    redis.connection.Connection.lib_version = None

    # Ждем готовности Redis
    import redis
    import time
    import logging

    max_retries = 60  # 10 минут при 10сек задержке
    retry_count = 0

    while retry_count < max_retries:
        try:
            r = redis.Redis.from_url(
                url,
                decode_responses=True,
                health_check_interval=30,
                socket_timeout=10,
            )
            # Test connection
            r.ping()
            logging.info("Redis connection established successfully")
            return r
        except redis.BusyLoadingError:
            retry_count += 1
            logging.warning(f"Redis is loading dataset, waiting... ({retry_count}/{max_retries})")
            time.sleep(10)
        except Exception as e:
            retry_count += 1
            logging.warning(f"Redis connection failed (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(10)

    raise Exception(f"Failed to connect to Redis after {max_retries} retries")

if __name__ == "__main__":
    r = get_redis()
    try:
        run(r)
    except KeyboardInterrupt:
        logging.info("Standby ingestor stopped by user")
    except Exception as e:
        logging.error("Standby ingestor crashed: %s", e)
        raise
