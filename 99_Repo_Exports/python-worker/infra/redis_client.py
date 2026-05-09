"""
Синхронный Redis-клиент с автоподключением.
"""

from functools import lru_cache

import redis


def _wait_for_redis_ready(url: str) -> redis.Redis:
    """Wait for Redis to be ready, handling BusyLoadingError"""
    import logging
    import time

    import redis

    max_retries = 60  # 10 минут при 10сек задержке
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Отключаем CLIENT SETINFO для совместимости со старыми версиями Redis
            import redis.connection
            redis.connection.Connection.lib_name = None
            redis.connection.Connection.lib_version = None

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


@lru_cache(maxsize=1)
def get_redis(url: str) -> redis.Redis:
    return _wait_for_redis_ready(url)


def try_get_json(r: redis.Redis, key: str) -> dict | None:
    import json
    v = r.get(key)
    if not v:
        return None
    try:
        return json.loads(v)
    except Exception:
        return None


