import hashlib
import logging
import os
import time
from typing import Any

import feedparser
import redis
import contextlib

log = logging.getLogger("news_ingestor_py")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NEWS_RAW_STREAM = os.getenv("NEWS_RAW_STREAM", "news:raw")
NEWS_RAW_DLQ = os.getenv("NEWS_RAW_DLQ", "news:raw:dlq")

LEADER_KEY = os.getenv("NEWS_INGESTOR_LEADER_KEY", "news:ingestor:leader")
LEADER_TTL_SEC = int(os.getenv("NEWS_INGESTOR_LEADER_TTL_SEC", "8"))

RSS_URLS = [u.strip() for u in os.getenv("NEWS_RSS_URLS", "").split(",") if u.strip()]
POLL_SEC = float(os.getenv("NEWS_POLL_SEC", "10"))
DEDUPE_TTL_SEC = int(os.getenv("NEWS_DEDUPE_TTL_SEC", str(30 * 60)))
STREAM_MAXLEN = int(os.getenv("NEWS_STREAM_MAXLEN", "100000"))


def stable_uid(source: str, url: str, title: str, ts_bucket: int) -> str:
    h = hashlib.sha1(f"{source}|{url}|{title}|{ts_bucket}".encode()).hexdigest()
    return h


def ts_bucket_sec(epoch_sec: int, bucket_sec: int = 300) -> int:
    return (epoch_sec // bucket_sec) * bucket_sec


def try_acquire_lock(r: redis.Redis, value: str) -> bool:
    return bool(r.set(LEADER_KEY, value, nx=True, ex=LEADER_TTL_SEC))


def renew_lock(r: redis.Redis, value: str) -> bool:
    # renew only if value matches
    script = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("EXPIRE", KEYS[1], ARGV[2])
    else
      return 0
    end
    """
    return bool(r.eval(script, 1, LEADER_KEY, value, LEADER_TTL_SEC))


def mark_dedupe(r: redis.Redis, uid: str) -> bool:
    return bool(r.set(f"news:dedupe:{uid}", "1", nx=True, ex=DEDUPE_TTL_SEC))


def xadd(r: redis.Redis, stream: str, fields: dict[str, Any]) -> None:
    r.xadd(stream, fields, maxlen=STREAM_MAXLEN, approximate=True)


def _wait_for_redis_ready(redis_url: str) -> redis.Redis:
    """Wait for Redis to be ready, handling BusyLoadingError"""
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
                redis_url,
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


def main() -> None:
    r = _wait_for_redis_ready(REDIS_URL)

    value = f"py:{int(time.time()*1e9)}"

    while True:
        try:
            if not try_acquire_lock(r, value):
                time.sleep(2.0)
                continue
        except (redis.exceptions.BusyLoadingError, redis.ConnectionError) as e:
            log.warning("Redis unavailable (loading/connection): %s. Waiting...", e)
            time.sleep(5.0)
            continue
        except Exception as e:
            log.error("Unexpected error acquiring lock: %s", e)
            time.sleep(5.0)
            continue

        log.info("I am leader (python fallback ingestor). RSS=%d", len(RSS_URLS))

        while True:
            renew_lock(r, value)

            for rss_url in RSS_URLS:
                try:
                    d = feedparser.parse(rss_url)
                except Exception as e:
                    log.warning("rss parse error url=%s err=%s", rss_url, e)
                    continue

                for it in d.entries or []:
                    title = (it.get("title") or "").strip()
                    link = (it.get("link") or "").strip()
                    if not title or not link:
                        continue

                    published = int(it.get("published_parsed") and time.mktime(it.published_parsed) or time.time())
                    bucket = ts_bucket_sec(published, 300)
                    uid = stable_uid(f"rss:{rss_url}", link, title, bucket)

                    try:
                        if not mark_dedupe(r, uid):
                            continue

                        xadd(r, NEWS_RAW_STREAM, {
                            "uid": uid,
                            "source": f"rss:{rss_url}",
                            "title": title,
                            "url": link,
                            "ts_ms": published * 1000,
                            "symbol": "",
                            "asset_class": "",
                        })
                    except Exception as e:
                        with contextlib.suppress(Exception):
                            xadd(r, NEWS_RAW_DLQ, {"uid": uid, "err": str(e), "source": f"rss:{rss_url}", "url": link})

            time.sleep(max(POLL_SEC, 0.2))

            # lock lost -> back to standby
            try:
                if r.get(LEADER_KEY) != value:
                    log.warning("leader lost -> standby")
                    break
            except Exception:
                break


if __name__ == "__main__":
    main()
