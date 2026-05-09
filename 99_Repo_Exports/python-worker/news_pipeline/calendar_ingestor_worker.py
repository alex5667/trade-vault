from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis

log = logging.getLogger("calendar_ingestor")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

HEARTBEAT_TTL_SEC = int(os.getenv("HEARTBEAT_TTL_SEC", "30"))
INSTANCE_ID = os.getenv("INSTANCE_ID", f"py-cal:{os.getpid()}")

from news_pipeline.leader_lock import LeaderLock
import contextlib

try:
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None  # type: ignore

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Standalone calendar ingestor (Python standby).
# It can run concurrently with Go news ingestor if you use a dedicated leader key.
LEADER_KEY = os.getenv("CALENDAR_INGESTOR_LEADER_KEY", "calendar:ingestor:leader")
LEADER_TTL_SEC = float(os.getenv("CALENDAR_INGESTOR_LEADER_TTL_SEC", "8"))

STREAM = os.getenv("CALENDAR_EVENTS_STREAM", "calendar:events")
DLQ = os.getenv("CALENDAR_INGESTOR_DLQ", "calendar:events:dlq")

POLL_SEC = float(os.getenv("FMP_CALENDAR_POLL_SEC", "60"))
DEDUP_TTL_SEC = int(os.getenv("CALENDAR_DEDUP_TTL_SEC", str(7 * 24 * 3600)))

FMP_API_KEY = os.getenv("FMP_API_KEY", "").strip()
FMP_URL = os.getenv("FMP_ECONOMIC_CAL_URL", "https://financialmodelingprep.com/stable/economic-calendar")
FMP_NAME = os.getenv("FMP_SOURCE_NAME", "fmp").strip()  # matches Go default "fmp"


def _sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _uid(source: str, event: str, country: str, currency: str, date_str: str) -> str:
    # Matches Go hashUID(source,event,country,currency,date).
    return _sha1_hex(f"{source}|{event}|{country}|{currency}|{date_str}")


def _http_get_json(url: str, params: dict[str, Any], timeout_sec: float = 10.0) -> Any:
    if httpx is not None:
        with httpx.Client(timeout=timeout_sec) as c:
            r = c.get(url, params=params)
            r.raise_for_status()
            return r.json()
    if requests is not None:
        r = requests.get(url, params=params, timeout=timeout_sec)
        r.raise_for_status()
        return r.json()
    raise RuntimeError("No HTTP client available (httpx/requests)")


def fetch_fmp_calendar() -> list[dict[str, Any]]:
    if not FMP_API_KEY:
        return []

    data = _http_get_json(FMP_URL, params={"apikey": FMP_API_KEY}, timeout_sec=12.0)
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    now_ms = get_ny_time_millis()

    for row in data:
        try:
            # FMP fields are not strictly documented; typical keys:
            # date, country, event, currency, actual, previous, forecast, impact
            date_str = (row.get("date") or "")
            event = str(row.get("event") or row.get("title") or "")
            country = (row.get("country") or "")
            currency = (row.get("currency") or "")
            impact = str(row.get("impact") or row.get("importance") or "").lower()

            # Normalize importance to 0..3
            # common values: "Low"/"Medium"/"High"
            if impact in ("high", "3"):
                importance = 3
            elif impact in ("medium", "2"):
                importance = 2
            elif impact in ("low", "1"):
                importance = 1
            else:
                importance = 0

            # Convert date to epoch ms (best-effort).
            # FMP often sends ISO-like "2026-01-03 13:30:00"
            ts_ms = 0
            try:
                from datetime import datetime

                # allow both with and without timezone
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
            except Exception:
                ts_ms = 0

            uid = _uid(FMP_NAME, event, country, currency, date_str)

            out.append(
                {
                    "uid": uid,
                    "event_ts_ms": ts_ms,
                    "ingested_ts_ms": now_ms,
                    "country": country,
                    "currency": currency,
                    "title": event,
                    "importance": importance,
                    "forecast": (row.get("forecast") or ""),
                    "previous": (row.get("previous") or ""),
                    "unit": (row.get("unit") or ""),
                    "source": FMP_NAME,
                    "payload": json.dumps(row, ensure_ascii=False),
                }
            )
        except Exception:
            continue

    return out


def publish_events(r: redis.Redis, events: list[dict[str, Any]]) -> int:
    n = 0
    for ev in events:
        uid = (ev.get("uid") or "")
        if not uid:
            continue

        # Dedupe (prevents re-publishing same event each poll)
        dedupe_key = f"calendar:dedupe:{uid}"
        if not r.set(dedupe_key, "1", nx=True, ex=DEDUP_TTL_SEC):
            continue

        try:
            r.xadd(STREAM, ev, maxlen=100_000, approximate=True)
            n += 1
        except Exception as e:
            # DLQ is best-effort
            with contextlib.suppress(Exception):
                r.xadd(DLQ, {"uid": uid, "err": str(e)[:256], "payload": json.dumps(ev)}, maxlen=200_000, approximate=True)
    return n


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
            log.info("Redis connection established successfully")
            return r
        except redis.BusyLoadingError:
            retry_count += 1
            log.warning(f"Redis is loading dataset, waiting... ({retry_count}/{max_retries})")
            time.sleep(10)
        except Exception as e:
            retry_count += 1
            log.warning(f"Redis connection failed (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(10)

    raise Exception(f"Failed to connect to Redis after {max_retries} retries")


def _write_heartbeat(r: redis.Redis, *, ok: bool, err: str = "", added: int = 0) -> None:
    """Write hb:calendar key so news-watchdog does not warn."""
    try:
        import json as _json
        obj = {
            "ts_ms": get_ny_time_millis(),
            "kind": "calendar",
            "ok": ok,
            "err": err[:512],
            "added": added,
            "instance": INSTANCE_ID,
        }
        r.set("hb:calendar", _json.dumps(obj, separators=(",", ":")), ex=HEARTBEAT_TTL_SEC)
    except Exception:
        pass


def main() -> None:
    r = _wait_for_redis_ready(REDIS_URL)

    lock = LeaderLock.new(r=r, key=LEADER_KEY, ttl_sec=LEADER_TTL_SEC, prefix="py-cal")
    is_leader = False
    last_renew = 0.0

    while True:
        try:
            if not is_leader:
                is_leader = lock.try_acquire()
                if not is_leader:
                    time.sleep(0.5)
                    continue

            # renew periodically (half-ttl)
            now = time.monotonic()
            if now - last_renew >= (LEADER_TTL_SEC / 2.0):
                if not lock.renew():
                    is_leader = False
                    continue
                last_renew = now

            events = fetch_fmp_calendar()
            added = 0
            if events:
                added = publish_events(r, events)
            _write_heartbeat(r, ok=True, added=added)

            time.sleep(POLL_SEC)

        except Exception as exc:
            # fail-open; do not crash the process
            _write_heartbeat(r, ok=False, err=str(exc)[:256])
            time.sleep(min(5.0, POLL_SEC))


if __name__ == "__main__":
    main()
