# python-worker/news_pipeline/standby/standby_ingestor.py
from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from news_pipeline.standby.sources_cryptopanic import fetch_cryptopanic
from news_pipeline.standby.sources_fmp import fetch_fmp_stock_news
from news_pipeline.standby.sources_newsapi import fetch_newsapi_everything
from news_pipeline.standby.sources_rss import fetch_rss
from news_pipeline.standby.uid import UIDPolicy
from utils.time_utils import get_ny_time_millis

NEWS_RAW_STREAM = os.getenv("STREAM_NEWS_RAW", "news:raw")
MAX_STREAM_LEN = int(os.getenv("MAX_STREAM_LEN", "200000"))
DEDUP_PREFIX = os.getenv("NEWS_DEDUP_PREFIX", "news:dedupe:")
DEDUP_TTL_SEC = int(os.getenv("DEDUPE_TTL_SEC", str(7 * 24 * 3600)))
NEWS_UID_BUCKET_SEC = int(os.getenv("NEWS_UID_BUCKET_SEC", str(6 * 3600)))  # 6h default

HB_KEY = os.getenv("NEWS_HB_KEY", "hb:news")
HB_STALE_AFTER_MS = int(os.getenv("STANDBY_STALE_AFTER_MS", "60000"))  # > HeartbeatTTL (30s) с запасом
LOCK_KEY = os.getenv("STANDBY_LOCK_KEY", "news:standby:lock")
LOCK_TTL_SEC = int(os.getenv("STANDBY_LOCK_TTL_SEC", "30"))

POLL_SEC = float(os.getenv("STANDBY_POLL_SEC", "15"))

def _now_ms() -> int:
    return get_ny_time_millis()

def _is_stale_hb(raw: str | None) -> bool:
    if not raw:
        return True
    try:
        obj = json.loads(raw)
        ts = int(obj.get("ts_ms") or 0)
        ok = bool(obj.get("ok", True))
        if ts <= 0:
            return True
        if (_now_ms() - ts) > HB_STALE_AFTER_MS:
            return True
        if not ok:
            # если ok=false — считаем "опасно" сразу (можно сделать grace N раз)
            return True
        return False
    except Exception:
        return True

def _xadd_dedup(r: redis.Redis, *, uid: str, fields: dict[str, Any]) -> bool:
    """
    Полностью совместимо с вашим Go: SETNX + TTL, затем XADD.
    """
    if not uid:
        return False
    key = f"{DEDUP_PREFIX}{uid}"
    ok = r.setnx(key, "1")
    if not ok:
        return False
    r.expire(key, DEDUP_TTL_SEC)

    r.xadd(
        NEWS_RAW_STREAM,
        fields,
        maxlen=MAX_STREAM_LEN,
        approximate=True,
    )
    return True

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

def run() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = _wait_for_redis_ready(redis_url)

    uid_policy = UIDPolicy(bucket_ms=NEWS_UID_BUCKET_SEC * 1000)

    while True:
        try:
            hb_raw = r.get(HB_KEY)
            stale = _is_stale_hb(hb_raw)

            if not stale:
                time.sleep(POLL_SEC)
                continue

            # лидерство (чтобы не было 2 standby)
            got_lock = r.set(LOCK_KEY, "1", nx=True, ex=LOCK_TTL_SEC)
            if not got_lock:
                time.sleep(1.0)
                continue

            cfg = _load_sources_cfg()

            # --- RSS (всегда можно) ---
            if cfg["rss"]["enabled"]:
                items = fetch_rss(
                    name="rss",
                    urls=cfg["rss"]["urls"],
                    user_agent=cfg["user_agent"],
                )
                _ingest_items(r, uid_policy, items, provider_id_fallback="rss")

            # --- CryptoPanic (только если токен) ---
            if cfg["cryptopanic"]["enabled"] and cfg["cryptopanic"]["token"]:
                items = fetch_cryptopanic(
                    base_url=cfg["cryptopanic"]["base_url"],
                    path=cfg["cryptopanic"]["path"],
                    auth_token=cfg["cryptopanic"]["token"],
                    currencies=cfg["cryptopanic"]["currencies"],
                    filter_=cfg["cryptopanic"]["filter"],
                    kind=cfg["cryptopanic"]["kind"],
                    region=cfg["cryptopanic"]["region"],
                    user_agent=cfg["user_agent"],
                )
                _ingest_items(r, uid_policy, items, provider_id_key="provider_id", provider_id_fallback="cryptopanic")

            # --- FMP stock news (только если ключ) ---
            if cfg["fmp"]["enabled"] and cfg["fmp"]["api_key"]:
                items = fetch_fmp_stock_news(
                    base_url=cfg["fmp"]["base_url"],
                    path=cfg["fmp"]["stock_news_path"],
                    api_key=cfg["fmp"]["api_key"],
                    tickers=cfg["fmp"]["tickers"],
                    user_agent=cfg["user_agent"],
                )
                _ingest_items(r, uid_policy, items, provider_id_key="provider_id", provider_id_fallback="fmp")

            # --- NewsAPI everything (только если ключ) ---
            if cfg["newsapi"]["enabled"] and cfg["newsapi"]["api_key"]:
                items = fetch_newsapi_everything(
                    base_url=cfg["newsapi"]["base_url"],
                    path=cfg["newsapi"]["path"],
                    api_key=cfg["newsapi"]["api_key"],
                    q=cfg["newsapi"]["q"],
                    language=cfg["newsapi"]["language"],
                    user_agent=cfg["user_agent"],
                )
                _ingest_items(r, uid_policy, items, provider_id_key="provider_id", provider_id_fallback="newsapi")

            time.sleep(POLL_SEC)

        except Exception:
            # fail-open: standby не должен падать навсегда
            time.sleep(2.0)

def _ingest_items(
    r: redis.Redis,
    uid_policy: UIDPolicy,
    items: list[dict[str, Any]],
    *,
    provider_id_key: str = "provider_id",
    provider_id_fallback: str = "na",
) -> None:
    now_ms = _now_ms()

    for it in items:
        source = (it.get("source") or "").strip() or "unknown"
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        if not title or not url:
            continue

        published_ts_ms = int(it.get("published_ts_ms") or now_ms)
        provider_id = (it.get(provider_id_key) or "") or provider_id_fallback

        uid = uid_policy.uid_for_news(
            source=source,
            url=url,
            title=title,
            provider_id=provider_id,
            published_ts_ms=published_ts_ms,
        )

        symbols = it.get("symbols") or []
        try:
            symbols_json = json.dumps(list(symbols), ensure_ascii=False)
        except Exception:
            symbols_json = "[]"

        payload = it.get("payload") or {}
        try:
            payload_json = json.dumps(payload, ensure_ascii=False)
        except Exception:
            payload_json = "{}"

        fields = {
            "uid": uid,
            "published_ts_ms": str(published_ts_ms),
            "ingested_ts_ms": str(int(it.get("ingested_ts_ms") or now_ms)),
            "source": source,
            "title": title,
            "url": url,
            "summary": (it.get("summary") or ""),
            "symbols": symbols_json,
            "importance": str(float(it.get("importance") or 0.0)),
            "payload": payload_json,
        }

        _xadd_dedup(r, uid=uid, fields=fields)

def _load_sources_cfg() -> dict[str, Any]:
    """
    Загружает NEWS_SOURCES_JSON и включает провайдеры только если есть ключи.
    """
    raw = os.getenv("NEWS_SOURCES_JSON", "").strip()
    obj: dict[str, Any] = {}
    if raw:
        try:
            obj = json.loads(raw)
        except Exception:
            obj = {}

    def _get(d: dict[str, Any], k: str, default: Any) -> Any:
        v = d.get(k)
        return default if v is None else v

    # дефолт RSS из вашего Excel
    rss_def = [
        "https://bitcoinmagazine.com/.rss/full/",
        "https://cointelegraph.com/rss",
        "https://thedefiant.io/feed",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://www.ecb.europa.eu/rss/press.html"]

    providers = _get(obj, "providers", ["rss"])
    ua = os.getenv("USER_AGENT", "trade-news-standby/1.0")

    rss = _get(obj, "rss", {})
    rss_enabled = bool(_get(rss, "enabled", True)) and ("rss" in providers),
    rss_urls = _get(rss, "urls", rss_def) or rss_def,

    cp = _get(obj, "cryptopanic", {}),
    cp_token = os.getenv("CRYPTOPANIC_AUTH_TOKEN", "").strip(),
    cp_enabled = bool(_get(cp, "enabled", True)) and ("cryptopanic" in providers) and bool(cp_token),

    fmp = _get(obj, "fmp", {}),
    fmp_key = os.getenv("FMP_API_KEY", "").strip(),
    fmp_enabled = bool(_get(fmp, "enabled", True)) and ("fmp" in providers) and bool(fmp_key),

    na = _get(obj, "newsapi", {}),
    na_key = os.getenv("NEWSAPI_KEY", "").strip(),
    na_enabled = bool(_get(na, "enabled", True)) and ("newsapi" in providers) and bool(na_key),

    return {
        "user_agent": ua,
        "rss": {"enabled": rss_enabled, "urls": list(rss_urls)},
        "cryptopanic": {
            "enabled": cp_enabled,
            "token": cp_token,
            "base_url": str(_get(cp, "base_url", "https://cryptopanic.com")),
            "path": str(_get(cp, "path", "/api/v1/posts/")),
            "currencies": list(_get(cp, "currencies", ["BTC", "ETH"])),
            "filter": str(_get(cp, "filter", "important")),
            "kind": str(_get(cp, "kind", "news")),
            "region": str(_get(cp, "region", "en"))
        },
        "fmp": {
            "enabled": fmp_enabled,
            "api_key": fmp_key,
            "base_url": str(_get(fmp, "base_url", "https://financialmodelingprep.com")),
            "stock_news_path": str(_get(fmp, "stock_news_path", "/api/v3/stock_news")),
            "tickers": list(_get(fmp, "tickers", ["SPY", "QQQ"])),
        },
        "newsapi": {
            "enabled": na_enabled,
            "api_key": na_key,
            "base_url": str(_get(na, "base_url", "https://newsapi.org")),
            "path": str(_get(na, "path", "/v2/everything")),
            "q": str(_get(na, "q", "(bitcoin OR ethereum OR crypto)")),
            "language": str(_get(na, "language", "en")),
        }
    }

if __name__ == "__main__":
    run()
