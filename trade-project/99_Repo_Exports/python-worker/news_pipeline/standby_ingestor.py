from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""news_pipeline.standby_ingestor

Python standby ingestor that can fully replace go-news-services when Go is down.

Goals
-----
1) Bit-for-bit compatible leader election with Go (no split-brain):
   - key: NEWS_INGESTOR_LEADER_KEY (default: news:ingestor:leader)
   - value: unique per process (python:<unixnano>)
   - acquire: SET NX PX ttl
   - renew: Lua (GET==value ? PEXPIRE : 0)

2) Produce the same Redis streams and heartbeats:
   - news:raw             (NewsRawItem fields)
   - calendar:events      (CalendarEvent fields)
   - hb:<kind>            JSON with TTL + history stream

3) Fetch the same providers and URL constructors as Go:
   - CryptoPanic (/api/v1/posts/)
   - FMP stock_news (/api/v3/stock_news)
   - FMP economic-calendar (/stable/economic-calendar)
   - NewsAPI (/v2/everything)
   - RSS list (feedparser)

Enable/disable
--------------
Set STANDBY_INGESTOR_ENABLE=1 to run.
If Go is running and holding the leader lock, the standby stays idle.
"""

import hashlib
import json
import logging
import os
import time
from calendar import timegm as _timegm
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import redis
import requests

log = logging.getLogger("standby_ingestor")

try:
    import feedparser  # type: ignore
except Exception:  # pragma: no cover
    feedparser = None  # type: ignore


# ---------------- utils ----------------

def now_ms() -> int:
    return get_ny_time_millis()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()


def bucket_start_ms(ts_ms: int, bucket_sec: int) -> int:
    if bucket_sec <= 0:
        return ts_ms
    b = bucket_sec * 1000
    return (ts_ms // b) * b


def stable_uid(*parts: str) -> str:
    # same idea as Go: stable across restarts for identical item
    # (we use sha1 for speed/compat; length does not matter, store full)
    raw = "\x1f".join([p.strip() for p in parts if p is not None])
    return _sha1_hex(raw)


# ---------------- leader lock ----------------

RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"""


class LeaderLock:
    def __init__(self, r: redis.Redis, key: str, ttl_ms: int, value: str) -> None:
        self.r = r
        self.key = key
        self.ttl_ms = ttl_ms
        self.value = value
        self._script = r.register_script(RENEW_SCRIPT)

    def try_acquire(self) -> bool:
        return bool(self.r.set(self.key, self.value, nx=True, px=self.ttl_ms))

    def renew(self) -> bool:
        try:
            res = self._script(keys=[self.key], args=[self.value, str(int(self.ttl_ms))])
            try:
                return int(res) > 0
            except Exception:
                return bool(res)
        except Exception:
            return False


# ---------------- heartbeat ----------------

def write_heartbeat(r: redis.Redis, *, kind: str, ok: bool, err: str = "", added: int = 0, ttl_sec: int = 30, instance: str = "") -> None:
    try:
        obj = {
            "ts_ms": now_ms(),
            "kind": kind,
            "ok": bool(ok),
            "err": (err or ""),
            "added": int(added),
            "instance": instance,
        }
        raw = json.dumps(obj, separators=(",", ":"))
        r.set(f"hb:{kind}", raw, ex=int(ttl_sec))
        # history stream
        r.xadd(
            f"hb:{kind}:stream",
            {
                "ts_ms": str(obj["ts_ms"]),
                "ok": "1" if ok else "0",
                "err": obj["err"][:512],
                "added": str(added),
                "instance": instance,
            },
            maxlen=10000,
            approximate=True,
        )
    except Exception:
        pass


# ---------------- provider configs ----------------

@dataclass(slots=True)
class RSSCfg:
    enabled: bool
    urls: list[str]
    user_agent: str = ""


@dataclass(slots=True)
class CryptoPanicCfg:
    enabled: bool
    base_url: str = "https://cryptopanic.com"
    currencies: list[str] = None  # type: ignore
    kind: str = ""
    filter: str = ""
    regions: str = ""
    user_agent: str = ""

    def __post_init__(self) -> None:
        if self.currencies is None:
            self.currencies = []


@dataclass(slots=True)
class FMPCfg:
    enabled: bool
    base_url: str = "https://financialmodelingprep.com"
    tickers: list[str] = None  # type: ignore
    limit: int = 50
    user_agent: str = ""

    # economic calendar sub-config
    cal_enabled: bool = False
    cal_back_days: int = 1
    cal_lookahead_days: int = 7
    cal_countries: list[str] = None  # type: ignore
    cal_importance: list[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.tickers is None:
            self.tickers = []
        if self.cal_countries is None:
            self.cal_countries = []
        if self.cal_importance is None:
            self.cal_importance = []


@dataclass(slots=True)
class NewsAPICfg:
    enabled: bool
    base_url: str = "https://newsapi.org"
    q: str = "crypto OR bitcoin OR ethereum"
    language: str = "en"
    page_size: int = 50
    user_agent: str = ""


def parse_sources_json(raw: str) -> tuple[RSSCfg, CryptoPanicCfg, FMPCfg, NewsAPICfg]:
    obj = {}
    try:
        obj = json.loads(raw or "{}")
    except Exception:
        obj = {}

    providers = set([p.strip().lower() for p in (obj.get("providers") or []) if isinstance(p, str)])

    # RSS
    rss_o = obj.get("rss") or {}
    rss = RSSCfg(
        enabled=("rss" in providers) and bool(rss_o.get("enabled", True)),
        urls=list(rss_o.get("urls") or []),
        user_agent=(rss_o.get("user_agent") or ""),
    )

    # CryptoPanic
    cp_o = obj.get("cryptopanic") or {}
    cp = CryptoPanicCfg(
        enabled=("cryptopanic" in providers) and bool(cp_o.get("enabled", True)),
        base_url=(cp_o.get("base_url") or "https://cryptopanic.com"),
        currencies=list(cp_o.get("currencies") or []),
        kind=(cp_o.get("kind") or ""),
        filter=(cp_o.get("filter") or ""),
        regions=(cp_o.get("regions") or ""),
        user_agent=(cp_o.get("user_agent") or ""),
    )

    # FMP
    fmp_o = obj.get("fmp") or {}
    econ_o = fmp_o.get("economic") or {}
    fmp = FMPCfg(
        enabled=("fmp" in providers) and bool(fmp_o.get("enabled", True)),
        base_url=(fmp_o.get("base_url") or "https://financialmodelingprep.com"),
        tickers=list(fmp_o.get("tickers") or []),
        limit=int(fmp_o.get("limit") or 50),
        user_agent=(fmp_o.get("user_agent") or ""),
        cal_enabled=bool(fmp_o.get("calendar_enabled", True)) and bool(fmp_o.get("enabled", True)),
        cal_back_days=int(fmp_o.get("backDays") or 1),
        cal_lookahead_days=int(fmp_o.get("lookaheadDays") or 7),
        cal_countries=list(econ_o.get("countries") or []),
        cal_importance=list(econ_o.get("importance") or []),
    )

    # NewsAPI
    na_o = obj.get("newsapi") or {}
    na = NewsAPICfg(
        enabled=("newsapi" in providers) and bool(na_o.get("enabled", True)),
        base_url=(na_o.get("base_url") or "https://newsapi.org"),
        q=(na_o.get("q") or ""),
        language=(na_o.get("language") or ""),
        page_size=int(na_o.get("pageSize") or 50),
        user_agent=(na_o.get("user_agent") or ""),
    )

    return rss, cp, fmp, na


# ---------------- fetch helpers ----------------

def _http_get_json(url: str, *, headers: dict[str, str], timeout: int) -> Any:
    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code // 100 != 2:
        raise RuntimeError(f"http {resp.status_code}")
    return resp.json()


def _add_query(url: str, params: dict[str, str]) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.update({k: v for k, v in params.items() if v != ""})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))


def _parse_rfc3339_ms(s: str) -> int:
    # lightweight parse without dateutil (avoid heavy dep)
    # accept "2024-01-02T03:04:05Z" or with offset
    try:
        import datetime as _dt
        t = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(t.timestamp() * 1000)
    except Exception:
        return 0


def _parse_fmp_time_ms(s: str) -> int:
    # Go prod layouts: RFC3339, "2006-01-02 15:04:05", "2006-01-02"
    if not s:
        return 0
    # try isoformat first
    ms = _parse_rfc3339_ms(s)
    if ms:
        return ms
    try:
        import datetime as _dt
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                t = _dt.datetime.strptime(s, fmt)
                t = t.replace(tzinfo=_dt.UTC)
                return int(t.timestamp() * 1000)
            except Exception:
                pass
    except Exception:
        return 0
    return 0


# ---------------- providers ----------------

def fetch_cryptopanic(cfg: CryptoPanicCfg, *, timeout: int) -> list[dict[str, Any]]:
    token = os.getenv("CRYPTOPANIC_AUTH_TOKEN", "").strip()
    if not cfg.enabled or not token:
        return []
    url = cfg.base_url.rstrip("/") + "/api/v1/posts/"
    params: dict[str, str] = {"auth_token": token}
    if cfg.currencies:
        params["currencies"] = ",".join(cfg.currencies)
    if cfg.kind:
        params["kind"] = cfg.kind
    if cfg.filter:
        params["filter"] = cfg.filter
    if cfg.regions:
        params["regions"] = cfg.regions
    url = _add_query(url, params)
    headers = {}
    if cfg.user_agent:
        headers["User-Agent"] = cfg.user_agent
    data = _http_get_json(url, headers=headers, timeout=timeout)
    posts = data.get("results") if isinstance(data, dict) else None
    if not isinstance(posts, list):
        return []
    out: list[dict[str, Any]] = []
    for it in posts:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "")
        link = str(it.get("url") or it.get("link") or "")
        published = str(it.get("published_at") or it.get("created_at") or "")
        published_ms = _parse_rfc3339_ms(published) or now_ms()
        # symbols: try currencies list
        symbols: list[str] = []
        try:
            cur = it.get("currencies") or []
            if isinstance(cur, list):
                for c in cur:
                    if isinstance(c, dict) and c.get("code"):
                        symbols.append(str(c["code"]))
                    elif isinstance(c, str):
                        symbols.append(c)
        except Exception:
            pass
        provider_id = (it.get("id") or "")
        uid = stable_uid("cryptopanic", provider_id, link, title, str(bucket_start_ms(published_ms, _env_int("NEWS_UID_BUCKET_SEC", 6*3600))))
        out.append(
            {
                "uid": uid,
                "published_ts_ms": published_ms,
                "source": "cryptopanic",
                "title": title,
                "url": link,
                "summary": (it.get("domain") or ""),
                "symbols": symbols,
                "payload": it,
            }
        )
    return out


def fetch_fmp_stock_news(cfg: FMPCfg, *, timeout: int) -> list[dict[str, Any]]:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not cfg.enabled or not key:
        return []
    url = cfg.base_url.rstrip("/") + "/api/v3/stock_news"
    params: dict[str, str] = {
        "apikey": key,
        "limit": str(int(cfg.limit or 50)),
    }
    if cfg.tickers:
        params["tickers"] = ",".join(cfg.tickers)
    url = _add_query(url, params)
    headers = {}
    if cfg.user_agent:
        headers["User-Agent"] = cfg.user_agent
    data = _http_get_json(url, headers=headers, timeout=timeout)
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for it in data:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "")
        link = (it.get("url") or "")
        published = str(it.get("publishedDate") or it.get("published_date") or "")
        published_ms = _parse_fmp_time_ms(published) or now_ms()
        sym = (it.get("symbol") or "")
        symbols = [sym] if sym else []
        provider_id = str(it.get("id") or it.get("publishedDate") or "")
        uid = stable_uid("fmp_stock_news", provider_id, link, title, str(bucket_start_ms(published_ms, _env_int("NEWS_UID_BUCKET_SEC", 6*3600))))
        out.append(
            {
                "uid": uid,
                "published_ts_ms": published_ms,
                "source": "fmp",
                "title": title,
                "url": link,
                "summary": (it.get("text") or "")[:512],
                "symbols": symbols,
                "payload": it,
            }
        )
    return out


def fetch_newsapi(cfg: NewsAPICfg, *, timeout: int) -> list[dict[str, Any]]:
    key = os.getenv("NEWSAPI_KEY", "").strip()
    if not cfg.enabled or not key:
        return []
    url = cfg.base_url.rstrip("/") + "/v2/everything"
    params: dict[str, str] = {
        "q": cfg.q,
        "pageSize": str(int(cfg.page_size or 50)),
    }
    if cfg.language:
        params["language"] = cfg.language
    url = _add_query(url, params)
    headers = {"X-Api-Key": key}
    if cfg.user_agent:
        headers["User-Agent"] = cfg.user_agent
    data = _http_get_json(url, headers=headers, timeout=timeout)
    articles = data.get("articles") if isinstance(data, dict) else None
    if not isinstance(articles, list):
        return []
    out: list[dict[str, Any]] = []
    for it in articles:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "")
        link = (it.get("url") or "")
        published = (it.get("publishedAt") or "")
        published_ms = _parse_rfc3339_ms(published) or now_ms()
        # NewsAPI doesn't provide tickers; keep GLOBAL
        provider_id = str(it.get("source", {}).get("id") or it.get("source", {}).get("name") or "")
        uid = stable_uid("newsapi", provider_id, link, title, str(bucket_start_ms(published_ms, _env_int("NEWS_UID_BUCKET_SEC", 6*3600))))
        out.append(
            {
                "uid": uid,
                "published_ts_ms": published_ms,
                "source": "newsapi",
                "title": title,
                "url": link,
                "summary": (it.get("description") or "")[:512],
                "symbols": [],
                "payload": it,
            }
        )
    return out


def _map_importance(v: Any) -> int:
    # accept: 0..3 int OR strings: High/Medium/Low
    try:
        i = int(v)
        return max(0, min(3, i))
    except Exception:
        s = (v or "").strip().lower()
        if s in ("high", "3"):
            return 3
        if s in ("medium", "2", "med"):
            return 2
        if s in ("low", "1"):
            return 1
        return 0


def fetch_fmp_calendar(cfg: FMPCfg, *, timeout: int) -> list[dict[str, Any]]:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not (cfg.enabled and cfg.cal_enabled) or not key:
        return []
    import datetime as _dt

    now = _dt.datetime.now(dt.timezone.utc).replace(tzinfo=_dt.UTC)
    frm = (now - _dt.timedelta(days=max(0, int(cfg.cal_back_days)))).date().isoformat()
    to = (now + _dt.timedelta(days=max(1, int(cfg.cal_lookahead_days)))).date().isoformat()

    endpoint = cfg.base_url.rstrip("/") + "/stable/economic-calendar"
    url = _add_query(endpoint, {"from": frm, "to": to, "apikey": key})
    headers = {}
    if cfg.user_agent:
        headers["User-Agent"] = cfg.user_agent

    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code in (401, 403):
        return []
    if resp.status_code // 100 != 2:
        raise RuntimeError(f"fmp calendar http {resp.status_code}")

    data = resp.json()
    if not isinstance(data, list):
        return []

    out: list[dict[str, Any]] = []
    for it in data:
        if not isinstance(it, dict):
            continue
        # typical FMP fields: date, country, event, currency, previous, estimate/forecast, impact
        title = str(it.get("event") or it.get("title") or "")
        date_s = (it.get("date") or "")
        event_ts_ms = _parse_fmp_time_ms(date_s)
        if not event_ts_ms:
            continue
        country = (it.get("country") or "").upper()
        currency = (it.get("currency") or "").upper()
        importance = _map_importance(it.get("impact") or it.get("importance") or 0)
        forecast = str(it.get("estimate") or it.get("forecast") or "")
        previous = (it.get("previous") or "")
        unit = (it.get("unit") or "")
        uid = stable_uid("fmp", title, country, currency, date_s)
        out.append(
            {
                "uid": uid,
                "event_ts_ms": event_ts_ms,
                "country": country,
                "currency": currency,
                "title": title,
                "importance": importance,
                "forecast": forecast,
                "previous": previous,
                "unit": unit,
                "source": "fmp",
                "payload": it,
            }
        )
    return out


def fetch_rss(cfg: RSSCfg) -> list[dict[str, Any]]:
    if not cfg.enabled or not cfg.urls:
        return []
    if feedparser is None:
        log.warning("RSS enabled but feedparser is not installed; install feedparser or disable RSS.")
        return []
    out: list[dict[str, Any]] = []
    bucket_sec = _env_int("NEWS_UID_BUCKET_SEC", 6 * 3600)
    for feed_url in cfg.urls:
        try:
            d = feedparser.parse(feed_url)
            for e in d.entries or []:
                title = str(getattr(e, "title", "") or "")
                link = str(getattr(e, "link", "") or "")
                guid = str(getattr(e, "id", "") or getattr(e, "guid", "") or "")
                summary = str(getattr(e, "summary", "") or "")[:512]
                # published time
                ts = 0
                try:
                    if getattr(e, "published_parsed", None):
                        ts = int(_timegm(e.published_parsed) * 1000)  # type: ignore[arg-type]
                    elif getattr(e, "updated_parsed", None):
                        ts = int(_timegm(e.updated_parsed) * 1000)  # type: ignore[arg-type]
                except Exception:
                    ts = 0
                if not ts:
                    ts = now_ms()
                uid = stable_uid("rss", feed_url, guid, link, title, str(bucket_start_ms(ts, bucket_sec)))
                out.append(
                    {
                        "uid": uid,
                        "published_ts_ms": ts,
                        "source": "rss",
                        "title": title,
                        "url": link,
                        "summary": summary,
                        "symbols": [],
                        "payload": {"feed": feed_url, "guid": guid},
                    }
                )
        except Exception as e:
            log.warning("rss fetch failed %s: %s", feed_url, e)
            continue
    return out


# ---------------- main loop ----------------

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
    if not _env_bool("STANDBY_INGESTOR_ENABLE", False):
        raise SystemExit("STANDBY_INGESTOR_ENABLE is not set")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = _wait_for_redis_ready(redis_url)

    instance = os.getenv("INSTANCE_ID", f"py:{os.getpid()}")

    leader_key = os.getenv("NEWS_INGESTOR_LEADER_KEY", "news:ingestor:leader")
    ttl_ms = _env_int("NEWS_INGESTOR_LEADER_TTL_MS", 8000)
    lock_value = f"python:{time.time_ns()}"
    lock = LeaderLock(r, leader_key, ttl_ms, lock_value)

    news_stream = os.getenv("NEWS_RAW_STREAM", RS.NEWS_RAW)
    cal_stream = os.getenv("CALENDAR_EVENTS_STREAM", RS.CALENDAR_EVENTS)
    news_maxlen = _env_int("NEWS_RAW_MAXLEN", 200000)
    cal_maxlen = _env_int("CALENDAR_EVENTS_MAXLEN", 200000)
    dedupe_ttl_sec = _env_int("DEDUPE_TTL_SEC", 6 * 3600)  # 6h default per request
    poll_interval = _env_float("POLL_INTERVAL_SEC", 15.0)
    cal_poll_interval = _env_float("CAL_POLL_INTERVAL_SEC", 60.0)
    hb_ttl = _env_int("HEARTBEAT_TTL_SEC", 30)
    http_timeout = _env_int("HTTP_TIMEOUT_SEC", 8)

    sources_json = os.getenv("NEWS_SOURCES_JSON", "{}")
    rss_cfg, cp_cfg, fmp_cfg, na_cfg = parse_sources_json(sources_json)

    last_news = 0.0
    last_cal = 0.0

    next_renew = 0.0
    leader = False

    while True:
        now = time.time()

        # Acquire or renew leader lock
        if not leader:
            try:
                leader = lock.try_acquire()
                if leader:
                    next_renew = now + (ttl_ms / 1000.0) * 0.5
                    log.info("standby became leader: key=%s value=%s", leader_key, lock_value)
            except Exception:
                leader = False
        else:
            if now >= next_renew:
                ok = lock.renew()
                if not ok:
                    leader = False
                    log.warning("leader lock lost")
                else:
                    next_renew = now + (ttl_ms / 1000.0) * 0.5

        if not leader:
            # idle, but still write heartbeat
            write_heartbeat(r, kind="news", ok=True, err="idle (not leader)", ttl_sec=hb_ttl, instance=instance)
            write_heartbeat(r, kind="calendar", ok=True, err="idle (not leader)", ttl_sec=hb_ttl, instance=instance)
            time.sleep(1.0)
            continue

        # ---- NEWS ----
        if now - last_news >= poll_interval:
            last_news = now
            added = 0
            err = ""
            ok = True
            try:
                items: list[dict[str, Any]] = []
                items.extend(fetch_rss(rss_cfg))
                items.extend(fetch_cryptopanic(cp_cfg, timeout=http_timeout))
                items.extend(fetch_fmp_stock_news(fmp_cfg, timeout=http_timeout))
                items.extend(fetch_newsapi(na_cfg, timeout=http_timeout))

                ing_ms = now_ms()
                for it in items:
                    uid = str(it["uid"])
                    # dedupe key
                    if not r.set(f"news:dedupe:{uid}", "1", nx=True, ex=int(dedupe_ttl_sec)):
                        continue
                    fields = {
                        "uid": uid,
                        "published_ts_ms": str(int(it.get("published_ts_ms") or ing_ms)),
                        "ingested_ts_ms": str(ing_ms),
                        "source": (it.get("source") or ""),
                        "title": (it.get("title") or "")[:512],
                        "url": (it.get("url") or "")[:1024],
                        "summary": (it.get("summary") or "")[:1024],
                        "symbols": json.dumps(it.get("symbols") or [], separators=(",", ":")),
                        "payload": json.dumps(it.get("payload") or {}, separators=(",", ":"))[:4096],
                    }
                    r.xadd(news_stream, fields, maxlen=news_maxlen, approximate=True)
                    added += 1

                write_heartbeat(r, kind="news", ok=True, added=added, ttl_sec=hb_ttl, instance=instance)

            except Exception as e:
                ok = False
                err = str(e)[:512]
                write_heartbeat(r, kind="news", ok=False, err=err, ttl_sec=hb_ttl, instance=instance)

        # ---- CALENDAR ----
        if now - last_cal >= cal_poll_interval:
            last_cal = now
            added = 0
            err = ""
            try:
                events = fetch_fmp_calendar(fmp_cfg, timeout=http_timeout)
                ing_ms = now_ms()
                for ev in events:
                    uid = str(ev["uid"])
                    if not r.set(f"calendar:dedupe:{uid}", "1", nx=True, ex=int(dedupe_ttl_sec)):
                        continue
                    fields = {
                        "uid": uid,
                        "event_ts_ms": str(int(ev.get("event_ts_ms") or 0)),
                        "ingested_ts_ms": str(ing_ms),
                        "country": (ev.get("country") or ""),
                        "currency": (ev.get("currency") or ""),
                        "title": (ev.get("title") or "")[:512],
                        "importance": str(int(ev.get("importance") or 0)),
                        "forecast": (ev.get("forecast") or "")[:64],
                        "previous": (ev.get("previous") or "")[:64],
                        "unit": (ev.get("unit") or "")[:16],
                        "source": (ev.get("source") or "fmp"),
                        "payload": json.dumps(ev.get("payload") or {}, separators=(",", ":"))[:4096],
                    }
                    r.xadd(cal_stream, fields, maxlen=cal_maxlen, approximate=True)
                    added += 1
                write_heartbeat(r, kind="calendar", ok=True, added=added, ttl_sec=hb_ttl, instance=instance)

            except Exception as e:
                err = str(e)[:512]
                write_heartbeat(r, kind="calendar", ok=False, err=err, ttl_sec=hb_ttl, instance=instance)

        time.sleep(0.2)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    run()
