# news_pipeline/sources/fmp_calendar.py
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from news_pipeline.calendar_mapping import map_calendar_asset_classes
from utils.time_utils import get_ny_time_millis

DEFAULT_BASE_URL = "https://financialmodelingprep.com"


def stable_uid(*parts: str) -> str:
    """
    StableUID: sha256(parts joined with 0x1f)[:24]
    """
    sep = b"\x1f"
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(sep)
    return h.hexdigest()[:24]


def bucket_start_ms(ts_ms: int, bucket_ms: int) -> int:
    if ts_ms <= 0 or bucket_ms <= 0:
        return 0
    return (int(ts_ms) // int(bucket_ms)) * int(bucket_ms)


def _parse_time_ms(s: str) -> int:
    """
    Аналог Go-логики (плюс немного расширено):
      - RFC3339
      - "YYYY-MM-DD HH:MM:SS"
      - "YYYY-MM-DD"
      - ISO без tz
    """
    if not s:
        return 0
    s = s.strip()
    layouts = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for l in layouts:
        try:
            dt = datetime.strptime(s, l)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _importance_to_int(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return max(0, min(3, int(v)))
    s = str(v).strip().lower()
    if s in ("high", "3"):
        return 3
    if s in ("medium", "2"):
        return 2
    if s in ("low", "1"):
        return 1
    return 0


def _http_get_json(url: str, *, headers: dict[str, str], timeout_sec: float) -> Any:
    req = Request(url, method="GET")
    for k, v in headers.items():
        req.add_header(k, v)
    with urlopen(req, timeout=timeout_sec) as resp:
        code = getattr(resp, "status", 200)
        if code < 200 or code >= 300:
            raise RuntimeError(f"http {code}")
        data = resp.read(8 * 1024 * 1024)  # 8MB max
    return json.loads(data.decode("utf-8", errors="replace"))


@dataclass(frozen=True, slots=True)
class FMPCalendarConfig:
    enabled: bool = True
    base_url: str = DEFAULT_BASE_URL
    back_days: int = 1
    lookahead_days: int = 7
    countries: tuple[str, ...] = ()
    importance: tuple[str, ...] = ()
    user_agent: str = "trade-news-standby/1.0"
    http_timeout_sec: float = 12.0

    @staticmethod
    def from_sources_json(src: dict[str, Any]) -> FMPCalendarConfig:
        """
        Совместимо с вашим Go-конфигом:
        {
          "enabled": true,
          "base_url": "...",
          "backDays": 1,
          "lookaheadDays": 7,
          "countries": ["US","EU"],
          "importance": ["High","Medium"],
          "user_agent": "..."
        }
        """
        if not isinstance(src, dict):
            return FMPCalendarConfig(enabled=False)

        enabled = bool(src.get("enabled", True))
        base_url = str(src.get("base_url") or src.get("baseURL") or DEFAULT_BASE_URL)
        back_days = int(src.get("backDays", src.get("back_days", 1)) or 1)
        lookahead_days = int(src.get("lookaheadDays", src.get("lookahead_days", 7)) or 7)
        countries = tuple([str(x).upper() for x in (src.get("countries") or []) if str(x).strip()])
        importance = tuple([str(x).capitalize() for x in (src.get("importance") or []) if str(x).strip()])
        ua = str(src.get("user_agent") or src.get("userAgent") or "trade-news-standby/1.0")
        timeout = float(src.get("http_timeout_sec", src.get("httpTimeoutSec", 12.0)) or 12.0)

        return FMPCalendarConfig(
            enabled=enabled,
            base_url=base_url,
            back_days=max(0, back_days),
            lookahead_days=max(1, lookahead_days),
            countries=countries,
            importance=importance,
            user_agent=ua,
            http_timeout_sec=timeout,
        )


def fetch_fmp_economic_calendar(*, cfg: FMPCalendarConfig) -> list[dict[str, Any]]:
    """
    GET /stable/economic-calendar?from=YYYY-MM-DD&to=YYYY-MM-DD&apikey=...
    """
    api_key = (os.getenv("FMP_API_KEY", "") or "").strip()
    if not cfg.enabled or not api_key:
        return []

    now = datetime.now(UTC)
    from_str = now.replace(hour=0, minute=0, second=0, microsecond=0).date().isoformat()
    # back/lookahead
    from_ord = now.date().toordinal() - cfg.back_days
    to_ord = now.date().toordinal() + cfg.lookahead_days
    from_str = datetime.fromordinal(from_ord).date().isoformat()
    to_str = datetime.fromordinal(to_ord).date().isoformat()

    base = (cfg.base_url or DEFAULT_BASE_URL).rstrip("/")
    url = f"{base}/stable/economic-calendar?" + urlencode({"from": from_str, "to": to_str, "apikey": api_key})

    headers = {}
    if cfg.user_agent:
        headers["User-Agent"] = cfg.user_agent

    obj = _http_get_json(url, headers=headers, timeout_sec=cfg.http_timeout_sec)

    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        data = obj.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    return []


def normalize_calendar_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Превращает raw rows -> stream fields (stringable), делает fan-out по asset_class.
    """
    out: list[dict[str, Any]] = []
    now_ms = get_ny_time_millis()
    DAY_MS = 24 * 3600 * 1000

    for r in rows:
        title = str(r.get("event") or r.get("title") or r.get("name") or "").strip()
        if not title:
            continue

        date_s = str(r.get("date") or r.get("datetime") or r.get("publishedDate") or "").strip()
        event_ts_ms = _parse_time_ms(date_s)
        if event_ts_ms <= 0:
            continue

        country = str(r.get("country") or r.get("countryCode") or "").strip()
        currency = str(r.get("currency") or r.get("currencyCode") or "").strip().upper()
        importance = _importance_to_int(r.get("impact") or r.get("importance") or r.get("volatility"))

        # при желании — фильтруйте по cfg.countries/cfg.importance на уровне fetcher-а
        forecast = "" if r.get("forecast") is None else (r.get("forecast"))
        previous = "" if r.get("previous") is None else (r.get("previous"))
        unit = "" if r.get("unit") is None else (r.get("unit"))

        provider_id = str(r.get("id") or r.get("eventId") or r.get("updated") or "").strip()
        if not provider_id:
            provider_id = f"{date_s}|{currency}|{country}|{title}"

        bucket_ms = bucket_start_ms(event_ts_ms, DAY_MS)

        for ac in map_calendar_asset_classes(country=country, currency=currency, title=title, importance=importance):
            uid = stable_uid("fmpcal", currency, title, provider_id, str(bucket_ms), ac)

            out.append({
                "uid": uid,
                "asset_class": ac,
                "event_ts_ms": str(int(event_ts_ms)),
                "ingested_ts_ms": str(int(now_ms)),
                "country": country,
                "currency": currency,
                "title": title,
                "importance": str(int(importance)),
                "forecast": forecast,
                "previous": previous,
                "unit": unit,
                "source": "fmp",
                "payload": json.dumps(r, ensure_ascii=False),
            })

    return out
