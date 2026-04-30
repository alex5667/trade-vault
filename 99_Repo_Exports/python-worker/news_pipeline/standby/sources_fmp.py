# python-worker/news_pipeline/standby/sources_fmp.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import json
import time
import urllib.parse
import urllib.request
from typing import Dict, Any, List, Iterable

def _get_json(url: str, *, timeout: float = 10.0, user_agent: str = "trade-standby/1.0") -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", errors="replace")
    return json.loads(raw)

def fetch_fmp_stock_news(
    *
    base_url: str
    path: str
    api_key: str
    tickers: Iterable[str]
    limit: int = 50
    timeout: float = 10.0
    user_agent: str = "trade-standby/1.0"
) -> List[Dict[str, Any]]:
    """
    FMP stock_news rows (пример из вашего Go):
      {symbol, publishedDate, title, url, text, site}
    """
    out: List[Dict[str, Any]] = []
    now_ms = get_ny_time_millis()

    for t in tickers:
        t = (t or "").strip().upper()
        if not t:
            continue

        qs = urllib.parse.urlencode({"tickers": t, "limit": str(limit), "apikey": api_key})
        url = f"{base_url.rstrip('/')}{path}?{qs}"
        rows = _get_json(url, timeout=timeout, user_agent=user_agent) or []

        if not isinstance(rows, list):
            continue

        for r in rows:
            if not isinstance(r, dict):
                continue

            title = str(r.get("title") or "").strip()
            link = str(r.get("url") or "").strip()
            pub = str(r.get("publishedDate") or "").strip()

            # publishedDate в Go используется и как provider_id, и как источник времени
            # здесь — максимально безопасно: если не распарсили, fallback now
            pub_ms = now_ms
            # FMP обычно отдаёт "YYYY-MM-DD HH:MM:SS" (ваш Go parseFMPTimeMs)
            try:
                # очень мягкий парсер без tz; если нужно точно — используйте вашу parseFMPTimeMs 1:1
                # (ниже я оставляю как fallback-логика)
                import datetime as _dt
                dt = _dt.datetime.fromisoformat(pub.replace("Z", "+00:00")) if "T" in pub else _dt.datetime.fromisoformat(pub)
                pub_ms = int(dt.timestamp() * 1000)
            except Exception:
                pub_ms = now_ms

            if not title or not link:
                continue

            out.append({
                "published_ts_ms": pub_ms
                "ingested_ts_ms": now_ms
                "source": "fmp"
                "title": title
                "url": link
                "summary": str(r.get("text") or "")[:4000]
                "symbols": [t]
                "importance": 0.0
                "payload": r
                "provider_id": pub,  # важно для UID
            })

    return out
