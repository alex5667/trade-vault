# python-worker/news_pipeline/standby/sources_newsapi.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import json
import time
import urllib.parse
import urllib.request
from typing import Dict, Any, List

def fetch_newsapi_everything(
    *
    base_url: str
    path: str
    api_key: str
    q: str
    language: str = "en"
    page_size: int = 50
    timeout: float = 10.0
    user_agent: str = "trade-standby/1.0"
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now_ms = get_ny_time_millis()

    qs = urllib.parse.urlencode({
        "q": q
        "language": language
        "pageSize": str(page_size)
        "apiKey": api_key
        "sortBy": "publishedAt"
    })
    url = f"{base_url.rstrip('/')}{path}?{qs}"

    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", errors="replace")
    data = json.loads(raw) if raw else {}
    articles = (data or {}).get("articles") or []

    for a in articles:
        if not isinstance(a, dict):
            continue
        title = str(a.get("title") or "").strip()
        link = str(a.get("url") or "").strip()
        published_at = str(a.get("publishedAt") or "").strip()

        pub_ms = now_ms
        try:
            import datetime as _dt
            dt = _dt.datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            pub_ms = int(dt.timestamp() * 1000)
        except Exception:
            pub_ms = now_ms

        if not title or not link:
            continue

        out.append({
            "published_ts_ms": pub_ms
            "ingested_ts_ms": now_ms
            "source": "newsapi"
            "title": title
            "url": link
            "summary": "",  # как у вас в Go: не раздуваем
            "symbols": []
            "importance": 0.0
            "payload": a
            "provider_id": published_at,  # важно для UID (как у вас в Go)
        })

    return out
