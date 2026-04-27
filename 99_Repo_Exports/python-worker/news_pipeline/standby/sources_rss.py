# python-worker/news_pipeline/standby/sources_rss.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import time
import feedparser
from typing import Iterable, Dict, Any, List

def fetch_rss(*, name: str, urls: Iterable[str], user_agent: str = "trade-standby/1.0") -> List[Dict[str, Any]]:
    """
    Возвращает "сырые" новости без uid — uid будет назначен выше по pipeline.
    """
    out: List[Dict[str, Any]] = []
    now_ms = get_ny_time_millis()

    # feedparser сам ходит в сеть; UA задаётся через request_headers
    headers = {"User-Agent": user_agent}

    for u in urls:
        u = (u or "").strip()
        if not u:
            continue
        d = feedparser.parse(u, request_headers=headers)

        for e in d.entries or []:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            if not title or not link:
                continue

            # published/updated → fallback now
            published_ms = now_ms
            if getattr(e, "published_parsed", None):
                published_ms = int(time.mktime(e.published_parsed) * 1000)
            elif getattr(e, "updated_parsed", None):
                published_ms = int(time.mktime(e.updated_parsed) * 1000)

            summary = (getattr(e, "summary", "") or "").strip()
            payload = {
                "feed_url": u,
                "feed_title": getattr(d.feed, "title", "") if getattr(d, "feed", None) else "",
            }

            out.append({
                "published_ts_ms": published_ms if published_ms > 0 else now_ms,
                "ingested_ts_ms": now_ms,
                "source": name,
                "title": title,
                "url": link,
                "summary": summary[:2000],
                "symbols": [],          # на ingest не угадываем
                "importance": 0.0,
                "payload": payload,
            })

    return out
