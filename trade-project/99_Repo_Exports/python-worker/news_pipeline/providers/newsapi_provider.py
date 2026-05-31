from __future__ import annotations

import os
from typing import Any

from news_pipeline.httpx_client import http_get_json
from news_pipeline.models import NewsRawItem
from utils.time_utils import get_ny_time_millis

NEWSAPI_BASE = os.getenv("NEWSAPI_BASE_URL", "https://newsapi.org/v2/everything")


def _ts_ms() -> int:
    return get_ny_time_millis()


def fetch_newsapi(cfg: dict[str, Any]) -> list[NewsRawItem]:
    api_key = os.getenv("NEWSAPI_KEY", "").strip()
    if not api_key:
        return []

    q = (cfg.get("q") or "").strip()
    if not q:
        return []

    params: dict[str, Any] = {
        "q": q,
        "language": cfg.get("language") or "en",
        "sortBy": cfg.get("sortBy") or "publishedAt",
        "pageSize": int(cfg.get("pageSize") or 50),
        "page": int(cfg.get("page") or 1),
        "apiKey": api_key,
    }

    js, err = http_get_json(NEWSAPI_BASE, params=params)
    if err or not isinstance(js, dict):
        return []

    arts = js.get("articles")
    if not isinstance(arts, list):
        return []

    out: list[NewsRawItem] = []
    now = _ts_ms()

    for it in arts:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        if not title or not url:
            continue

        src = it.get("source") if isinstance(it.get("source"), dict) else {}
        src_name = (src.get("name") or "newsapi").strip().lower()

        uid = f"na:{hash(url)}"
        out.append(
            NewsRawItem(
                uid=uid,
                ts_ms=now,
                source=f"newsapi:{src_name}"[:48],
                title=title[:280],
                url=url[:700],
                summary=(it.get("description") or "")[:600],
                symbols=[],  # tags/assets определит LLM
                importance=0.0,
                payload=it,
            )
        )

    return out
