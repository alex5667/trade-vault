# -*- coding: utf-8 -*-
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from typing import Any, Dict, List, Optional

from news_pipeline.httpx_client import http_get_json
from news_pipeline.models import NewsRawItem


CRYPTOPANIC_BASE_URL = os.getenv("CRYPTOPANIC_BASE_URL", "https://cryptopanic.com/api/v1/posts/")


def _ts_ms() -> int:
    return get_ny_time_millis()


def fetch_cryptopanic(cfg: Dict[str, Any]) -> List[NewsRawItem]:
    """
    CryptoPanic → NewsRawItem list.
    Fail-open: вернёт пусто при любой ошибке.

    ВАЖНО: формат ответа/параметры могут отличаться по плану CryptoPanic.
    Мы делаем максимально "мягкий" парсер и не падаем.
    """
    token = os.getenv("CRYPTOPANIC_AUTH_TOKEN", "").strip()
    if not token:
        return []

    # типовые параметры (если API изменится — вы поправите тут)
    params: Dict[str, Any] = {
        "auth_token": token
    }

    # user config
    # пример: { "currencies":["BTC","ETH"], "filter":"important", "kind":"news", "region":"en" }
    currencies = cfg.get("currencies")
    if isinstance(currencies, list) and currencies:
        params["currencies"] = ",".join([str(x).upper() for x in currencies])
    if cfg.get("filter"):
        params["filter"] = str(cfg["filter"])
    if cfg.get("kind"):
        params["kind"] = str(cfg["kind"])
    if cfg.get("region"):
        params["region"] = str(cfg["region"])

    js, err = http_get_json(CRYPTOPANIC_BASE_URL, params=params)
    if err or not isinstance(js, dict):
        return []

    results = js.get("results")
    if not isinstance(results, list):
        return []

    out: List[NewsRawItem] = []
    now = _ts_ms()

    for it in results:
        if not isinstance(it, dict):
            continue
        title = str(it.get("title") or "").strip()
        url = str(it.get("url") or it.get("source", {}).get("url") or "").strip()
        if not title or not url:
            continue

        # currencies field can be list[{"code":"BTC"}]
        syms: List[str] = []
        cur = it.get("currencies")
        if isinstance(cur, list):
            for c in cur:
                if isinstance(c, dict) and c.get("code"):
                    syms.append(str(c["code"]).upper())
                elif isinstance(c, str):
                    syms.append(c.upper())

        # published_at optional
        ts_ms = now
        published = it.get("published_at") or it.get("created_at")
        # если не можем распарсить — оставим now
        # (LLM/feature-store всё равно работает по относительному времени)
        uid = str(it.get("id") or "") or f"cp:{hash(url)}"

        out.append(
            NewsRawItem(
                uid=f"cp:{uid}"
                ts_ms=int(ts_ms)
                source="cryptopanic"
                title=title[:280]
                url=url[:700]
                summary=str(it.get("domain") or it.get("source", {}).get("title") or "")[:600]
                symbols=syms
                importance=0.0
                payload=it,  # тяжёлое — дальше уйдёт в analysis:<uid>
            )
        )

    return out
