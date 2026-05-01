# -*- coding: utf-8 -*-
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from typing import Any, Dict, List

from news_pipeline.httpx_client import http_get_json
from news_pipeline.models import NewsRawItem


FMP_BASE = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com")
# по документации: /stable/news/crypto-latest, /stable/news/stock-latest (аналогично),
# и /stable/economic-calendar. (Если у вас другой endpoint — поправите base/path.)


def _ts_ms() -> int:
    return get_ny_time_millis()


def fetch_fmp(cfg: Dict[str, Any]) -> List[NewsRawItem]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return []

    out: List[NewsRawItem] = []
    now = _ts_ms()

    # --- crypto latest ---
    # page/limit: чтобы не убить лимиты
    crypto_enabled = True
    if isinstance(cfg.get("crypto"), dict):
        crypto_enabled = bool(cfg["crypto"].get("enabled", True))

    if crypto_enabled:
        url = f"{FMP_BASE}/stable/news/crypto-latest"
        js, err = http_get_json(url, params={"page": 0, "limit": 50, "apikey": api_key})
        if not err and isinstance(js, list):
            for it in js:
                if not isinstance(it, dict):
                    continue
                title = str(it.get("title") or "").strip()
                link = str(it.get("url") or "").strip()
                if not title or not link:
                    continue
                uid = str(it.get("id") or "") or f"fmpc:{hash(link)}"
                out.append(
                    NewsRawItem(
                        uid=f"fmp:crypto:{uid}",
                        ts_ms=now,
                        source="fmp",
                        title=title[:280],
                        url=link[:700],
                        summary=str(it.get("text") or it.get("site") or "")[:600],
                        symbols=[],  # можно заполнить через LLM tags/assets
                        importance=0.0,
                        payload=it,
                    )
                )

    # --- stock latest (equities/indices) ---
    stock_enabled = True
    if isinstance(cfg.get("stocks"), dict):
        stock_enabled = bool(cfg["stocks"].get("enabled", True))

    if stock_enabled:
        url = f"{FMP_BASE}/stable/news/stock-latest"
        js, err = http_get_json(url, params={"page": 0, "limit": 50, "apikey": api_key})
        if not err and isinstance(js, list):
            for it in js:
                if not isinstance(it, dict):
                    continue
                title = str(it.get("title") or "").strip()
                link = str(it.get("url") or "").strip()
                if not title or not link:
                    continue
                uid = str(it.get("id") or "") or f"fmps:{hash(link)}"

                # если API отдаёт symbol — хорошо, иначе оставим пусто
                sym = str(it.get("symbol") or "").strip().upper()
                symbols = [sym] if sym else []

                out.append(
                    NewsRawItem(
                        uid=f"fmp:stock:{uid}",
                        ts_ms=now,
                        source="fmp",
                        title=title[:280],
                        url=link[:700],
                        summary=str(it.get("text") or it.get("site") or "")[:600],
                        symbols=symbols,
                        importance=0.0,
                        payload=it,
                    )
                )

    return out
