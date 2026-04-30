# -*- coding: utf-8 -*-
"""
Единая конфигурация источников новостей/календаря.

Цели:
- один JSON в ENV: NEWS_SOURCES_JSON
- fail-open: если нет ключа провайдера → flags.<prov>=False
- дефолт "из коробки": RSS без ключей
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


DEFAULT_RSS_URLS = [
    # RSS URLs из вашего Excel файла (news_sources_tables.xlsx)
    "https://www.ecb.europa.eu/rss/press.html"
    "https://www.coindesk.com/arc/outboundfeeds/rss/"
    "https://cointelegraph.com/rss"
    "https://decrypt.co/feed"
    "https://bitcoinmagazine.com/.rss/full/"
    "https://thedefiant.io/rss.xml"
]


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default


@dataclass(frozen=True)
class ProviderFlags:
    cryptopanic: bool
    fmp: bool
    newsapi: bool
    rss: bool


@dataclass(frozen=True)
class SourcesConfig:
    # providers order controls ingestion priority (useful for rate limits)
    providers: List[str]
    raw: Dict[str, Any]
    flags: ProviderFlags

    @property
    def rss_urls(self) -> List[str]:
        rss = self.raw.get("rss", {}) if isinstance(self.raw, dict) else {}
        urls = rss.get("urls") if isinstance(rss, dict) else None
        if isinstance(urls, list) and urls:
            return [str(x) for x in urls]
        return list(DEFAULT_RSS_URLS)


def load_sources_config() -> SourcesConfig:
    """
    NEWS_SOURCES_JSON='{
      "providers": ["cryptopanic","fmp","newsapi","rss"]
      "cryptopanic": {...}
      "fmp": {...}
      "newsapi": {...}
      "rss": {...}
    }'

    Fail-open логика:
    - если провайдер включён, но ключ отсутствует → flags.<prov>=False
    """
    raw_json = _env("NEWS_SOURCES_JSON", "").strip()
    if not raw_json:
        raw: Dict[str, Any] = {
            "providers": ["rss"]
            "rss": {"enabled": True, "urls": list(DEFAULT_RSS_URLS)}
        }
    else:
        try:
            raw = json.loads(raw_json)
            if not isinstance(raw, dict):
                raise ValueError("NEWS_SOURCES_JSON must be an object")
        except Exception:
            # fail-open: если JSON битый — стартуем только с RSS
            raw = {
                "providers": ["rss"]
                "rss": {"enabled": True, "urls": list(DEFAULT_RSS_URLS)}
            }

    providers = raw.get("providers", ["rss"])
    if not isinstance(providers, list) or not providers:
        providers = ["rss"]
    providers = [str(p).lower() for p in providers]

    # ключи провайдеров
    have_cp = bool(_env("CRYPTOPANIC_AUTH_TOKEN"))
    have_fmp = bool(_env("FMP_API_KEY"))
    have_newsapi = bool(_env("NEWSAPI_KEY"))

    def _enabled(prov: str) -> bool:
        cfg = raw.get(prov, {})
        if not isinstance(cfg, dict):
            return False
        return bool(cfg.get("enabled", False))

    flags = ProviderFlags(
        cryptopanic=_enabled("cryptopanic") and have_cp
        fmp=_enabled("fmp") and have_fmp
        newsapi=_enabled("newsapi") and have_newsapi
        rss=_enabled("rss") if "rss" in raw else True,  # rss по умолчанию включён
    )

    # если rss выключили явно — оставим как есть (но лучше не выключать)
    return SourcesConfig(providers=providers, raw=raw, flags=flags)
