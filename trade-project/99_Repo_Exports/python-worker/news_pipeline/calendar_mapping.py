# news_pipeline/calendar_mapping.py
from __future__ import annotations

_FIAT_CCY = {
    "USD","EUR","GBP","JPY","CHF","AUD","CAD","NZD","CNY",
    "SEK","NOK","DKK","PLN","CZK","HUF","TRY","MXN","BRL","ZAR","INR","KRW","SGD","HKD",
}

def map_calendar_asset_classes(*, country: str, currency: str, title: str, importance: int) -> list[str]:
    """
    Стратегия:
      - baseline: forex
      - USD/Fed/FOMC: +metals
      - USD high-impact: +crypto
      - EUR medium/high (ECB): +crypto
    """
    ccy = (currency or "").strip().upper()
    ctry = (country or "").strip().upper()
    t = (title or "").strip().lower()
    imp = int(importance or 0)

    out = set()

    # baseline: всё фиатное макро -> forex
    if ccy in _FIAT_CCY or ccy == "":
        out.add("forex")
    else:
        out.add("forex")

    is_us = (ccy == "USD") or (ctry in ("US", "USA", "UNITED STATES")) or ("fomc" in t) or ("fed" in t)
    is_eu = (ccy == "EUR") or ("ecb" in t) or (ctry in ("EU", "EUROZONE"))

    if is_us:
        out.add("metals")
        if imp >= 2:
            out.add("crypto")

    if is_eu and imp >= 2:
        out.add("crypto")

    # стабильный порядок
    order = ["forex", "metals", "crypto"]
    return [k for k in order if k in out]
