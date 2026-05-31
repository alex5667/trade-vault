"""news_pipeline.prompt_v2

Prompt v2.1.0 for the Playwright enrichment pipeline.
Richer schema: event_type / event_class / grade_id / affected_symbols /
directional_bias / recommended_action / reason_code / time_window_sec.

Used ONLY when NEWS_LLM_PLAYWRIGHT_ENABLE=1.
The v1 prompt (in playwright_llm_client.py) is used when playwright is disabled.
"""
from __future__ import annotations

import time
from typing import Any

PROMPT_VERSION = "v2.1.0"

_SUPPORTED_SYMBOLS = (
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "MATICUSDT",
    "XAUUSD", "EURUSDT", "GBPUSDT", "JPYUSDT",
)

_TEMPLATE = """\
You are a financial-news extraction engine for an automated crypto/futures risk gate.

You do NOT open trades.
You do NOT give price targets.
You ONLY extract structured risk context.

Return ONLY a compact JSON object.
No prose. No markdown fences. No chain-of-thought outside <think>.

=== ALLOWED VALUES ===

event_type (pick ONE):
  macro_cpi | macro_ppi | macro_fomc | macro_fed_speech | macro_nfp |
  macro_rates | macro_inflation | crypto_regulation | crypto_etf |
  exchange_outage | exchange_listing | exchange_delisting | security_hack |
  geopolitics | liquidation | earnings | market_commentary | noise | unknown

event_class (pick ONE):
  macro | crypto | exchange | security | geopolitics | earnings | market | noise | unknown

sentiment (pick ONE):
  risk_on | risk_off | neutral | mixed | unknown

recommended_action (pick ONE):
  allow | tighten | block | protective_only

=== CLASSIFICATION RULES ===
- Vague or low-impact news → event_type="noise" or "market_commentary".
- High uncertainty → recommended_action="tighten", NOT "block".
- Do not invent symbols or facts not present in the input.
- CPI / FOMC / NFP / Fed rate events → grade_id 4 or 5.
- Exchange outage or security hack → grade_id 4-5, affected_symbols = directly related exchange coins.
- confidence = your confidence in classification (0..1), not probability of profit.
- risk_score = expected price-movement magnitude (0=none, 1=extreme).
- surprise_score = how unexpected the event is (-1=very expected, +1=completely surprising).
- risk_factor_bps = position-size multiplier in BPS (10000=no reduction, 0=full block).
- time_window_sec = how long this event affects trading (0 if unknown/noise).
- reason_code = snake_case identifier, max 40 chars.
- affected_symbols = only from: {symbols}
- evidence = list of short string tags from the title/summary that justify the classification.
- dq_flags = leave empty (filled by validator).
- summary = max 240 chars, factual, no opinion.

=== OUTPUT SCHEMA ===
{{
  "schema_ver": "news_llm_analysis_v2",
  "event_type": "...",
  "event_class": "...",
  "grade_id": 0,
  "risk_score": 0.0,
  "surprise_score": 0.0,
  "confidence": 0.0,
  "sentiment": "...",
  "affected_symbols": [],
  "directional_bias": {{}},
  "recommended_action": "...",
  "risk_factor_bps": 10000,
  "reason_code": "snake_case_max_40",
  "time_window_sec": 0,
  "evidence": [],
  "dq_flags": [],
  "summary": "max 240 chars"
}}

=== INPUT ===
source: {source}
title: {title}
url: {url}
summary: {summary}
published_ts_ms: {published_ts_ms}
ingested_ts_ms: {ingested_ts_ms}
now_ts_ms: {now_ts_ms}
"""


def build_prompt_v2(
    *,
    title: str,
    url: str,
    source: str,
    summary: str = "",
    published_ts_ms: int = 0,
    ingested_ts_ms: int = 0,
    symbols: tuple[str, ...] = _SUPPORTED_SYMBOLS,
) -> str:
    now_ms = int(time.time() * 1000)
    return _TEMPLATE.format(
        symbols=", ".join(symbols),
        source=source or "unknown",
        title=title,
        url=url,
        summary=summary or "",
        published_ts_ms=published_ts_ms or now_ms,
        ingested_ts_ms=ingested_ts_ms or now_ms,
        now_ts_ms=now_ms,
    )
