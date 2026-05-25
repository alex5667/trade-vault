"""news_pipeline.classifier

Deterministic rule-based news classifier.

Design constraints:
- NO LLM in hot path — rules must be regex/keyword only.
- Every output must have an event_type and reason_code.
- grade_id 0..5: 0=ignore, 1=low, 2=medium, 3=high, 4=extreme, 5=critical.
- LLM enrichment (confidence/surprise/sentiment) is OPTIONAL and added async.
- Replay-safe: same input → same output, no wall-clock, no randomness.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# ─── Event type catalogue ────────────────────────────────────────────────────

@dataclass(frozen=True)
class EventRule:
    event_type: str
    grade_id: int          # 0..5
    reason_code: str
    symbols: tuple[str, ...]
    asset_classes: tuple[str, ...]
    default_action: str    # allow / tighten / block / protective_only
    sentiment: str         # risk_off / risk_on / mixed / neutral
    # pre_sec/post_sec: block window around event_ts_ms
    pre_sec: int = 600     # 10 min before
    post_sec: int = 900    # 15 min after


_CRYPTO_MAJORS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT")
_CRYPTO_LIQUID = _CRYPTO_MAJORS + ("DOGEUSDT", "AVAXUSDT", "LINKUSDT", "MATICUSDT")
_ALL_CRYPTO    = ("*crypto",)  # sentinel: all active crypto symbols
_MACRO_ASSETS  = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XAUUSD") + tuple(
    f"{p}USDT" for p in ("EUR", "GBP", "JPY", "CHF", "AUD", "NZD")
)

RULES: list[tuple[re.Pattern[str], EventRule]] = [
    (
        re.compile(
            r"\b(CPI|consumer price index|core inflation|pce deflator"
            r"|PPI|producer price index)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="macro_cpi",
            grade_id=5,
            reason_code="macro_high_impact_cpi",
            symbols=_MACRO_ASSETS,
            asset_classes=("crypto", "fx", "gold"),
            default_action="block",
            sentiment="risk_off",
            pre_sec=600,
            post_sec=1800,
        ),
    ),
    (
        re.compile(
            r"\b(FOMC|federal open market|fed rate|interest rate decision"
            r"|rate hike|rate cut|fed funds|federal reserve meeting)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="macro_fomc",
            grade_id=5,
            reason_code="macro_high_impact_fomc",
            symbols=_MACRO_ASSETS,
            asset_classes=("crypto", "fx", "gold"),
            default_action="block",
            sentiment="risk_off",
            pre_sec=600,
            post_sec=1800,
        ),
    ),
    (
        re.compile(
            r"\b(NFP|nonfarm payrolls?|non.?farm payrolls?"
            r"|unemployment rate|jobless claims|ADP employment)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="macro_jobs",
            grade_id=5,
            reason_code="macro_high_impact_jobs",
            symbols=_MACRO_ASSETS,
            asset_classes=("crypto", "fx", "gold"),
            default_action="block",
            sentiment="risk_off",
            pre_sec=600,
            post_sec=1200,
        ),
    ),
    (
        re.compile(
            r"\b(fed speech|fed chair|powell|yellen|waller|bowman|barr"
            r"|fed governor|FOMC minutes|FOMC statement)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="macro_fed_speech",
            grade_id=4,
            reason_code="macro_fed_speech",
            symbols=_MACRO_ASSETS,
            asset_classes=("crypto", "fx", "gold"),
            default_action="tighten",
            sentiment="risk_off",
            pre_sec=300,
            post_sec=900,
        ),
    ),
    (
        re.compile(
            r"\b(GDP|gross domestic product|retail sales|ISM|PMI"
            r"|durable goods|trade balance|current account)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="macro_data",
            grade_id=3,
            reason_code="macro_data_release",
            symbols=_MACRO_ASSETS,
            asset_classes=("crypto", "fx", "gold"),
            default_action="tighten",
            sentiment="mixed",
            pre_sec=300,
            post_sec=600,
        ),
    ),
    # ── crypto regulation ───────────────────────────────────────────────────
    (
        re.compile(
            r"\b(SEC|CFTC|FinCEN|MiCA|FCA|FSOC|OCC).{0,80}"
            r"(bitcoin|crypto|ethereum|BTC|ETH|exchange|token|stablecoin)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="crypto_regulation",
            grade_id=4,
            reason_code="crypto_regulation",
            symbols=_CRYPTO_MAJORS,
            asset_classes=("crypto",),
            default_action="tighten",
            sentiment="risk_off",
            pre_sec=0,
            post_sec=1800,
        ),
    ),
    (
        re.compile(
            r"\b(ETF approval|ETF rejected|ETF denied|spot bitcoin ETF"
            r"|spot ETH ETF|bitcoin ETF approved|crypto ETF)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="crypto_etf",
            grade_id=5,
            reason_code="crypto_etf_decision",
            symbols=("BTCUSDT", "ETHUSDT"),
            asset_classes=("crypto",),
            default_action="protective_only",
            sentiment="risk_on",
            pre_sec=0,
            post_sec=3600,
        ),
    ),
    # ── crypto security / exchange ──────────────────────────────────────────
    (
        re.compile(
            r"\b(hack|exploit|stolen funds|security breach|drained"
            r"|million stolen|funds at risk|rekt)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="crypto_security",
            grade_id=5,
            reason_code="crypto_security_incident",
            symbols=_CRYPTO_LIQUID,
            asset_classes=("crypto",),
            default_action="block",
            sentiment="risk_off",
            pre_sec=0,
            post_sec=3600,
        ),
    ),
    (
        re.compile(
            r"\b(Binance|Coinbase|Kraken|OKX|Bybit|BitMEX|Huobi|Gate\.io)"
            r".{0,60}(outage|maintenance|API down|trading halt|suspended|issue)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="exchange_status",
            grade_id=5,
            reason_code="exchange_status_degraded",
            symbols=_ALL_CRYPTO,
            asset_classes=("crypto",),
            default_action="block",
            sentiment="risk_off",
            pre_sec=0,
            post_sec=3600,
        ),
    ),
    (
        re.compile(
            r"\b(lawsuit|indictment|charged|arrested).{0,60}"
            r"(bitcoin|crypto|exchange|binance|coinbase|ceo|founder)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="crypto_legal",
            grade_id=4,
            reason_code="crypto_legal_action",
            symbols=_CRYPTO_MAJORS,
            asset_classes=("crypto",),
            default_action="tighten",
            sentiment="risk_off",
            pre_sec=0,
            post_sec=1800,
        ),
    ),
    # ── listing / delisting ─────────────────────────────────────────────────
    (
        re.compile(
            r"\b(delist|delisting|token removal|removed from|will be removed)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="exchange_delisting",
            grade_id=3,
            reason_code="exchange_delisting",
            symbols=(),   # populated dynamically from title context
            asset_classes=("crypto",),
            default_action="tighten",
            sentiment="risk_off",
            pre_sec=0,
            post_sec=1800,
        ),
    ),
    (
        re.compile(
            r"\b(new listing|will list|trading begins|trading opens|launch.{0,30}trading)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="exchange_listing",
            grade_id=2,
            reason_code="exchange_listing",
            symbols=(),
            asset_classes=("crypto",),
            default_action="tighten",
            sentiment="risk_on",
            pre_sec=0,
            post_sec=900,
        ),
    ),
    # ── geopolitics / war ───────────────────────────────────────────────────
    (
        re.compile(
            r"\b(war|invasion|attack|missile|airstrike|sanctions|nuclear"
            r"|conflict escalat|troops|military operation)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="geopolitics",
            grade_id=4,
            reason_code="geopolitical_event",
            symbols=_MACRO_ASSETS,
            asset_classes=("crypto", "fx", "gold"),
            default_action="tighten",
            sentiment="risk_off",
            pre_sec=0,
            post_sec=3600,
        ),
    ),
    # ── large liquidation event ─────────────────────────────────────────────
    (
        re.compile(
            r"\b(mass liquidation|liquidation cascade|cascade|short squeeze"
            r"|long squeeze|\$[0-9]+[MB].{0,20}liquidated)\b",
            re.IGNORECASE,
        ),
        EventRule(
            event_type="market_liquidation",
            grade_id=3,
            reason_code="market_liquidation_event",
            symbols=_CRYPTO_MAJORS,
            asset_classes=("crypto",),
            default_action="tighten",
            sentiment="mixed",
            pre_sec=0,
            post_sec=600,
        ),
    ),
]

# Fallback rule for unmatched items
_RULE_UNKNOWN = EventRule(
    event_type="unknown",
    grade_id=0,
    reason_code="no_match",
    symbols=(),
    asset_classes=(),
    default_action="allow",
    sentiment="neutral",
    pre_sec=0,
    post_sec=0,
)


# ─── Classifier ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ClassifyResult:
    event_type: str
    grade_id: int
    reason_code: str
    symbols: tuple[str, ...]
    asset_classes: tuple[str, ...]
    default_action: str
    sentiment: str
    pre_sec: int
    post_sec: int
    matched: bool


def classify(title: str, summary: str = "", source: str = "") -> ClassifyResult:
    """Deterministic classification based on title + optional summary.

    Always returns a result; grade_id=0/event_type='unknown' means no match.
    """
    text = f"{title} {summary}"
    best: EventRule | None = None
    best_grade = -1

    for pattern, rule in RULES:
        if pattern.search(text):
            if rule.grade_id > best_grade:
                best_grade = rule.grade_id
                best = rule

    if best is None:
        return ClassifyResult(
            event_type=_RULE_UNKNOWN.event_type,
            grade_id=_RULE_UNKNOWN.grade_id,
            reason_code=_RULE_UNKNOWN.reason_code,
            symbols=_RULE_UNKNOWN.symbols,
            asset_classes=_RULE_UNKNOWN.asset_classes,
            default_action=_RULE_UNKNOWN.default_action,
            sentiment=_RULE_UNKNOWN.sentiment,
            pre_sec=_RULE_UNKNOWN.pre_sec,
            post_sec=_RULE_UNKNOWN.post_sec,
            matched=False,
        )

    return ClassifyResult(
        event_type=best.event_type,
        grade_id=best.grade_id,
        reason_code=best.reason_code,
        symbols=best.symbols,
        asset_classes=best.asset_classes,
        default_action=best.default_action,
        sentiment=best.sentiment,
        pre_sec=best.pre_sec,
        post_sec=best.post_sec,
        matched=True,
    )


def action_for_grade(grade_id: int) -> str:
    """Conservative default action based solely on grade."""
    if grade_id >= 5:
        return "block"
    if grade_id >= 4:
        return "tighten"
    if grade_id >= 3:
        return "tighten"
    if grade_id >= 2:
        return "tighten"
    return "allow"


def risk_factor_bps_for_action(action: str, grade_id: int) -> int:
    """Risk factor in bps (10000 = no reduction, 0 = full disable)."""
    if action == "block":
        return 0
    if action == "protective_only":
        return 0
    if action == "tighten":
        if grade_id >= 5:
            return 1500
        if grade_id >= 4:
            return 2500
        if grade_id >= 3:
            return 4000
        return 6000
    return 10000
