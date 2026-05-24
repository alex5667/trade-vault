"""Phase D.1 (P3): Entry profile classification.

Концепция (новый слой поверх существующего `kind` = microstructure pattern):

  EntryProfile  — это trading setup style, описывающий контекст входа,
  валидные режимы, и каноничный execution / exit profile.

Поддерживаемые профили:

  REVERSAL_RANGE_SCALP     — fast scalp в calm/range; SL близко, TP1=0.3R, BE only.
  TREND_CONTINUATION       — следование тренда; ATR-based SL, rocket_v1 trail.
  EXPANSION_BREAKOUT       — breakout после squeeze/range; trail после TP2.
  NEWS_SHOCK_PROTECTIVE    — defensive; tight SL, минимальная экспозиция.
  NO_TRADE_ADVERSE         — гейт vetoes (например, BTC drop block, HTF bias).

Архитектура:

  - Pure function `classify_entry_profile(ctx)` → `EntryProfileResult`.
  - Не пишет в Redis / Prometheus. Только returns labelled result.
  - `signal_pipeline._evaluate_entry_profile()` дальше:
      - shadow phase: пишет `entry_profile_shadow` в indicators;
      - enforce phase: применяет profile к TP/SL/trail (через regime_exec).

Контекст входа:

  ctx = {
      "kind": "breakout" | ... ,        # current microstructure label
      "vol_regime": "shock" | "calm" | ...,
      "trend_regime": "trending" | "range" | "squeeze" | ...,
      "side": "LONG" | "SHORT",
      "og_score": float,                # gate score (out of all OG gates)
      "smt_coh": float,                 # confirmed SMT coherence
      "news_shock": bool,               # есть ли активный news shock flag
      "adverse_cross": bool,            # exec/DQ adverse signal
  }
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ──────────────────────────────── enums ─────────────────────────────────────────
class EntryProfile:
    REVERSAL_RANGE_SCALP = "REVERSAL_RANGE_SCALP"
    TREND_CONTINUATION   = "TREND_CONTINUATION"
    EXPANSION_BREAKOUT   = "EXPANSION_BREAKOUT"
    NEWS_SHOCK_PROTECTIVE = "NEWS_SHOCK_PROTECTIVE"
    NO_TRADE_ADVERSE     = "NO_TRADE_ADVERSE"
    UNKNOWN              = "UNKNOWN"


@dataclass
class EntryProfileResult:
    profile: str
    confidence: float           # 0..1; для отчётов и future-ML use
    reasons: list[str]


# ───────────────────────── thresholds (tunable via ENV) ─────────────────────────
_DEFAULT_OG_SCORE_MIN = 0.5     # OG-консенсус считаем "ok" с >=0.5
_DEFAULT_SMT_COH_MIN  = 0.4     # SMT уверенность


# ──────────────────────────────── core ──────────────────────────────────────────
def classify_entry_profile(ctx: dict[str, Any]) -> EntryProfileResult:
    """Pure classifier. Стабилен по input; без I/O.

    Порядок проверок (highest priority first):
      1) adverse / news shock → NO_TRADE_ADVERSE
      2) news_shock без adverse → NEWS_SHOCK_PROTECTIVE
      3) trend bucket + SMT coherence → TREND_CONTINUATION
      4) squeeze→expansion → EXPANSION_BREAKOUT
      5) calm/range + reclaim/absorption → REVERSAL_RANGE_SCALP
      6) UNKNOWN (fallback)
    """
    kind = (ctx.get("kind") or "").lower()
    vol = (ctx.get("vol_regime") or "").lower()
    trend = (ctx.get("trend_regime") or "").lower()
    og_score = _float(ctx.get("og_score"), 0.0)
    smt_coh = _float(ctx.get("smt_coh"), 0.0)
    news_shock = bool(ctx.get("news_shock"))
    adverse = bool(ctx.get("adverse_cross"))

    reasons: list[str] = []

    # 1) Veto — high-priority.
    if adverse:
        return EntryProfileResult(
            EntryProfile.NO_TRADE_ADVERSE, 1.0,
            ["adverse_cross=true"],
        )

    # 2) News shock без adverse: торгуем, но в защитном профиле.
    if news_shock:
        return EntryProfileResult(
            EntryProfile.NEWS_SHOCK_PROTECTIVE, 0.9,
            ["news_shock=true"],
        )

    # 3) Trend continuation: trend regime + SMT coh.
    if trend in {"trending", "trending_bear"} and smt_coh >= _DEFAULT_SMT_COH_MIN:
        conf = min(1.0, 0.5 + smt_coh * 0.5)
        reasons.append(f"trend={trend}")
        reasons.append(f"smt_coh={smt_coh:.2f}>={_DEFAULT_SMT_COH_MIN}")
        if og_score >= _DEFAULT_OG_SCORE_MIN:
            reasons.append(f"og_ok={og_score:.2f}")
            conf = min(1.0, conf + 0.1)
        return EntryProfileResult(EntryProfile.TREND_CONTINUATION, conf, reasons)

    # 4) Expansion breakout: trend=expansion ИЛИ переход из squeeze.
    if trend in {"expansion"} or (trend == "trending" and vol == "shock"):
        reasons.append(f"trend={trend}")
        if og_score >= _DEFAULT_OG_SCORE_MIN:
            reasons.append(f"og_ok={og_score:.2f}")
        return EntryProfileResult(EntryProfile.EXPANSION_BREAKOUT, 0.7, reasons)

    # 5) Range scalp: calm + range + reclaim/absorption/sweep.
    if vol in {"calm"} and trend in {"range"}:
        if kind in {"reclaim", "absorption", "sweep", "obi_spike"}:
            reasons.append(f"calm+range+kind={kind}")
            return EntryProfileResult(EntryProfile.REVERSAL_RANGE_SCALP, 0.75, reasons)

    # 6) Fallback.
    reasons.append(f"no_match: vol={vol} trend={trend} kind={kind}")
    return EntryProfileResult(EntryProfile.UNKNOWN, 0.3, reasons)


def _float(v: Any, default: float) -> float:
    if v is None:
        return default
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return default
    return default
