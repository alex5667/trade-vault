from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class RegimeLabel(StrEnum):
    UNKNOWN = "unknown"
    MIXED = "mixed"
    RANGE = "range"
    TREND = "trend"
    TRENDING = "trending"  # legacy alias for TREND
    TRENDING_BULL = "trending_bull"
    TRENDING_BEAR = "trending_bear"
    # expansion: high-ATR trend phase — separate ML feature (ID 3.1/3.2)
    EXPANSION_BULL = "expansion_bull"
    EXPANSION_BEAR = "expansion_bear"
    # squeeze / range sub-labels used by regime_service._decide_regime
    SQUEEZE = "squeeze"
    SQUEEZE_BULLISH = "squeeze_bullish"
    SQUEEZE_BEARISH = "squeeze_bearish"
    RANGE_BULLISH = "range_bullish"
    RANGE_BEARISH = "range_bearish"
    NA = "na"


@dataclass(frozen=True)
class RegimeSnapshot:
    symbol: str
    label: RegimeLabel
    direction: int              # -1 bear, 0 neutral, +1 bull
    score: float                # [-1..+1]
    confidence: float           # [0..1]
    features: dict[str, float] = field(default_factory=dict)
    ts_event_ms: int = 0        # биржевое/данное время
    ts_calc_ms: int = 0         # время расчёта
    source: str = "market_regime_service"
    schema_ver: int = 1

    def age_ms(self, now_ms: int) -> int:
        return max(0, now_ms - (self.ts_calc_ms or self.ts_event_ms or 0))

    def is_stale(self, now_ms: int, max_age_ms: int) -> bool:
        return self.age_ms(now_ms) > max_age_ms


@dataclass(frozen=True)
class RegimeSwitchPolicy:
    enter_trend_score: float = 0.40
    enter_range_score: float = -0.40
    exit_band_score: float = 0.15
    confirm_bars: int = 3
    min_hold_ms: int = 180_000
    fast_override_score: float = 0.65
    max_stale_ms: int = 10_000


# Labels considered "in trend" for exit_band_score guard.
_TREND_LABELS: frozenset[str] = frozenset({
    "trending_bull", "trending_bear", "trend", "trending",
    "expansion_bull", "expansion_bear",
})

# Labels considered "range-like" for exit_band guard.
_RANGE_LABELS: frozenset[str] = frozenset({
    "range", "range_bullish", "range_bearish",
    "squeeze", "squeeze_bullish", "squeeze_bearish",
})


def should_switch(
    *,
    prev_label: str,
    next_label: str,
    score: float,
    confirm_count: int,
    now_ms: int,
    last_switch_ms: int,
    policy: RegimeSwitchPolicy,
) -> tuple[bool, str]:
    """Hysteresis gate: decide whether to switch from prev_label to next_label.

    Returns (do_switch, reason).
    """
    if next_label == prev_label:
        return False, "same_regime"

    held_ms = now_ms - last_switch_ms

    # Fast override: very strong signal skips all debounce.
    if abs(score) >= policy.fast_override_score:
        return True, "fast_override"

    # exit_band_score guard (P1): trending regime stays put if score
    # hasn't crossed the exit band towards range/mixed.
    if prev_label in _TREND_LABELS and next_label in ("mixed", "range", "unknown"):
        if abs(score) > policy.exit_band_score:
            return False, "hysteresis_exit_band"

    if prev_label == "range" and next_label in ("mixed", "unknown"):
        if abs(score) > policy.exit_band_score:
            return False, "hysteresis_exit_band"

    if held_ms < policy.min_hold_ms:
        return False, "min_hold"

    if confirm_count < policy.confirm_bars:
        return False, "need_confirm"

    return True, "confirmed_switch"
