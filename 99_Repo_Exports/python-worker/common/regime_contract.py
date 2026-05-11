from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class RegimeLabel(StrEnum):
    UNKNOWN = "unknown"
    MIXED = "mixed"
    RANGE = "range"
    TREND = "trend"
    TRENDING_BULL = "trending_bull"
    TRENDING_BEAR = "trending_bear"


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
        return max(0, int(now_ms) - int(self.ts_calc_ms or self.ts_event_ms or 0))

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
    if next_label == prev_label:
        return False, "same_regime"

    held_ms = now_ms - last_switch_ms

    if abs(score) >= policy.fast_override_score:
        return True, "fast_override"

    if held_ms < policy.min_hold_ms:
        return False, "min_hold"

    if confirm_count < policy.confirm_bars:
        return False, "need_confirm"

    return True, "confirmed_switch"
