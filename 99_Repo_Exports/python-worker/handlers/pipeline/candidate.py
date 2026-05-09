from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Candidate:
    """
    Детектор обязан вернуть только это.
    Валидаторы/скорер дальше НЕ лезут в "детект" и не создают кандидатов.
    """
    kind: str                 # breakout/absorption/extreme/obi_spike/...
    side: int                 # +1 buy / -1 sell / 0 unknown
    raw_score: float          # "сила события" до quality/regime/liquidity
    level_price: float | None = None
    level_key: str | None = None
    reasons: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidatedCandidate:
    cand: Candidate
    veto: bool
    quality_flags: list[str]
    conf_factor01: float
    parts: dict[str, float]
    reason: str
