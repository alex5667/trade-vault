from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Candidate:
    """
    Detector output (no quality checks here).
    side: +1 bullish, -1 bearish
    raw_score: detector score (signed)
    """
    kind: Any  # SignalKind (kept Any to avoid tight coupling)
    side: int
    raw_score: float
    level_key: str | None = None
    reasons: list[str] = field(default_factory=list)

    # populated by validators/scoring
    quality_flags: dict[str, Any] = field(default_factory=dict)
    veto: bool = False
    veto_reason: str = ""


@dataclass(frozen=True)
class ScoredCandidate:
    candidate: Candidate
    conf_factor: float          # [0..1]
    final_score: float          # raw_score * conf_factor
    confidence_pct: float       # [0..100] calibrated display metric
    score_parts: dict[str, Any] # breakdown for debug/audit
