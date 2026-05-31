# scoring/scoring_engine.py

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SignalQualityLabel(Enum):
    A = "A"
    B = "B"
    C = "C"
    REJECT = "REJECT"  # технический label для «заваленных» сигналов


@dataclass
class QualityResult:
    confidence: float                 # [0..1], возможно скорректированная
    label: SignalQualityLabel
    reasons: list[str]
    force_reject: bool = False        # если True, то сигнал запрещён независимо от score


@dataclass
class ScoringResult:
    score: float                    # базовый score (raw)
    final_score: float              # после pattern_weight и golden
    confidence: float               # [0..1]
    quality_label: SignalQualityLabel | None
    reasons: list[str] = field(default_factory=list)
    should_emit: bool = False
    debug: dict[str, Any] = field(default_factory=dict)
