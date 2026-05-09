from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

SignalKind = Literal["breakout", "absorption", "extreme", "obi_spike", "sweep", "reclaim", "custom"]


@dataclass
class Candidate:
    kind: str
    direction: int
    raw_score: float
    level_key: str | None = None
    reasons: list[str] = field(default_factory=list)


@dataclass
class QualityState:
    veto: bool = False
    veto_reason: str = ""
    quality_flags: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def add_flag(self, k: str, v: Any = True) -> None:
        self.quality_flags[k] = v

    def add_reason(self, r: str) -> None:
        if r:
            self.reasons.append(r)

    def veto_with(self, reason: str) -> None:
        self.veto = True
        self.veto_reason = reason or "veto"
        self.add_reason(f"VETO:{self.veto_reason}")


@dataclass
class ScoredCandidate:
    cand: Candidate
    score: float
    conf_factor: float
    parts: dict[str, Any]
    quality: QualityState
