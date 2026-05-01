from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, List, Dict, Any, Optional, Union

from common.enums import VetoReason


SignalKind = Literal["breakout", "absorption", "extreme", "obi_spike", "sweep", "reclaim", "custom"]


@dataclass
class Candidate:
    kind: str
    direction: int
    raw_score: float
    level_key: Optional[str] = None
    reasons: List[str] = field(default_factory=list)


@dataclass
class QualityState:
    veto: bool = False
    veto_reason: Union[VetoReason, str] = VetoReason.VETO_UNKNOWN
    quality_flags: Dict[str, Any] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)

    def add_flag(self, k: str, v: Any = True) -> None:
        self.quality_flags[k] = v

    def add_reason(self, r: str) -> None:
        if r:
            self.reasons.append(r)

    def veto_with(self, reason: Union[VetoReason, str]) -> None:
        self.veto = True
        self.veto_reason = VetoReason(reason) if reason in VetoReason.__members__.values() else reason or VetoReason.VETO_UNKNOWN
        self.add_reason(f"VETO:{self.veto_reason}")


@dataclass
class ScoredCandidate:
    cand: Candidate
    score: float
    conf_factor: float
    parts: Dict[str, Any]
    quality: QualityState

@dataclass(slots=True)
class SignalDTO:
    kind: Optional[str]
    side: Optional[int]
    symbol: Optional[str]
    ts: Optional[int]
    price: Optional[float]
    raw_score: float
    final_score: float
    confidence: float
    level_price: Optional[float]
    reasons: List[str]
    parts: Dict[str, Any]
    signal_id: str
    conf_factor: float
    decision_code: str
    decision_u16: int
    level_key: Optional[str]
    spread_bps: float
    taker_rate: float
    geometry_score: float
    labels: Optional[Dict[str, Any]] = None
    rc: Optional[int] = None
    rc16: Optional[str] = None
    reason_code: Optional[Union[VetoReason, str]] = None
    qf: Optional[List[int]] = None
    qf16: Optional[str] = None
    atr_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "side": self.side,
            "symbol": self.symbol,
            "ts": self.ts,
            "price": self.price,
            "raw_score": self.raw_score,
            "final_score": self.final_score,
            "confidence": self.confidence,
            "level_price": self.level_price,
            "reasons": self.reasons,
            "parts": self.parts,
            "signal_id": self.signal_id,
            "conf_factor": self.conf_factor,
            "decision_code": self.decision_code,
            "decision_u16": self.decision_u16,
            "level_key": self.level_key,
            "spread_bps": self.spread_bps,
            "taker_rate": self.taker_rate,
            "geometry_score": self.geometry_score,
            "labels": self.labels,
            "rc": self.rc,
            "rc16": self.rc16,
            "reason_code": self.reason_code,
            "qf": self.qf,
            "qf16": self.qf16,
            "atr_pct": self.atr_pct,
        }
