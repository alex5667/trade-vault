from __future__ import annotations

"""Structured evidence payload for signals.

This is an operational feature:
- Makes debugging and post-mortem deterministic.
- Lets Telegram show only a short summary while Redis keeps full evidence.

Design goals
------------
- Backward compatible: you can add `evidence` field without breaking old consumers.
- JSON-serializable: dataclasses expose `to_dict()`.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class StrongGateDecision:
    """Decision returned by strong gate evaluation."""
    ok: bool
    scenario: str
    need: int
    have: int
    a: int = 0
    b: int = 0
    c: int = 0
    reason: str = ""
    gate_bits: int = 0
    # Optional detailed legs for debug (A/B/C breakdown is often enough)
    # Kept for backward compatibility with signal_pipeline
    legs: Optional[Dict[str, int]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "ok": 1 if self.ok else 0,
            "scenario": str(self.scenario),
            "need": int(self.need),
            "have": int(self.have),
            "a": int(self.a),
            "b": int(self.b),
            "c": int(self.c),
            "reason": str(self.reason),
            "gate_bits": int(self.gate_bits),
            "legs": {str(k): int(v) for k, v in (self.legs or {}).items()} if self.legs else None,
        }


@dataclass
class SignalPayload:
    confirmations: Dict[str, Any]
    indicators: Dict[str, Any]
    gate: Optional[StrongGateDecision] = None
    confidence_parts: Optional[Dict[str, Any]] = None
    rejection_reason: Optional[str] = None
    
    # Optional extras to capture context
    ts_ms: int = 0
    symbol: str = ""
    signal_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "confirmations": dict(self.confirmations or {}),
            "indicators": dict(self.indicators or {}),
            "rejection_reason": self.rejection_reason,
            "ts_ms": self.ts_ms,
            "symbol": self.symbol,
            "signal_id": self.signal_id,
        }
        if self.gate is not None:
            d["gate"] = self.gate.to_dict()
        if self.confidence_parts is not None:
            d["confidence_parts"] = dict(self.confidence_parts)
        return d
