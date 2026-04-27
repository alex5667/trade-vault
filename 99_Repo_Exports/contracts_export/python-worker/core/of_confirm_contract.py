from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict


# Gate bits (stable, for UI/analytics)
BIT_A = 1 << 0  # A: delta_z + weak_progress OR abs_lvl (per cfg)
BIT_B = 1 << 1  # B: sweep + reclaim
BIT_C = 1 << 2  # C: obi_stable or iceberg_strict OR abs_lvl (per cfg)
BIT_D = 1 << 3  # D: abs_lvl_ok (explicit) (optional)


def pack_bits(a: bool, b: bool, c: bool, d: bool = False) -> int:
    x = 0
    if a: x |= BIT_A
    if b: x |= BIT_B
    if c: x |= BIT_C
    if d: x |= BIT_D
    return x


@dataclass
class OFConfirmV3:
    """
    Stable contract to embed into raw signals and publish into signals:of:confirm.
    Versioned and introspectable: every decision is explainable.
    """
    v: int
    symbol: str
    ts_ms: int
    direction: str            # LONG/SHORT
    scenario: str             # reversal/continuation/none
    ok: int                   # 1/0
    score: float              # 0..1
    have: int
    need: int
    gate_bits: int            # BIT_A|BIT_B|...
    reason: str               # stable reason code for veto/allow
    evidence: Dict[str, Any]  # compact evidence (ages, key flags, fp stats)
    contrib: Dict[str, float] # score contributions (optional)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
