from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple


class BarrierOutcome(str, Enum):
    TP_HIT = "TP_HIT"
    SL_HIT = "SL_HIT"
    TIMEOUT = "TIMEOUT"
    NO_TICKS = "NO_TICKS"


@dataclass(frozen=True)
class BarrierSpec:
    h_ms: int
    tp_bps: float
    sl_bps: float


@dataclass(frozen=True)
class BarrierResult:
    outcome: BarrierOutcome  # TP_HIT | SL_HIT | TIMEOUT | NO_TICKS
    hit_ms: int
    mae_bps: float
    mfe_bps: float
    adverse_proxy: float


def _bps_move(px: float, ref: float) -> float:
    if ref <= 0:
        return 0.0
    return (px - ref) / ref * 10_000.0


def pick_entry_price(path: List[Tuple[int, float]]) -> float:
    return float(path[0][1]) if path else 0.0


def label_path(
    *,
    ts0_ms: int,
    direction: str,
    entry_px: float,
    path: List[Tuple[int, float]],  # ascending
    spec: BarrierSpec,
) -> BarrierResult:
    if entry_px <= 1e-9 or not path:
        return BarrierResult(outcome=BarrierOutcome.NO_TICKS, hit_ms=0, mae_bps=0.0, mfe_bps=0.0, adverse_proxy=0.0)

    assert entry_px > 1e-9, f"entry_px must be positive, got {entry_px}"

    d = (direction or "").upper()
    is_long = d == "LONG"
    tp = float(spec.tp_bps)
    sl = float(spec.sl_bps)

    def signed_bps(px: float) -> float:
        b = _bps_move(px, entry_px)
        return b if is_long else -b

    mae = 0.0  # most negative (adverse)
    mfe = 0.0  # most positive (favorable)
    hit_ms = ts0_ms + spec.h_ms
    outcome = BarrierOutcome.TIMEOUT

    for ts, px in path:
        sb = signed_bps(px)
        if sb > mfe:
            mfe = sb
        if sb < mae:
            mae = sb

        if sb >= tp:
            outcome = BarrierOutcome.TP_HIT
            hit_ms = int(ts)
            break
        if sb <= -sl:
            outcome = BarrierOutcome.SL_HIT
            hit_ms = int(ts)
            break

    mae_mag = abs(mae)
    mfe_mag = max(0.0, mfe)
    adverse_proxy = (mae_mag / mfe_mag) if mfe_mag > 1e-9 else mae_mag
    return BarrierResult(outcome=outcome, hit_ms=hit_ms, mae_bps=mae_mag, mfe_bps=mfe_mag, adverse_proxy=adverse_proxy)
