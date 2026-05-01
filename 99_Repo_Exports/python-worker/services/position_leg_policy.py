from __future__ import annotations
"""Position leg policy — pure-math module for scale-in TP schema computation.

No external dependencies beyond stdlib.  Used by:
- ExecutionRouter  (pre-trade budget / WCL checks)
- BinanceExecutor  (TP qty allocation on resize)

Key concepts:
    PositionLeg    — one entry event (qty, entry price, side, signal_id).
    blended_entry  — qty-weighted average across all legs.
    worst_case_loss — max drawdown at SL for the entire position (all legs).
    max_add_qty    — how much more can be added without exceeding risk budget.
    build_scale_in_tp_schema — TP prices/qtys where TP1 closes the new (second) leg.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PositionLeg:
    """One entry into a position (original open or a scale-in add)."""
    entry: float               # fill price
    qty: float                 # absolute qty (always > 0)
    side: str                  # "LONG" | "SHORT"
    signal_id: str = ""        # source signal that triggered this leg
    ts_ms: int = 0             # epoch-ms when the leg was filled
    seq: int = 0               # monotonic sequence within the owner position

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry": self.entry,
            "qty": self.qty,
            "side": self.side,
            "signal_id": self.signal_id,
            "ts_ms": self.ts_ms,
            "seq": self.seq,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PositionLeg":
        return cls(
            entry=float(d.get("entry") or 0.0),
            qty=float(d.get("qty") or 0.0),
            side=str(d.get("side") or "LONG").upper(),
            signal_id=str(d.get("signal_id") or ""),
            ts_ms=int(d.get("ts_ms") or 0),
            seq=int(d.get("seq") or 0),
        )


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def blended_entry_price(legs: List[PositionLeg]) -> float:
    """Qty-weighted average entry price across all legs.

    Returns 0.0 when legs are empty or total qty is zero.
    """
    total_qty = sum(leg.qty for leg in legs)
    if total_qty <= 0:
        return 0.0
    return sum(leg.entry * leg.qty for leg in legs) / total_qty


def worst_case_loss_usdt(legs: List[PositionLeg], sl: float) -> float:
    """Max drawdown in USDT if SL is hit, across all legs.

    For LONG: loss = (entry - sl) * qty  (positive when sl < entry)
    For SHORT: loss = (sl - entry) * qty (positive when sl > entry)

    Returns total absolute loss (always >= 0).
    """
    if not legs or sl <= 0:
        return 0.0
    total_loss = 0.0
    for leg in legs:
        if leg.side.upper() == "LONG":
            loss = (leg.entry - sl) * leg.qty
        else:
            loss = (sl - leg.entry) * leg.qty
        total_loss += max(0.0, loss)
    return total_loss


def max_add_qty_for_budget(
    legs: List[PositionLeg],
    sl: float,
    budget_usdt: float,
    new_entry: float = 0.0,
) -> float:
    """Max additional qty that can be added without exceeding risk budget.

    budget_usdt = max acceptable WCL for the entire position after the add.
    new_entry   = expected fill price for the new leg (0 → use blended).

    Returns max qty (>= 0).  Returns 0.0 when budget is already exhausted.
    """
    if budget_usdt <= 0 or sl <= 0:
        return 0.0
    current_wcl = worst_case_loss_usdt(legs, sl)
    remaining = budget_usdt - current_wcl
    if remaining <= 0:
        return 0.0
    entry = new_entry if new_entry > 0 else blended_entry_price(legs)
    if entry <= 0:
        return 0.0
    # Per-unit loss for the new leg at SL
    side = legs[0].side.upper() if legs else "LONG"
    if side == "LONG":
        per_unit = entry - sl
    else:
        per_unit = sl - entry
    if per_unit <= 0:
        # SL is on the wrong side — no risk, can add infinite (cap at reasonble)
        return remaining / entry if entry > 0 else 0.0
    return remaining / per_unit


# ---------------------------------------------------------------------------
# Scale-in TP schema builder
# ---------------------------------------------------------------------------

def build_scale_in_tp_schema(
    existing_legs: List[PositionLeg],
    new_qty: float,
    tp_prices: List[float],
    original_tp_qtys: Optional[List[float]] = None,
) -> Tuple[List[float], List[float], int]:
    """Build TP prices + qty allocation for a scale-in resize.

    Design intent:
    - TP1 closes the new (second) leg entirely.
    - Remaining TPs are re-distributed across the combined position.
    - trail_activate_tp_level = 1 (activate trailing after TP1 = second leg closed).

    Args:
        existing_legs:    Legs already in the position.
        new_qty:          Qty of the new add (scale-in leg).
        tp_prices:        Requested TP price levels (at least 1).
        original_tp_qtys: Previous TP qty allocation (optional).

    Returns:
        (tp_prices, tp_qtys, trail_activate_tp_level)

    Convention:
        - tp_qtys[i] corresponds to tp_prices[i].
        - Sum of tp_qtys MUST equal total position qty (existing + new).
        - Monotonicity of prices is the caller's responsibility.
    """
    if not tp_prices:
        return [], [], 1

    total_existing = sum(leg.qty for leg in existing_legs)
    total_qty = total_existing + new_qty

    if total_qty <= 0:
        return list(tp_prices), [], 1

    n_tps = len(tp_prices)

    # TP1 qty = new leg qty (close the add first)
    tp1_qty = min(new_qty, total_qty)
    remaining = total_qty - tp1_qty

    tp_qtys: List[float] = [tp1_qty]

    if n_tps == 1:
        # Only one TP level — it gets everything
        tp_qtys = [total_qty]
    elif remaining > 0 and n_tps > 1:
        # Distribute remaining evenly across TP2..TPn
        n_remaining = n_tps - 1
        per_tp = remaining / n_remaining
        for i in range(n_remaining - 1):
            tp_qtys.append(per_tp)
            remaining -= per_tp
        # Last TP gets the remainder to avoid dust
        tp_qtys.append(remaining)
    else:
        # remaining == 0 and n_tps > 1: fill with zeros
        for _ in range(n_tps - 1):
            tp_qtys.append(0.0)

    # trail_activate_tp_level = 1 → trailing activates after TP1 (second leg closed)
    trail_activate_tp_level = 1

    return list(tp_prices), tp_qtys, trail_activate_tp_level
