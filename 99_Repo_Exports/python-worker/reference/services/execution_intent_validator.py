from __future__ import annotations

"""Local exit-intent validator for Binance Futures execution.

The goal is to reject malformed exit contracts *before* they hit Binance:
- in Hedge mode `positionSide` is mandatory
- `reduceOnly` is not a universal close flag
- Algo orders must not combine `closePosition=true` with `quantity`
- Algo orders must not combine `closePosition=true` with `reduceOnly`
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ExitIntentResult:
    will_reduce_exposure: bool
    will_open_new_exposure: bool
    is_valid_exit_contract: bool
    reason: str = ""


def validate_exit_intent(
    *
    position_mode: str
    position_side: Optional[str]
    exit_intent: str
    reduce_only: bool
    close_position: bool
    quantity: Optional[float]
    order_type: str
    working_type: Optional[str]
    is_algo: bool
) -> ExitIntentResult:
    pm = (position_mode or "oneway").strip().lower()
    ps = (position_side or "").strip().upper() or None
    ei = (exit_intent or "").strip().lower()
    ot = (order_type or "").strip().upper()
    wt = (working_type or "").strip().upper() or None
    qty = float(quantity) if quantity is not None else None

    if pm not in {"oneway", "hedge"}:
        return ExitIntentResult(False, True, False, "invalid_position_mode")

    if pm == "hedge" and ps not in {"LONG", "SHORT"}:
        return ExitIntentResult(False, True, False, "positionSide_required_in_hedge")

    if wt is not None and wt not in {"MARK_PRICE", "CONTRACT_PRICE"}:
        return ExitIntentResult(False, True, False, "invalid_workingType")

    if is_algo and close_position and qty not in (None, 0.0):
        return ExitIntentResult(False, True, False, "algo_closePosition_incompatible_with_quantity")

    if is_algo and close_position and reduce_only:
        return ExitIntentResult(False, True, False, "algo_closePosition_incompatible_with_reduceOnly")

    if not is_algo and pm == "hedge" and reduce_only:
        return ExitIntentResult(False, True, False, "reduceOnly_forbidden_in_hedge_plain_order")

    if ei not in {"reduce", "close"}:
        return ExitIntentResult(False, True, False, "unsupported_exit_intent")

    if close_position:
        return ExitIntentResult(True, False, True, "close_position")

    if reduce_only:
        return ExitIntentResult(True, False, True, "reduce_only")

    # In hedge mode a SELL on LONG side / BUY on SHORT side can still be a valid
    # reducing order, but without reduceOnly/closePosition we cannot prove this
    # contract is safe enough for the executor.
    return ExitIntentResult(False, True, False, "exit_contract_not_provably_reducing")
