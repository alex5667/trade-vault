from __future__ import annotations

"""Local exit-intent validator for Binance Futures execution.

The goal is to reject malformed exit contracts *before* they hit Binance:
- in Hedge mode `positionSide` is mandatory
- `reduceOnly` is not a universal close flag
- Algo orders must not combine `closePosition=true` with `quantity`
- Algo orders must not combine `closePosition=true` with `reduceOnly`
"""

import time
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ExitIntentResult:
    will_reduce_exposure: bool
    will_open_new_exposure: bool
    is_valid_exit_contract: bool
    reason: str = ""


def validate_exit_intent(
    *,
    position_mode: str,
    position_side: str | None,
    exit_intent: str,
    reduce_only: bool,
    close_position: bool,
    quantity: float | None,
    order_type: str,
    working_type: str | None,
    is_algo: bool,
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


@dataclass(slots=True)
class ExecutionIntent:
    sid: str
    symbol: str
    action: Literal["open", "modify", "cancel", "resize", "close"]
    direction: Literal["LONG", "SHORT"]
    qty: float
    entry_type: Literal["MARKET", "LIMIT"]
    ts_signal_ms: int
    ts_decision_ms: int
    ts_enqueue_ms: int
    max_ttd_ms: int
    expires_at_ms: int

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ExecutionIntent:
        action = (payload.get("action") or "").strip().lower()
        if action not in {"open", "modify", "cancel", "resize", "close"}:
            action = "open"

        raw_side = str(payload.get("side") or payload.get("direction") or "").upper()
        direction = "LONG" if raw_side in {"BUY", "LONG"} else "SHORT"

        qty_val = 0.0
        if payload.get("qty") is not None:
            qty_val = float(payload.get("qty") or 0.0)
        elif payload.get("quantity") is not None:
            qty_val = float(payload.get("quantity") or 0.0)
        elif payload.get("lot") is not None:
            qty_val = float(payload.get("lot") or 0.0)

        entry = payload.get("entry")
        price = None
        if entry not in (None, 0, "", "0"):
            price = entry

        order_type = str(payload.get("type") or ("limit" if price else "market")).upper()
        entry_type = "MARKET" if order_type in {"MARKET", "MKT"} else "LIMIT"

        now_ms = int(time.time() * 1000)

        ts_exec_start_ms = payload.get("ts_exec_start_ms") or now_ms
        ts_decision_ms = payload.get("ts_decision_ms") or payload.get("ts_queue_ms") or payload.get("ts_signal_ms") or ts_exec_start_ms

        max_ttd_ms = payload.get("max_ttd_ms")
        if max_ttd_ms is None:
            max_ttd_ms = 50

        return cls(
            sid=(payload.get("sid") or ""),
            symbol=(payload.get("symbol") or "").upper(),
            action=action, # type: ignore
            direction=direction, # type: ignore
            qty=qty_val,
            entry_type=entry_type, # type: ignore
            ts_signal_ms=int(payload.get("ts_signal_ms") or ts_decision_ms),
            ts_decision_ms=int(ts_decision_ms),
            ts_enqueue_ms=int(payload.get("ts_enqueue_ms") or ts_decision_ms),
            max_ttd_ms=int(max_ttd_ms),
            expires_at_ms=int(payload.get("expires_at_ms") or (ts_decision_ms + max_ttd_ms))
        )


def validate_execution_intent(intent: ExecutionIntent, now_ms: int) -> None:
    age_ms = now_ms - intent.ts_decision_ms
    if age_ms > intent.max_ttd_ms:
        raise ValueError("INTENT_EXPIRED")
