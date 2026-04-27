import re

with open("python-worker/services/binance_executor.py", "r") as f:
    code = f.read()

# 1. Imports
if "ExecutionPolicyDecision" not in code:
    code = code.replace("from services.telegram.telegram_client import TelegramClient", 
"""try:
    from services.execution_policy import (
        MAKER_FIRST,
        SAFETY_FIRST,
        ExecutionPolicyDecision,
        resolve_execution_policy,
    )
except Exception:
    pass

from services.telegram.telegram_client import TelegramClient""")

if "def compute_limit_tp_price" not in code:
    top_funcs = """
def compute_limit_tp_price(tp_trigger_price: float, logical_side: str, *, offset_bps: float, tick_size: float) -> float:
    px = float(tp_trigger_price)
    off = abs(float(offset_bps)) / 10000.0
    raw = px * (1.0 + off) if logical_side == "LONG" else px * (1.0 - off)
    tick = float(tick_size or 0.0)
    if tick <= 0:
        return raw
    if logical_side == "LONG":
        return math.ceil(raw / tick) * tick
    return math.floor(raw / tick) * tick

def compute_trailing_activate_price(
    logical_side: str,
    *,
    latest_price: float,
    tick_size: float,
    buffer_bps: float,
    user_activate_price: float | None = None,
) -> float:
    latest = float(latest_price)
    if latest <= 0:
        raise ValueError("latest_price must be > 0 for trailing activation")

    tick = float(tick_size or 0.0)
    buf = abs(float(buffer_bps)) / 10000.0
    if user_activate_price is not None:
        raw = float(user_activate_price)
    else:
        raw = latest * (1.0 + buf) if logical_side == "LONG" else latest * (1.0 - buf)

    if tick > 0:
        if logical_side == "LONG":
            px = math.ceil(raw / tick) * tick
            if px <= latest:
                px += tick
        else:
            px = math.floor(raw / tick) * tick
            if px >= latest:
                px -= tick
        if px <= 0:
            raise ValueError("computed activatePrice <= 0")
    else:
        px = raw

    if logical_side == "LONG" and not (px > latest):
        raise ValueError("activatePrice must be above latest price for LONG trailing exit")
    if logical_side == "SHORT" and not (px < latest):
        raise ValueError("activatePrice must be below latest price for SHORT trailing exit")
    return px

def _tp_state_name(level: int, state: str) -> str:
    return f"TP{int(level)}_{str(state).strip().upper()}"
"""
    code = code.replace("\n# ---------------------------------------------------------------------------\n# Symbol filter cache", top_funcs + "\n# ---------------------------------------------------------------------------\n# Symbol filter cache")

# Add missing methods to class
if "def _resolve_execution_policy" not in code:
    missing_methods = """
    def _resolve_execution_policy(self, payload: dict, symbol: str) -> 'ExecutionPolicyDecision':
        return resolve_execution_policy(
            payload=payload,
            symbol=symbol,
            default_policy=self.exec_policy_default,
            maker_allowed_symbols=self.exec_policy_maker_allowed_symbols,
            tp_market_working_type=self.tp_market_working_type,
            tp_limit_trigger_working_type=self.tp_limit_trigger_working_type,
            tp_limit_time_in_force=self.tp_limit_time_in_force,
            watchdog_enabled=self.tp_limit_watchdog_enable,
            watchdog_timeout_ms=self.tp_limit_watchdog_timeout_ms,
        )

    def _position_qty_tolerance(self, symbol: str, *, filters: "FiltersCache") -> float:
        try:
            return max(float(filters.get(symbol).step_size or 0.0), 1e-12)
        except Exception:
            return 1e-12

    def _emit_tp_state(self, sid: str, symbol: str, level: int, state: str, **extra) -> None:
        tp_state = _tp_state_name(level, state)
        ev = {
            "sid": sid,
            "symbol": symbol,
            "action": "tp_state",
            "tp_level": int(level),
            "tp_state": tp_state,
            **extra,
        }
        self._exec_event(ev)
        state_doc = {f"tp{int(level)}_state": tp_state}
        for k, v in extra.items():
            state_doc[f"tp{int(level)}_{k}"] = v
        self._save_order_state(sid, state_doc)

    def _submit_reduce_only_market_exit(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        reason_tag: str,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> dict:
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q_close, _ = self._quantize(symbol, qty, None, filters=filters)
        params = {
            "symbol": symbol,
            "side": exit_side,
            "type": "MARKET",
            "quantity": q_close,
            "newClientOrderId": _make_cid(sid, reason_tag),
            "newOrderRespType": "RESULT",
        }
        if self.position_mode == "oneway":
            params["reduceOnly"] = True
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=True,
                close_position=False,
                quantity=float(q_close),
                order_type="MARKET",
                working_type=None,
                is_algo=False,
            )
        elif pos_side:
            params["positionSide"] = pos_side
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=False,
                close_position=False,
                quantity=float(q_close),
                order_type="MARKET",
                working_type=None,
                is_algo=False,
            )
        j = client.post_plain_order(params)
        return {
            "close_order_id": j.get("orderId"),
            "close_client_id": params["newClientOrderId"],
            "close_order_status": j.get("status"),
            "close_reason_tag": reason_tag,
        }

    def _emergency_flatten_position(self, *, sid: str, symbol: str, logical_side: str, qty: float, client: "BinanceFuturesClient", filters: "FiltersCache") -> dict:
        close = self._submit_reduce_only_market_exit(sid=sid, symbol=symbol, logical_side=logical_side, qty=qty, reason_tag="emerg", client=client, filters=filters)
        return {"emergency_order_id": close.get("close_order_id"), "emergency_client_id": close.get("close_client_id")}
"""
    code = code.replace("    def _place_protective(", missing_methods + "\n    def _place_protective(")

with open("python-worker/services/binance_executor.py", "w") as f:
    f.write(code)

print("Injected core missing functions safely.")
