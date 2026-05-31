import re

with open("python-worker/services/binance_executor.py", "r") as f:
    code = f.read()

# 1. Imports
if "from services.execution_policy import" not in code:
    code = code.replace("from services.telegram.telegram_client import TelegramClient",
"""from services.telegram.telegram_client import TelegramClient
try:
    from services.execution_policy import (
        MAKER_FIRST,
        SAFETY_FIRST,
        ExecutionPolicyDecision,
        resolve_execution_policy,
    )
except ImportError:
    pass
""")

# 2. Add methods if missing
def insert_before(target_str, insert_str, code_str):
    if insert_str.strip().split("\n")[0] in code_str:
        return code_str
    return code_str.replace(target_str, insert_str + "\n\n" + target_str)

top_funcs = """
def compute_limit_tp_price(tp_trigger_price: float, logical_side: str, *, offset_bps: float, tick_size: float) -> float:
    px = float(tp_trigger_price)
    off = abs(float(offset_bps)) / 10000.0
    raw = px * (1.0 + off) if logical_side == "LONG" else px * (1.0 - off)
    tick = float(tick_size or 0.0)
    if tick <= 0:
        return raw
    if logical_side == "LONG":
        import math
        return math.ceil(raw / tick) * tick
    import math
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
        import math
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
code = insert_before("\n# ---------------------------------------------------------------------------", top_funcs, code)

# Note: `python-worker/services/binance_executor.py` already contains a lot of the new methods. 
# We need to make sure we don't break existing P0 edits like `_validate_protective_prices`.
