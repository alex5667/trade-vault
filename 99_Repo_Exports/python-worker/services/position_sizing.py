from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

try:
    from common.balance_provider import BalanceProvider
except Exception:
    BalanceProvider = None  # type: ignore

@dataclass(frozen=True)
class SizingResult:
    qty: float
    risk_usd: float
    notional: float
    ok: bool
    reason: str = ""

def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        if v and str(v).strip():
            return float(v)
    except Exception:
        pass
    return default

def calculate_qty_fixed_risk(
    risk_usd: float,
    sl_dist: float,
    entry_price: float,
    lot_step: float,
    min_lot: float,
    max_lot: float,
    min_notional: float = 5.0,
) -> SizingResult:
    """
    Calculate quantity for fixed USD risk on linear contracts.
    
    Formula: risk = qty * sl_dist
    => qty = risk / sl_dist
    
    Applies floor-rounding to lot_step, and min/max/notional gates.
    """
    if sl_dist <= 1e-9 or entry_price <= 0:
         return SizingResult(0.0, 0.0, 0.0, False, "invalid_prices")

    # Linear formula: risk = qty * sl_dist
    raw_qty = risk_usd / sl_dist

    # Round down to step to ensure risk <= target (conservative sizing)
    if lot_step > 0:
        steps = math.floor((raw_qty / lot_step) + 1e-12)
        qty = steps * lot_step
    else:
        qty = raw_qty

    # 1. Cap at max_lot immediately (hard limit)
    if qty > max_lot:
        qty = max_lot

    # 2. Check min_lot
    if qty < min_lot:
        return SizingResult(qty, risk_usd, qty * entry_price, False, "min_lot")

    # 3. Check min_notional
    notional = qty * entry_price
    risk_bump_flag = False

    if notional < min_notional:
         # Must increase qty to meet min_notional
         # This WILL increase risk > target_risk for small accounts/stops
         req_qty = min_notional / entry_price
         if lot_step > 0:
             req_qty = math.ceil(req_qty / lot_step) * lot_step

         if req_qty > max_lot:
              return SizingResult(qty, risk_usd, notional, False, "min_notional_impossible")

         qty = req_qty
         risk_bump_flag = True

    # Recalculate actual risk
    actual_risk = qty * sl_dist
    reason = "ok"
    if risk_bump_flag:
        reason = "min_notional_bumps_risk"

    return SizingResult(qty, actual_risk, qty * entry_price, True, reason)

def calculate_qty_fixed_notional(
    target_notional: float,
    sl_dist: float,
    entry_price: float,
    lot_step: float,
    min_lot: float,
    max_lot: float,
    min_notional: float = 5.0,
) -> SizingResult:
    """
    Calculate quantity for fixed notional target.
    
    Formula: qty = target_notional / entry_price
    
    Applies floor-rounding to lot_step, and min/max/notional gates.
    """
    if entry_price <= 0:
         return SizingResult(0.0, 0.0, 0.0, False, "invalid_prices")

    raw_qty = target_notional / entry_price

    if lot_step > 0:
        steps = math.floor((raw_qty / lot_step) + 1e-12)
        qty = steps * lot_step
    else:
        qty = raw_qty

    if qty > max_lot:
        qty = max_lot

    if qty < min_lot:
        return SizingResult(qty, qty * sl_dist, qty * entry_price, False, "min_lot")

    notional = qty * entry_price
    risk_bump_flag = False

    if notional < min_notional:
         req_qty = min_notional / entry_price
         if lot_step > 0:
             req_qty = math.ceil(req_qty / lot_step) * lot_step

         if req_qty > max_lot:
              return SizingResult(qty, qty * sl_dist, notional, False, "min_notional_impossible")

         qty = req_qty
         risk_bump_flag = True

    actual_risk = qty * sl_dist if sl_dist > 0 else 0.0
    reason = "ok"
    if risk_bump_flag:
        reason = "min_notional_bumps_risk"

    return SizingResult(qty, actual_risk, qty * entry_price, True, reason)

def round_price_conservative(price: float, tick_size: float, side_int: int, is_tp: bool = True) -> float:
    """
    Round price to tick_size conservatively (improving hit-rate for TP).
    
    TP LONG (Above entry): Round DOWN (Closer to entry)
    TP SHORT (Below entry): Round UP (Closer to entry)
    
    SL LONG (Below entry): Round UP (Closer to entry = Safer/Tighter)
    SL SHORT (Above entry): Round DOWN (Closer to entry = Safer/Tighter)
    """
    if tick_size <= 1e-12:
        return price

    steps = price / tick_size

    if side_int > 0: # LONG
        if is_tp:
            # TP Above: Round DOWN -> smaller price -> closer
            final = math.floor(steps + 1e-9) * tick_size
        else:
            # SL Below: Round UP -> larger price -> closer
            final = math.ceil(steps - 1e-9) * tick_size
    else: # SHORT
        if is_tp:
             # TP Below: Round UP -> larger price -> closer
             final = math.ceil(steps - 1e-9) * tick_size
        else:
             # SL Above: Round DOWN -> smaller price -> closer
             final = math.floor(steps + 1e-9) * tick_size

    return final

def apply_position_sizing_to_ctx(
    ctx: Any,
    cfg: dict[str, Any],
    symbol: str,
    logger: Any = None
) -> None:
    """
    Applies fixed NOTIONAL position sizing based STRICTLY on environment vars.
    Notional = ACCOUNT_DEPOSIT_USD * (RISK_PERCENT / 100) * ACCOUNT_LEVERAGE.
    This guarantees that virtual and real trades have identical position sizes.
    """
    try:
        tp_mode = (cfg.get("TP_MODE") or "").upper()
        if tp_mode != "RR":
            return

        # 1. Config Check
        deposit_v = _env_float("ACCOUNT_DEPOSIT_USD", 0.0)
        risk_pct = _env_float("RISK_PERCENT", 0.0)
        leverage = _env_float("ACCOUNT_LEVERAGE", 100.0)

        # Do NOT use BalanceProvider to ensure identical sizing for real/virtual
        if deposit_v <= 0:
            deposit_v = float(getattr(ctx, "deposit_usd", 0.0) or 0.0)

        target_notional = 0.0
        if deposit_v > 0 and risk_pct > 0 and leverage > 0:
            target_notional = deposit_v * (risk_pct / 100.0) * leverage

        # Override with exact risk USD per trade if explicitly needed (fallback)
        risk_usd = _env_float("RISK_USD_PER_TRADE", 0.0)
        if risk_usd > 0 and target_notional <= 0:
            target_notional = risk_usd * leverage

        if target_notional <= 0:
            return

        # ── 2. Data Check ─────────────────────────────────────────────────
        sl_dist = float(getattr(ctx, "stop_dist", 0.0) or 0.0)
        entry = float(getattr(ctx, "entry_price", 0.0) or 0.0)
        if entry <= 1e-9:
            from common.dq_flags import append_dq_flag
            append_dq_flag(ctx, "sizing_no_levels")
            return

        # ── 3. Specs & Constants ──────────────────────────────────────────
        lot_step = 0.001
        min_lot = 0.001
        max_lot = 1000.0
        min_notional = _env_float("RISK_MIN_NOTIONAL_USD", 5.0)

        specs = getattr(ctx, "specs", None)
        if specs:
            lot_step = float(getattr(specs, "lot_step", 0.001))
            min_lot = float(getattr(specs, "min_lot", 0.001))
            max_lot = float(getattr(specs, "max_lot", 1000.0))
            if hasattr(specs, "min_notional"):
                min_notional = float(specs.min_notional)
        else:
            r = getattr(ctx, "redis", None)
            if r:
                try:
                    from symbol_specs_store import SymbolSpecsStore
                    sp = SymbolSpecsStore(r).get(symbol)
                    lot_step = float(sp.lot_step)
                    min_lot = float(sp.min_lot)
                    max_lot = float(sp.max_lot)
                    if hasattr(sp, "min_notional"):
                        min_notional = float(sp.min_notional)
                except Exception:
                    pass

        # ENV overrides for safety
        env_max = _env_float("RISK_MAX_QTY", 0.0)
        if env_max > 0:
            max_lot = min(max_lot, env_max)

        # 4. Calculation
        res = calculate_qty_fixed_notional(
            target_notional=target_notional,
            sl_dist=sl_dist,
            entry_price=entry,
            lot_step=lot_step,
            min_lot=min_lot,
            max_lot=max_lot,
            min_notional=min_notional
        )

        if res.ok:
            ctx.qty = res.qty
            ctx.risk_usd = res.risk_usd
            ctx.risk_usd_target = float(deposit_v * (risk_pct / 100.0))
            ctx.sl_dist = sl_dist
            ctx.sizing_ok = True

            if "min_notional_bumps_risk" in res.reason:
                 from common.dq_flags import append_dq_flag
                 append_dq_flag(ctx, "sizing_min_notional_bumps_risk")
        else:
            from common.dq_flags import append_dq_flag
            append_dq_flag(ctx, f"sizing_failed_{res.reason}")

    except Exception as e:
        if logger:
             logger.error(f"Sizing error: {e}")
        from common.dq_flags import append_dq_flag
        append_dq_flag(ctx, "sizing_exception")
