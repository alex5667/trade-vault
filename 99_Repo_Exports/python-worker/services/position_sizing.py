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
        if v and v.strip():
            return float(v)
    except Exception:
        pass
    return default


def _env_on(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

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
    Apply position sizing to ctx.

    Two modes (controlled by RISK_USE_FIXED_DOLLAR_SIZING):

    MODE 1 — Fixed-dollar-risk (RISK_USE_FIXED_DOLLAR_SIZING=1)  [RECOMMENDED]
        risk_usd = deposit * risk_pct / 100
        qty      = risk_usd / sl_dist
        Guarantees: actual_risk_usd <= target_risk_usd * RISK_MAX_ACTUAL_OVER_TARGET

    MODE 0 — Fixed-notional (legacy, backward-compat)
        notional = deposit * risk_pct / 100 * leverage
        qty      = notional / entry_price
        WARNING: actual risk grows proportionally with wider SL — not risk-controlled.

    Stop-noise floor gate (STOP_NOISE_FLOOR_ENABLE=1):
        Denies the trade if planned stop_bps < noise floor.
    """
    from common.dq_flags import append_dq_flag

    try:
        tp_mode = (cfg.get("TP_MODE") or "").upper()
        if tp_mode != "RR":
            return

        # ── Global SL guard: sl_dist must be positive ────────────────────────
        sl_dist = getattr(ctx, "stop_dist", 0.0)
        if sl_dist is None or sl_dist <= 1e-9:
            append_dq_flag(ctx, "sizing_no_sl_dist")
            return
        # ──────────────────────────────────────────────────────────────

        # ── P0: Hard deny — SLQ reject must block sizing ─────────────────────
        # If slq_risk_adjust set sizing_ok=False, honour it here before any
        # sizing logic runs. This is the canonical enforcement point.
        if cfg.get("sizing_ok") is False:
            reason = str(cfg.get("slq_decision") or "sizing_cfg_denied")
            append_dq_flag(ctx, reason)
            try:
                ctx.sizing_ok = False
                ctx.sizing_deny_reason = reason
            except Exception:
                pass
            if logger:
                logger.warning(
                    f"[sizing] {symbol} hard-denied by cfg: reason={reason}"
                )
            return
        # ──────────────────────────────────────────────────────────────

        # ── 1. Risk budget ────────────────────────────────────────────────
        deposit_v = _env_float("ACCOUNT_DEPOSIT_USD", 0.0)
        risk_pct  = _env_float("RISK_PERCENT", 0.0)
        leverage  = _env_float("ACCOUNT_LEVERAGE", 100.0)

        if deposit_v <= 0:
            deposit_v = float(getattr(ctx, "deposit_usd", 0.0) or 0.0)

        risk_usd_target = 0.0
        if deposit_v > 0 and risk_pct > 0:
            risk_usd_target = deposit_v * (risk_pct / 100.0)

        # RISK_USD_PER_TRADE override
        risk_usd_fixed = _env_float("RISK_USD_PER_TRADE", 0.0)
        if risk_usd_fixed > 0 and risk_usd_target <= 0:
            risk_usd_target = risk_usd_fixed

        if risk_usd_target <= 0:
            return

        # ── 2. Price / SL data ────────────────────────────────────────────
        sl_dist = float(getattr(ctx, "stop_dist", 0.0) or 0.0)
        entry   = float(getattr(ctx, "entry_price", 0.0) or 0.0)
        if entry <= 1e-9:
            append_dq_flag(ctx, "sizing_no_entry_price")
            return

        # ── 3. Stop-noise floor gate ──────────────────────────────────────
        if _env_on("STOP_NOISE_FLOOR_ENABLE"):
            try:
                from services.risk.stop_contract import evaluate_stop_noise_floor_from_ctx
                floor_bps, floor_enabled = evaluate_stop_noise_floor_from_ctx(ctx, symbol)
                if floor_enabled and entry > 0 and sl_dist > 0:
                    planned_stop_bps = (sl_dist / entry) * 10_000.0
                    kind = str(getattr(ctx, "kind", "na") or "na")
                    # Expose for metrics/logs
                    try:
                        ctx.stop_noise_floor_bps = floor_bps
                        ctx.planned_stop_bps = planned_stop_bps
                    except Exception:
                        pass
                    if planned_stop_bps < floor_bps:
                        append_dq_flag(ctx, "stop_inside_noise_floor")
                        ctx.sizing_ok = False
                        if logger:
                            logger.warning(
                                f"[sizing] {symbol} stop_inside_noise_floor "
                                f"planned={planned_stop_bps:.2f}bps "
                                f"floor={floor_bps:.2f}bps"
                            )
                        return
            except Exception as e:
                if logger:
                    logger.error(f"[sizing] noise_floor check error: {e}")
                # Fail-open: continue sizing

        # ── 4. Exchange specs ─────────────────────────────────────────────
        lot_step    = 0.001
        min_lot     = 0.001
        max_lot     = 1000.0
        min_notional = _env_float("RISK_MIN_NOTIONAL_USD", 5.0)

        specs = getattr(ctx, "specs", None)
        if specs:
            lot_step = float(getattr(specs, "lot_step", 0.001))
            min_lot  = float(getattr(specs, "min_lot",  0.001))
            max_lot  = float(getattr(specs, "max_lot",  1000.0))
            if hasattr(specs, "min_notional"):
                min_notional = float(specs.min_notional)
        else:
            r = getattr(ctx, "redis", None)
            if r is not None:
                try:
                    _mod = type(r).__module__ or ""
                    if "asyncio" in _mod or "aioredis" in _mod:
                        r = None
                except Exception:
                    r = None
            if r:
                try:
                    from symbol_specs_store import SymbolSpecsStore
                    sp = SymbolSpecsStore(r).get(symbol)
                    lot_step = sp.lot_step
                    min_lot  = sp.min_lot
                    max_lot  = sp.max_lot
                    if hasattr(sp, "min_notional"):
                        min_notional = float(sp.min_notional)
                except Exception:
                    pass

        env_max = _env_float("RISK_MAX_QTY", 0.0)
        if env_max > 0:
            max_lot = min(max_lot, env_max)

        # ── 5. Sizing calculation ─────────────────────────────────────────
        use_fixed_risk = _env_on("RISK_USE_FIXED_DOLLAR_SIZING", "0")

        if use_fixed_risk:
            # ── MODE 1: Fixed-dollar-risk (canary/prod) ───────────────────
            if sl_dist <= 1e-9:
                append_dq_flag(ctx, "sizing_no_sl_dist")
                return

            res = calculate_qty_fixed_risk(
                risk_usd=risk_usd_target,
                sl_dist=sl_dist,
                entry_price=entry,
                lot_step=lot_step,
                min_lot=min_lot,
                max_lot=max_lot,
                min_notional=min_notional,
            )

            if not res.ok:
                append_dq_flag(ctx, f"sizing_failed_{res.reason}")
                return

            # Hard guard: actual_risk_usd <= target * max_over_ratio
            max_over = _env_float("RISK_MAX_ACTUAL_OVER_TARGET", 1.02)
            actual_risk_usd = res.qty * sl_dist
            if actual_risk_usd > risk_usd_target * max_over:
                append_dq_flag(ctx, "sizing_risk_budget_exceeded")
                ctx.sizing_ok = False
                if logger:
                    logger.error(
                        f"[sizing] {symbol} risk_budget_exceeded "
                        f"actual={actual_risk_usd:.4f} "
                        f"target={risk_usd_target:.4f} "
                        f"ratio={actual_risk_usd / max(risk_usd_target, 1e-9):.3f}"
                    )
                return

            ctx.qty = res.qty
            ctx.risk_usd        = actual_risk_usd
            ctx.risk_usd_target = risk_usd_target
            ctx.sl_dist         = sl_dist
            ctx.sizing_ok       = True
            ctx.sizing_mode     = "fixed_risk"

            if "min_notional_bumps_risk" in res.reason:
                append_dq_flag(ctx, "sizing_min_notional_bumps_risk")

        else:
            # ── MODE 0: Fixed-notional (legacy) ───────────────────────────
            target_notional = risk_usd_target * leverage

            res = calculate_qty_fixed_notional(
                target_notional=target_notional,
                sl_dist=sl_dist,
                entry_price=entry,
                lot_step=lot_step,
                min_lot=min_lot,
                max_lot=max_lot,
                min_notional=min_notional,
            )

            if res.ok:
                ctx.qty = res.qty
                ctx.risk_usd        = res.risk_usd
                ctx.risk_usd_target = risk_usd_target
                ctx.sl_dist         = sl_dist
                ctx.sizing_ok       = True
                ctx.sizing_mode     = "fixed_notional"

                if "min_notional_bumps_risk" in res.reason:
                    append_dq_flag(ctx, "sizing_min_notional_bumps_risk")
            else:
                append_dq_flag(ctx, f"sizing_failed_{res.reason}")

    except Exception as e:
        if logger:
            logger.error(f"[sizing] Sizing error: {e}")
        from common.dq_flags import append_dq_flag
        append_dq_flag(ctx, "sizing_exception")
