from __future__ import annotations

import math
import os
import uuid
from collections.abc import Sequence
from typing import Any

from common.log import setup_logger

# domain/handlers.py
from domain.evidence_keys import MetaKeys
from domain.time_utils import session_from_ts_ms

logger = setup_logger("DomainHandlers")

def _parse_boolish(v: Any, default: bool) -> bool:
    """Parse 0/1, 'true'/'false', bool, int; fail-open to default."""
    try:
        if v is None:
            return bool(default)
        if isinstance(v, bool):
            return bool(v)
        if isinstance(v, (int, float)):
            return int(v) != 0
        if isinstance(v, str):
            s = v.strip().lower()
            if s.isdigit():
                return int(s) != 0
            if s in {"true","yes","on"}:
                return True
            if s in {"false","no","off"}:
                return False
        return bool(default)
    except Exception:
        return bool(default)

from domain.calculators import (
    calc_missed_profit,
    calc_trailing_sl,
    pnl_pct_simple,
    snapshot_tp1_excursions,
)
from domain.models import PositionState, SignalNorm, Tick, TradeClosed, TradeEvent
from domain.normalizers import bucket_close_reason
from domain.tick_price import trigger_prices
import contextlib

ADVERSE_BUCKETS_MS = (100, 200, 400, 800)

def _update_excursions_and_adverse(pos: PositionState, tick) -> None:
    """
    O(1) update of favorable/adverse extremes with timestamps.
    Handles LONG/SHORT semantics deterministically.
    Also fixates survival/impact probes (adverse move bps) at windows.
    """
    mid = float(getattr(tick, "mid", 0.0) or getattr(tick, "price", 0.0) or 0.0)
    ts_ms = int(getattr(tick, "ts_ms", 0) or 0)
    if mid <= 0 or ts_ms <= 0:
        return

    # excursions (price space, side-aware)
    if pos.direction == "LONG":
        # favorable=max, adverse=min
        if mid > pos.max_favorable_price:
            pos.max_favorable_price = mid
            pos.max_favorable_ts_ms = ts_ms
        if pos.max_adverse_price == 0.0 or mid < pos.max_adverse_price:
            pos.max_adverse_price = mid
            pos.max_adverse_ts_ms = ts_ms
    else:
        # SHORT: favorable=min, adverse=max
        if mid < pos.max_favorable_price:
            pos.max_favorable_price = mid
            pos.max_favorable_ts_ms = ts_ms
        if mid > pos.max_adverse_price:
            pos.max_adverse_price = mid
            pos.max_adverse_ts_ms = ts_ms

    # adverse move in bps
    entry = float(pos.entry_price)
    if entry <= 0:
        return
    age = ts_ms - int(pos.entry_ts_ms)

    if pos.direction == "LONG":
        adverse_now = max(0.0, (entry - mid) / entry * 10000.0)
    else:
        adverse_now = max(0.0, (mid - entry) / entry * 10000.0)

    for b in ADVERSE_BUCKETS_MS:
        if b in pos.adverse_bps_t:
            continue
        prev = pos.adverse_bps_running.get(b, 0.0)
        pos.adverse_bps_running[b] = max(prev, adverse_now)
        if age >= b:
            pos.adverse_bps_t[b] = pos.adverse_bps_running[b]
            pos.adverse_bps_running.pop(b, None)


def _enrich_closed_from_pos(closed: TradeClosed, pos: PositionState, exit_px: float, now_ms: int) -> TradeClosed:
    """
    Expert Implementation: Centralized TradeClosed enrichment.
    Ensures BPS metrics and metadata are populated consistently.
    """
    entry_ts = int(pos.entry_ts_ms)
    entry = float(pos.entry_price)

    # --- Time Sync Defense (Expert Recommendation) ---
    # hold_ms must always be >= 0. If now_ms < entry_ts, clamp to 0.
    if now_ms < entry_ts:
        skew = entry_ts - now_ms
        logger.warning(f"🚨 [TIME_SYNC] Enrichment desync: now_ms ({now_ms}) < entry_ts ({entry_ts}) for pos {pos.id} (skew={skew}ms). Clamping hold_ms to 0.")
        hold_ms = 0
    else:
        hold_ms = int(now_ms - entry_ts)

    if entry > 0:
        if pos.direction == "LONG":
            mfe_bps = max(0.0, (pos.max_favorable_price - entry) / entry * 10000.0)
            mae_bps = max(0.0, (entry - pos.max_adverse_price) / entry * 10000.0)
            mfe_ts = int(pos.max_favorable_ts_ms)
        else:
            mfe_bps = max(0.0, (entry - pos.max_favorable_price) / entry * 10000.0)
            mae_bps = max(0.0, (pos.max_adverse_price - entry) / entry * 10000.0)
            mfe_ts = int(pos.max_favorable_ts_ms)

        # Defensive: time_to_mfe_ms must also be >= 0
        if mfe_ts and mfe_ts < entry_ts:
            time_to_mfe_ms = 0
        else:
            time_to_mfe_ms = max(0, mfe_ts - entry_ts) if mfe_ts else None
    else:
        mfe_bps = mae_bps = 0.0
        time_to_mfe_ms = None

    # fill TradeClosed fields (P0 Identity)
    closed.schema_version = max(int(getattr(closed, "schema_version", 2) or 2), 2)
    closed.trade_id = pos.id
    closed.signal_id = pos.p0_signal_id
    closed.symbol = pos.symbol
    closed.regime = pos.p0_regime
    closed.session = pos.p0_session
    closed.scenario = pos.p0_scenario
    closed.entry_reason = pos.p0_entry_reason

    # FIX: Ensure direction is always propagated from PositionState → TradeClosed
    closed.direction = pos.direction
    closed.side = str(pos.direction)

    # Execution Details
    closed.qty = float(pos.lot)
    closed.entry_px = entry
    closed.exit_px = float(exit_px)

    if closed.fees_usd is None:
        closed.fees_usd = float(getattr(closed, "fees", 0.0) or 0.0) or None

    closed.spread_bps_at_entry = pos.p0_spread_bps_at_entry
    closed.slippage_bps_est = pos.p0_slippage_bps_est
    closed.p0_slippage_bps_est = pos.p0_slippage_bps_est
    closed.book_age_ms = pos.p0_book_age_ms
    closed.entry_regime = str(getattr(pos, "entry_regime", "na") or "na")

    closed.hold_ms = hold_ms
    closed.mae_bps = mae_bps
    closed.mfe_bps = mfe_bps
    closed.time_to_mfe_ms = time_to_mfe_ms

    # ✅ Realized execution quality (slippage model by fact)
    if entry > 0:
        mid_exit = float(getattr(pos, "exit_mid_price", 0.0) or 0.0)
        if mid_exit > 0:
            # |exit_price - mid| / mid * 10000
            closed.realized_slippage_bps = abs(float(exit_px) - mid_exit) / mid_exit * 10000.0
            closed.realized_spread_bps = float(getattr(pos, "exit_spread_bps", 0.0) or 0.0)

    # ✅ Fix: ensure order_id and is_virtual are propagated
    closed.order_id = pos.id
    closed.is_virtual = getattr(pos, "is_virtual", False)

    # merge features (prefer explicitly provided keys from closed.features)
    feats = dict(pos.p0_features_snapshot or {})
    if pos.adverse_bps_t:
        feats["adverse_bps_t"] = dict(pos.adverse_bps_t)
    if closed.features:
        merged = dict(feats)
        merged.update(dict(closed.features))
        closed.features = merged
    else:
        closed.features = feats

    # P41 Native Meta Fields
    closed.meta_enforce_cov_bucket = getattr(pos, "meta_enforce_cov_bucket", "")
    closed.meta_enforce_applied = getattr(pos, "meta_enforce_applied", -1)

    # Phase 5: ATR selection metadata for post-trade analytics
    # Extract from meta.atr_profile (enriched in Phase 4 SignalPipeline)
    profile = (pos.signal_payload.get("meta") or {}).get("atr_profile") or {}
    closed.atr_sel_tf = str(profile.get("atr_tf") or getattr(pos, "atr_tf_ms", "") or "")
    closed.atr_sel_src = str(profile.get("src") or getattr(pos, "atr_source", "") or "")
    closed.atr_sel_age_ms = int(profile.get("atr_age_ms") or getattr(pos, "atr_age_ms", 0) or 0)

    return closed



# =============================================================================
# Empirical time-bucket snapshots (MFE@T / MAE@T)
# =============================================================================

def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _compute_spread_bps_from_tick(tick: Any, mid: float) -> float:
    """
    Best-effort оценка спреда в bps на основе полей тика.

    Почему best-effort:
      - в разных источниках тик может иметь разные поля (bid/ask, best_bid/best_ask, b/a, etc.)
      - если bid/ask недоступны — возвращаем 0.0 (fail-open).

    spread_bps = (ask - bid) / mid * 10000
    """
    try:
        if mid <= 0:
            return 0.0
        # Популярные варианты имён полей:
        bid = None
        ask = None
        for bname in ("bid", "best_bid", "b", "bid_price", "l1_bid", "bb"):
            if hasattr(tick, bname):
                bid = getattr(tick, bname)
                break
            if isinstance(tick, dict) and bname in tick:
                bid = tick.get(bname)
                break
        for aname in ("ask", "best_ask", "a", "ask_price", "l1_ask", "ba"):
            if hasattr(tick, aname):
                ask = getattr(tick, aname)
                break
            if isinstance(tick, dict) and aname in tick:
                ask = tick.get(aname)
                break
        b = _safe_float(bid, 0.0)
        a = _safe_float(ask, 0.0)
        if b > 0 and a > 0 and a >= b:
            return float((a - b) / float(mid) * 10_000.0)
    except Exception:
        pass
    return 0.0


def _micro_buckets_ms_from_env() -> list[int]:
    """
    Бакеты для adverse_bps@T (миллисекунды).
    По умолчанию: 500ms и 2000ms (то, что вы просили как старт).
    """
    raw = (os.getenv("EMP_ADVERSE_BUCKETS_MS", "500,2000") or "").strip()
    out: list[int] = []
    for p in raw.split(","):
        s = p.strip()
        if not s:
            continue
        try:
            v = int(float(s))
            if v > 0:
                out.append(v)
        except Exception:
            pass
    out.sort()
    return out


def update_adverse_bps_snapshots(pos: PositionState, *, mid: float, ts_ms: int) -> None:
    """
    Обновляет adverse_bps_running и фиксирует adverse_bps_t для бакетов.

    ВАЖНО:
      - функция должна быть максимально безопасной (fail-open)
      - не должна влиять на торговую логику даже при ошибках
    """
    try:
        if not _env_bool("EMP_ADVERSE_SNAPSHOTS_ENABLED", True):
            return
        if pos.entry_ts_ms <= 0 or pos.entry_price <= 0:
            return
        mid_f = float(mid)
        if not math.isfinite(mid_f) or mid_f <= 0:
            return
        elapsed = int(ts_ms) - int(pos.entry_ts_ms)
        if elapsed < 0:
            return
        buckets = _micro_buckets_ms_from_env()
        if not buckets:
            return

        # adverse_bps: движение ПРОТИВ позиции, всегда >= 0
        if pos.is_long():
            adverse = max(0.0, (pos.entry_price - mid_f) / pos.entry_price * 10_000.0)
        else:
            adverse = max(0.0, (mid_f - pos.entry_price) / pos.entry_price * 10_000.0)

        for b in buckets:
            # пока не достигли бакета — копим running max
            if elapsed <= b:
                cur = float(pos.adverse_bps_running.get(b, 0.0) or 0.0)
                if adverse > cur:
                    pos.adverse_bps_running[b] = float(adverse)
            # когда бакет пройден — фиксируем один раз
            if elapsed >= b and b not in pos.adverse_bps_t:
                pos.adverse_bps_t[b] = float(pos.adverse_bps_running.get(b, adverse) or 0.0)
    except Exception:
        # fail-open: никогда не ломаем торговлю
        return

def _should_start_trailing_after_tp1(pos) -> bool:
    """
    Policy: should trailing be allowed to start / be applied after TP1?

    Priority:
      1) TRAIL_FORCE_ALWAYS_AFTER_TP1=1  -> ALWAYS allow (emergency override).
      2) TRAIL_COND_ENABLED=1            -> allow only if pos.trail_after_tp1 == True
         (field comes from signal publisher; default True for fail-open).
      3) TRAIL_COND_ENABLED=0            -> fallback to legacy FORCE_TRAIL_AFTER_TP1.

    This function is intentionally pure and safe: no external deps, no exceptions.
    """
    try:
        if _env_bool("TRAIL_FORCE_ALWAYS_AFTER_TP1", False):
            return True
        if _env_bool("TRAIL_COND_ENABLED", True):
            # -----------------------------------------------------------------
            # NEW: robust read.
            #
            # Some deployments may not copy trail_after_tp1 into PositionState fields,
            # but they DO keep the original signal payload in pos.signal_payload.
            # We support both:
            #   - pos.trail_after_tp1 (preferred)
            #   - pos.signal_payload["trail_after_tp1"] (fallback)
            #
            # Fail-open default: True.
            # -----------------------------------------------------------------
            v = getattr(pos, "trail_after_tp1", None)
            if v is None:
                try:
                    sp = getattr(pos, "signal_payload", None) or {}
                    if isinstance(sp, dict) and "trail_after_tp1" in sp:
                        v = sp.get("trail_after_tp1")
                except Exception:
                    v = None
            return bool(True if v is None else v)
        return _env_bool("FORCE_TRAIL_AFTER_TP1", True)
    except Exception:
        return True  # fail-open

def _trail_offset_from_payload(pos) -> float:
    """
    Determine trailing distance at arming time (TP1 moment).

    Source of truth:
      - pos.signal_payload["atr"] and pos.signal_payload["trailing_tp1_offset_atr"]
        (TradeMonitor._normalize_signal() ensures defaults from spec/env are applied).

    Fail-open:
      - return 0.0 if unavailable.
    """
    try:
        p = getattr(pos, "signal_payload", None) or {}
        atr = float(p.get("atr") or 0.0)
        mult = float(p.get("trailing_tp1_offset_atr") or 0.0)
        if atr > 0 and mult > 0:
            return float(atr * mult)
    except Exception:
        pass
    return 0.0


def maybe_arm_trailing_after_tp1(pos, *, spec, ts_ms: int) -> TradeEvent | None:
    """
    Arm trailing right at TP1, based on conditional policy.

    Why this exists:
      - You already have _should_start_trailing_after_tp1(pos) but process_tick had 'pass'.
      - Some TP logic (rocket_v1) relies on pos.trailing_started to switch behavior for TP2/TP3.
      - TradeMonitor.on_tick trailing logic expects trailing_started/active flags to be set.

    Behavior:
      - If allowed: call apply_trailing_update(pos, new_sl=pos.sl) to flip trailing_started/active.
        We do NOT move SL immediately here (new_sl == current sl), making this safe.
      - If denied: mark skipped flags for audit and return a TRALING_SKIPPED event.
    """
    if getattr(pos, "closed", False):
        return None
    if getattr(pos, "trailing_started", False):
        return None

    allow = _should_start_trailing_after_tp1(pos)
    if not allow:
        # Audit fields (safe even if dataclass doesn't have them in older deployments)
        try:
            pos.trailing_skipped_after_tp1 = True
            pos.trailing_skipped_reason = str(getattr(pos, "trail_after_tp1_reason", "") or "COND_DISABLED")
            pos.trailing_skip_reason = pos.trailing_skipped_reason
            pos.trailing_active = False
        except Exception:
            pass
        try:
            return TradeEvent(
                event_type="TRAILING_SKIPPED",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=int(ts_ms),
                payload={
                    "reason": str(getattr(pos, "trail_after_tp1_reason", "") or "COND_DISABLED"),
                    "trail_profile": str(getattr(pos, "trail_profile", "") or ""),
                },
            )
        except Exception:
            return None

    # Allowed: arm trailing (no immediate SL move -> NOW: Secure Profit)
    try:
        pos.trailing_armed_ts_ms = int(ts_ms)
        pos.trailing_start_reason = str(getattr(pos, "trail_after_tp1_reason", "") or "ARMED")
    except Exception:
        pass

    offset = _trail_offset_from_payload(pos)

    # --------------------------------------------------------------------------
    # SECURE PROFIT: Move SL to BreakEven + Fees + Slippage immediately
    # User Request: "After TP1 move stop to BE + (fees+slip)"
    # --------------------------------------------------------------------------
    current_sl = float(getattr(pos, "sl", 0.0) or 0.0)
    secured_sl = current_sl

    try:
        # 1. Estimate costs (Fees + Slippage)
        # Commission: default to 4bps roundtrip (0.0004) if not set
        comm_rate = getattr(spec, "commission_rate", None)
        if comm_rate is None:
            comm_rate = 0.0005 # conservative default for crypto maker/taker mix

        # Roundtrip fees = rate * 2
        fees_bps = comm_rate * 2.0

        # Slippage buffer: conservative 2 bps
        slip_bps = 0.0005

        total_verify_bps = fees_bps + slip_bps
        entry_price = float(pos.entry_price or 0.0)

        if entry_price > 0:
            buffer_price = entry_price * total_verify_bps
            is_long = (pos.direction == "LONG")

            # Calculate BE+ level
            if is_long:
                be_plus = entry_price + buffer_price
                # Move SL up if better
                if be_plus > current_sl:
                    secured_sl = be_plus
            else:
                be_minus = entry_price - buffer_price
                # Move SL down if better (SL < current) works for short?
                # For SHORT: SL is above price. We lower it to lock profit?
                # No, for SHORT, Entry is high. Profit is low.
                # SL must be LOWER than Entry to be in profit (Wait, SL for Short is ABOVE price usually)
                # Correct: SHORT SL is STOP LOSS.
                # Initial SL > Entry.
                # Profitable price < Entry.
                # To lock profit (BE+), SL must be < Entry.
                # So we want SL = Entry - Buffer.
                if be_minus < current_sl:
                    secured_sl = be_minus
    except Exception:
        # Fallback to current behavior if calc fails
        secured_sl = current_sl

    ev = apply_trailing_update(
        pos,
        new_sl=float(secured_sl),
        ts_ms=int(ts_ms),
        trailing_distance=float(offset) if offset > 0 else 0.0,
        point_size=0.0,
        clear_future_tp_levels=False,
    )
    if ev is not None:
        try:
            ev.payload["armed_after_tp1"] = 1
            ev.payload["start_reason"] = str(getattr(pos, "trail_after_tp1_reason", "") or "")
            if abs(secured_sl - current_sl) > 1e-9:
                 ev.payload["secure_sl_applied"] = 1
                 ev.payload["secured_sl"] = secured_sl
        except Exception:
            pass
    return ev

def _trail_after_tp1_reason(pos) -> str:
    """
    Best-effort reason string for audit/debug.
    Priority:
      1) pos.trail_after_tp1_reason
      2) pos.signal_payload["trail_after_tp1_reason"]
      3) "NO_REASON"
    """
    try:
        v = getattr(pos, "trail_after_tp1_reason", None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass
    try:
        sp = getattr(pos, "signal_payload", None) or {}
        if isinstance(sp, dict):
            v2 = sp.get("trail_after_tp1_reason")
            if isinstance(v2, str) and v2.strip():
                return v2.strip()
    except Exception:
        pass
    return "NO_REASON"

def _arm_trailing_after_tp1(pos, *, ts_ms: int) -> TradeEvent | None:
    """
    Arms trailing AFTER TP1 (one-time).
    We intentionally do NOT compute a new SL here (that's trade_monitor's job).
    We only:
      - set trailing_started/active via apply_trailing_update
      - optionally clear future TP levels (configurable) for non-rocket profiles
      - store audit fields on pos
    """
    try:
        # audit fields (safe even if dataclass has slots=False; if slots=True we'll add fields in models.py diff)
        try:
            pos.trailing_armed_ts_ms = int(ts_ms)
            pos.trailing_start_reason = _trail_after_tp1_reason(pos)
        except Exception:
            pass

        trail_profile = str(getattr(pos, "trail_profile", "") or (getattr(pos, "signal_payload", None) or {}).get("trail_profile", "")).lower()

        # By default we want "trailing-only after TP1" for non-rocket profiles.
        # For rocket_v1 we must KEEP TP2/TP3 levels to allow "count hits without closing qty".
        clear_tp_default = True
        clear_future_tp_levels = _env_bool("TRAIL_CLEAR_FUTURE_TPS_ON_START", clear_tp_default)
        if trail_profile == "rocket_v1":
            clear_future_tp_levels = False

        # Keep existing trailing params if already set; otherwise pass zeros (trade_monitor uses its own offsets).
        td = float(getattr(pos, "trailing_distance", 0.0) or 0.0)
        pt = float(getattr(pos, "trailing_point", 0.0) or 0.0)

        # IMPORTANT: new_sl is set to current SL => no immediate SL move here.
        return apply_trailing_update(
            pos,
            new_sl=float(pos.sl),
            ts_ms=int(ts_ms),
            trailing_distance=float(td) if math.isfinite(td) and td > 0 else 0.0,
            point_size=float(pt) if math.isfinite(pt) and pt > 0 else 0.0,
            clear_future_tp_levels=bool(clear_future_tp_levels),
        )
    except Exception:
        return None

def _rocket_trailing_only_mode(pos, *, is_rocket_trail: bool, idx: int) -> bool:
    """
    Rocket v1 special behavior:
      after TP1, if trailing is active -> do NOT close further partial TP volume,
      just count TP hits for reporting.

    With conditional trailing enabled, we must NOT enter this mode unless
    trailing-after-TP1 is allowed by policy (_should_start_trailing_after_tp1()).
    """
    if not is_rocket_trail:
        return False
    if not bool(getattr(pos, "trailing_started", False)):
        return False
    if idx < 1:
        return False
    return _should_start_trailing_after_tp1(pos)


def _safe_call_apply_trailing_update(
    pos: Any,
    *,
    spec: Any,
    tick: Any,
    px: float,
) -> bool:
    """
    Calls apply_trailing_update() if it exists, but does not assume its signature.

    Why:
      - You said apply_trailing_update() is implemented and used elsewhere,
        but process_tick does not call it after TP1.
      - We must reuse the existing trailing state machine (locks, stop calc, etc.)
        instead of setting flags manually (that would be dangerous / inconsistent).

    Contract:
      - Returns True if call succeeded.
      - Returns False if function is missing OR signature mismatch.
      - Never raises (fail-open for the trading loop stability).
    """
    fn = globals().get("apply_trailing_update", None)
    if not callable(fn):
        return False
    # Try common kw signatures first (cheapest and safest).
    kw_tries = [
        {"pos": pos, "spec": spec, "tick": tick, "price": px},
        {"pos": pos, "spec": spec, "tick": tick, "mid": px},
        {"pos": pos, "tick": tick, "price": px},
        {"pos": pos, "tick": tick, "mid": px},
        {"pos": pos, "price": px},
        {"pos": pos, "mid": px},
        {"pos": pos},
    ]
    for kw in kw_tries:
        try:
            fn(**kw)
            return True
        except TypeError:
            # signature mismatch; try next variant
            pass
        except Exception:
            # trailing code must not crash tick loop; treat as "handled"
            return True
    # Then try positional fallbacks.
    arg_tries = [
        (pos, spec, tick, px),
        (pos, tick, px),
        (pos, px),
        (pos,),
    ]
    for args in arg_tries:
        try:
            fn(*args)
            return True
        except TypeError:
            pass
        except Exception:
            return True
    return False


def _maybe_start_trailing_after_tp1(
    pos: Any,
    *,
    spec: Any,
    tick: Any,
    px: float,
) -> None:
    """
    Called right after TP1 is hit.

    Important: previously this branch contained `pass`, so trailing never started
    from process_tick even when FORCE_TRAIL_AFTER_TP1 was enabled.

    Behavior:
      - If policy disallows trailing-after-TP1 -> explicitly mark trailing as inactive.
      - If policy allows -> call apply_trailing_update() to start trailing properly.
      - If apply_trailing_update() is not available / cannot be called -> do NOT
        silently set trailing flags (unsafe). Mark auditable skip reason instead.
    """
    try:
        if getattr(pos, "trailing_started", False):
            return
        # TP1 must be real.
        if not getattr(pos, "tp1_hit", False):
            return
        if not _should_start_trailing_after_tp1(pos):
            try:
                pos.trailing_active = False
                pos.trailing_skip_reason = "POLICY_VETO_AFTER_TP1"
            except Exception:
                pass
            return
        ok = _safe_call_apply_trailing_update(pos, spec=spec, tick=tick, px=float(px))
        if not ok:
            # Auditable: trailing requested but could not be started here.
            try:
                pos.trailing_active = False
                pos.trailing_skip_reason = "APPLY_TRAIL_NOT_CALLABLE"
            except Exception:
                pass
    except Exception:
        # fail-open: never break tick processing
        return

def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

def _parse_csv_ints(s: str) -> Sequence[int]:
    out = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        with contextlib.suppress(Exception):
            out.append(int(p))
    return out

_EMP_BUCKETS_MS_CACHE: Sequence[int] | None = None

def _emp_buckets_ms() -> Sequence[int]:
    """
    Buckets are defined in minutes for operator readability.
    Keep this in sync with reader/writer (signals.empirical_levels / stats_aggregator).
    """
    global _EMP_BUCKETS_MS_CACHE
    if _EMP_BUCKETS_MS_CACHE is not None:
        return _EMP_BUCKETS_MS_CACHE
    mins = _parse_csv_ints(os.getenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45"))
    ms = sorted([m * 60_000 for m in mins if m and m > 0])
    _EMP_BUCKETS_MS_CACHE = ms
    return ms

def maybe_snapshot_time_buckets(pos: Any, spec: Any, ts_ms: int) -> None:
    """
    Snapshot MFE/MAE at fixed horizons since entry (fail-open, never throws).

    Important:
      - We snapshot ONLY once per bucket: first tick when elapsed >= bucket_ms.
      - We use excursions accumulated *up to that moment*:
          LONG: favorable=max_price_seen, adverse=min_price_seen
          SHORT: favorable=min_price_seen, adverse=max_price_seen
      - pnl is computed using the same spec.pnl_money signature you use for live MFE/MAE:
          spec.pnl_money(entry_price, price, lot, direction, symbol=pos.symbol)
    """
    try:
        if not _env_bool("EMP_TIME_SNAPSHOTS_WRITE", True):
            return
        entry_ts = int(getattr(pos, "entry_ts_ms", 0) or 0)
        if entry_ts <= 0:
            return
        elapsed = int(ts_ms) - entry_ts
        if elapsed <= 0:
            return
        buckets = _emp_buckets_ms()
        if not buckets:
            return
        tb = getattr(pos, "emp_time_buckets", None)
        if not isinstance(tb, dict):
            # fail-open: create on the fly if missing
            tb = {}
            try:
                pos.emp_time_buckets = tb
            except Exception:
                return

        direction = getattr(pos, "direction", "")
        is_long = str(direction).strip().lower() in {"long", "buy"}

        entry_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
        lot = float(getattr(pos, "lot", 0.0) or 0.0)
        sym = str(getattr(pos, "symbol", "") or "")
        if entry_price <= 0.0 or lot == 0.0:
            return

        # Current excursions accumulated up to now
        max_seen = float(getattr(pos, "max_price_seen", 0.0) or 0.0)
        min_seen = float(getattr(pos, "min_price_seen", 0.0) or 0.0)
        if max_seen <= 0.0 or min_seen <= 0.0:
            return

        favorable_price = max_seen if is_long else min_seen
        adverse_price = min_seen if is_long else max_seen

        for b in buckets:
            try:
                b = int(b)
            except Exception:
                continue
            if b <= 0 or elapsed < b:
                continue
            # snapshot only once per bucket
            if b in tb:
                continue

            try:
                mfe_pnl = float(spec.pnl_money(entry_price, favorable_price, lot, direction, symbol=sym))
                mae_pnl = float(spec.pnl_money(entry_price, adverse_price, lot, direction, symbol=sym))
            except Exception:
                continue

            tb[b] = {
                "ts_ms": int(ts_ms),
                "mfe_pnl": float(mfe_pnl),
                "mae_pnl": float(mae_pnl),
                "mfe_price": float(favorable_price),
                "mae_price": float(adverse_price),
            }
    except Exception:
        return


EPS_QTY = 1e-9


def _baseline_force_close(pos: PositionState, price: float, ts_ms: int, reason: str) -> None:
    """Фиксирует базовый (fixed-exit) сценарий закрытия, если ещё не закрыт."""
    if pos.baseline_closed:
        return
    pos.baseline_closed = True
    pos.baseline_exit_price = float(price)
    pos.baseline_exit_reason = reason
    pos.baseline_exit_ts_ms = int(ts_ms)


def _baseline_update(pos: PositionState, last_price: float, now_ms: int) -> None:
    """
    Обновляет "теневой" baseline-выход (edge входа без трейлинга/менеджмента).
    Закрывает baseline при достижении SL/TP или по времени.
    """
    if pos.baseline_closed:
        return

    try:
        last_price = float(last_price)
    except Exception:
        last_price = 0.0

    mode = (pos.baseline_mode or "tp_sl").lower()
    side_long = pos.is_long()

    if mode == "tp_sl":
        if side_long:
            if pos.baseline_sl > 0 and last_price <= pos.baseline_sl:
                _baseline_force_close(pos, pos.baseline_sl, now_ms, "BASELINE_SL")
                return
            for level, label in (
                (pos.baseline_tp3, "BASELINE_TP3"),
                (pos.baseline_tp2, "BASELINE_TP2"),
                (pos.baseline_tp1, "BASELINE_TP1"),
            ):
                if level > 0 and last_price >= level:
                    _baseline_force_close(pos, level, now_ms, label)
                    return
        else:
            if pos.baseline_sl > 0 and last_price >= pos.baseline_sl:
                _baseline_force_close(pos, pos.baseline_sl, now_ms, "BASELINE_SL")
                return
            for level, label in (
                (pos.baseline_tp3, "BASELINE_TP3"),
                (pos.baseline_tp2, "BASELINE_TP2"),
                (pos.baseline_tp1, "BASELINE_TP1"),
            ):
                if level > 0 and last_price <= level:
                    _baseline_force_close(pos, level, now_ms, label)
                    return

    elif mode == "time":
        horizon_ts = pos.entry_ts_ms + max(0, int(pos.baseline_horizon_ms))
        if now_ms >= horizon_ts:
            _baseline_force_close(pos, last_price, now_ms, "BASELINE_TIME")


def _build_features_snapshot(feats: dict[str, Any]) -> dict[str, Any]:
    """
    Whitelist-based feature snapshot for TradeClosed events.
    Prevents event bloat while keeping critical attribution dimensions.
    """
    whitelist = {
        "delta_z", "dn_usd", "obi", "cvd_slope", "absorption_score",
        "weak_progress", "vwap_pos", "atr_bps", "liq_scale",
        "confidence", "spread_bps_at_entry", "book_age_ms", "slippage_bps_est",
        "scenario", "regime", "tier", "data_health", "expected_slippage_bps"
    },
    if not isinstance(feats, dict):
        return {}

    trimmed = {k: v for k, v in feats.items() if k in whitelist}
    # Limit size of individual values (strings/lists)
    for k, v in trimmed.items():
        if isinstance(v, str) and len(v) > 100:
            trimmed[k] = v[:100] + "..."
        elif isinstance(v, list) and len(v) > 10:
            trimmed[k] = v[:10] + ["..."]
    return trimmed


def create_position(signal: SignalNorm, spec) -> PositionState:
    pos_id = str(uuid.uuid4())

    payload = signal.payload or {}
    # P41 fix: meta_enforce fields are in payload["indicators"], not top-level payload.
    # We extract them once here to use as fallback below.
    _indicators_pl = payload.get("indicators", {}) if isinstance(payload.get("indicators"), dict) else {}
    baseline_mode = (payload.get("baseline_mode") or "tp_sl").lower()
    baseline_horizon_ms = int(float(payload.get("baseline_horizon_ms") or payload.get("baseline_exit_ms") or 0))

    # резервные уровни baseline = исходные SL/TP
    baseline_sl = float(payload.get("baseline_sl") or signal.sl)
    baseline_tp1 = float(payload.get("baseline_tp1") or (signal.tp_levels[0] if len(signal.tp_levels) > 0 else 0.0))
    baseline_tp2 = float(payload.get("baseline_tp2") or (signal.tp_levels[1] if len(signal.tp_levels) > 1 else 0.0))
    baseline_tp3 = float(payload.get("baseline_tp3") or (signal.tp_levels[2] if len(signal.tp_levels) > 2 else 0.0))

    # trailing profile и min_lock_r из payload
    trail_profile = str(
        payload.get("trail_profile")
        or getattr(signal, "trail_profile", "")
        or ""
    ).lower()

    try:
        trailing_min_lock_r = float(payload.get("trailing_min_lock_r") or 0.0)
    except Exception:
        trailing_min_lock_r = 0.0

    # Prefer sized qty from payload (RR fixed-risk), fallback to signal.lot
    try:
        lot_v = float(payload.get("qty") or payload.get("lot") or signal.lot or 0.0)
    except Exception:
        lot_v = float(signal.lot or 0.0)

    pos = PositionState(
        id=pos_id,
        sid=signal.sid,
        strategy=signal.strategy,
        source=signal.source,
        symbol=signal.symbol,
        tf=signal.tf,
        direction=signal.direction,
        entry_price=float(signal.entry_price),
        entry_ts_ms=int(signal.entry_ts_ms),
        lot=float(lot_v),
        qty=float(lot_v),
        quantity=float(lot_v),
        remaining_qty=float(lot_v),
        sl=float(signal.sl),
        tp_levels=[float(x) for x in signal.tp_levels[:3]],
        signal_payload=signal.payload,
        entry_tag=str(signal.entry_tag or ""),
        trail_profile=trail_profile,
        trailing_min_lock_r=trailing_min_lock_r,
        min_lock_price=0.0,  # посчитаем ниже после one_r_money
        baseline_mode=baseline_mode,
        baseline_horizon_ms=baseline_horizon_ms,
        baseline_sl=baseline_sl,
        baseline_tp1=baseline_tp1,
        baseline_tp2=baseline_tp2,
        baseline_tp3=baseline_tp3,

        atr=float(payload.get("atr") or signal.payload.get("atr") or 0.0),
        # AB attribution
        ab_arm=(payload.get("ab_arm") or "A"),
        ab_group=(payload.get("ab_group") or "default"),
        ab_key=(payload.get("ab_key") or ""),
        arm_ver=int(float(payload.get("arm_ver") or 0)),
        entry_regime=(payload.get("regime") or "na"),
        entry_zone_id=(payload.get("zone_id") or ""),

        # P41 Native Meta Fields
        # NOTE: meta_enforce_cov_bucket is in payload["indicators"] (not top-level payload).
        # We first look at top-level (for future-proofing), then fall back to indicators dict.
        meta_enforce_cov_bucket=str(
            payload.get(MetaKeys.ENFORCE_COV_BUCKET)
            or _indicators_pl.get(MetaKeys.ENFORCE_COV_BUCKET)
            or ""
        ),
        meta_enforce_applied=int(
            payload.get(MetaKeys.ENFORCE_APPLIED)
            if payload.get(MetaKeys.ENFORCE_APPLIED) is not None
            else (
                _indicators_pl.get(MetaKeys.ENFORCE_APPLIED)
                if _indicators_pl.get(MetaKeys.ENFORCE_APPLIED) is not None
                else -1
            )
        )
    )

    # ---- Defensive: Recover Lot from Risk USD if Lot is 0 (Fix Zero PnL) ----
    # FIX: Two-stage recovery to avoid PnL=0.0 anomaly.
    # Stage 1: recover from risk_usd in payload (preferred)
    # Stage 2: fallback to DEFAULT_LOT ENV (last resort, emits WARNING)
    if pos.lot <= 1e-9:
        recovered = False
        try:
            r_usd_payload = float(payload.get("risk_usd") or 0.0)
            if r_usd_payload > 0:
                r_per_lot = float(spec.risk_money(pos.entry_price, pos.sl, 1.0, pos.direction, symbol=pos.symbol) or 0.0)
                if r_per_lot > 1e-9:
                    rec_lot = r_usd_payload / r_per_lot
                    pos.lot = rec_lot
                    pos.remaining_qty = rec_lot
                    recovered = True
                    logger.info(
                        "🔧 lot=0 recovered from risk_usd for %s: lot=%.6f (risk_usd=%.2f)",
                        pos.symbol, rec_lot, r_usd_payload,
                    )
        except Exception:
            pass

        # Stage 2: try ENV DEFAULT_LOT (last resort)
        if not recovered:
            try:
                default_lot = float(os.getenv("DEFAULT_LOT", "0") or 0)
                if default_lot > 1e-9:
                    pos.lot = default_lot
                    pos.remaining_qty = default_lot
                    logger.warning(
                        "⚠️ lot=0 and risk_usd=0 for %s sid=%s — using DEFAULT_LOT=%.4f. "
                        "Check signal publisher: qty/lot and risk_usd fields are both absent!",
                        pos.symbol, pos.sid, default_lot,
                    )
                else:
                    logger.warning(
                        "⚠️ lot=0 and no recovery possible for %s sid=%s — PnL will be 0.0. "
                        "Check signal publisher: qty/lot and risk_usd fields are both absent!",
                        pos.symbol, pos.sid,
                    )
            except Exception:
                pass

    # ---- excursions init (price space, side-aware semantics) ----
    pos.max_favorable_price = pos.entry_price
    pos.max_adverse_price = pos.entry_price
    pos.max_favorable_ts_ms = pos.entry_ts_ms
    pos.max_adverse_ts_ms = pos.entry_ts_ms

    # ---- P0 entry metadata (Expert Recommendation) ----
    payload = signal.payload or {}
    pos.p0_signal_id = str(payload.get("signal_id") or payload.get("id") or signal.sid)
    pos.p0_regime = str(payload.get("regime") or getattr(signal, "regime", None) or "na") or None
    pos.p0_scenario = str(payload.get("scenario") or getattr(signal, "scenario", None) or "na") or None
    pos.p0_session = str(payload.get("session") or getattr(signal, "session", None) or "") or None
    if not pos.p0_session:
        pos.p0_session = session_from_ts_ms(pos.entry_ts_ms)
    pos.p0_entry_reason = str(payload.get("entry_reason") or getattr(signal, "entry_reason", None) or signal.entry_tag or "na") or None

    # Deep metadata from indicators/features
    feats = payload.get("indicators") or payload.get("features") or payload or {}
    # Entry-time cost snapshot for LCB/analytics
    pos.p0_spread_bps_at_entry = float(feats.get("spread_bps") or feats.get("spread_bps_at_entry") or 0.0)
    pos.p0_slippage_bps_est = float(feats.get("slippage_bps_est") or feats.get("expected_slippage_bps") or 0.0)
    pos.p0_book_age_ms = int(feats.get("book_age_ms") or feats.get("ob_age_ms") or 0) or None
    pos.p0_features_snapshot = _build_features_snapshot(feats)

    # Preserve full signal payload for later attribution ...
    try:
        if getattr(pos, "signal_payload", None) is None:
            pos.signal_payload = {}
        if isinstance(signal.payload, dict):
            pos.signal_payload.update(signal.payload)
    except Exception:
        pass

    # --- Guarantee risk_money / risk_usd for evaluators (R-multiple) ---
    # Ensure risk_usd exists (use spec.risk_money for correctness across instruments)
    try:
        r_usd = float(getattr(pos, "risk_usd", 0.0) or 0.0)
        if r_usd <= 0.0:
            r_usd = float(spec.risk_money(pos.entry_price, pos.sl, pos.lot, pos.direction, pos.symbol) or 0.0)

        pos.risk_usd = r_usd

        # Merge back into signal_payload for Redis logging (TradeEventsLogger expansion)
        if isinstance(pos.signal_payload, dict):
            pos.signal_payload["risk_usd"] = r_usd
            # Ensure AB fields are flat for TradeMonitor/Logger
            pos.signal_payload["ab_arm"] = pos.ab_arm
            pos.signal_payload["ab_group"] = pos.ab_group
            pos.signal_payload["ab_key"] = pos.ab_key
            pos.signal_payload["arm_ver"] = pos.arm_ver
    except Exception:
        pass


    # --- Persist AB routing + entry_id + decision-time ctx into signal_payload (no schema changes) ---
    try:
        sp = getattr(pos, "signal_payload", None)
        if isinstance(getattr(signal, "payload", None), dict):
            sp = sp if isinstance(sp, dict) else {}
            pl = signal.payload

            # Core AB routing + entry_id for decision chain tracking
            for k in ("entry_id", "ab_arm", "ab_group", "ab_key"):
                if k in pl and k not in sp:
                    sp[k] = pl.get(k)

            # Copy ctx/of fields used by LCB threshold evaluator
            ctx = pl.get("ctx") if isinstance(pl.get("ctx"), dict) else {}
            of_dict = pl.get("of") if isinstance(pl.get("of"), dict) else {}
            zone = pl.get("zone") if isinstance(pl.get("zone"), dict) else {}

            # Decision-time features for threshold optimization
            for k in ("regime", "scenario", "zone_dist_bp", "obi_stable_sec", "iceberg_strict", "spread_z"):
                if k in ctx and k not in sp:
                    sp[k] = ctx.get(k)

            # OF confirmation score (critical for entry quality)
            if "of_confirm_score" in of_dict and "of_confirm_score" not in sp:
                sp["of_confirm_score"] = of_dict.get("of_confirm_score")

            # Zone distance fallback (if not in ctx)
            if "dist_bp" in zone and "zone_dist_bp" not in sp:
                sp["zone_dist_bp"] = zone.get("dist_bp")

            pos.signal_payload = sp
    except Exception:
        pass

    # ------------------------------------------------------------------
    # NEW: propagate execution dims from signal payload into PositionState
    # (dynamic attrs; no dataclass schema change needed)
    #
    # Why:
    #   - TradeClosed has no "kind"/"venue"/"confidence" fields by schema,
    #     but you already rely on dynamic fields being persisted via __dict__.
    #   - Needed for:
    #       * slipema:v2 (venue/kind)
    #       * reliability curves (confidence, kind, regime)
    # ------------------------------------------------------------------
    try:
        pl = signal.payload or {}
        # kind: ctx.kind -> outbox -> normalize_signal(payload) -> PositionState
        pos.kind = str(pl.get("kind") or pl.get("signal_kind") or signal.strategy or "na").lower()
    except Exception:
        pass
    try:
        pl = signal.payload or {}
        pos.venue = (pl.get("venue") or "na").lower()
    except Exception:
        pass
    try:
        pl = signal.payload or {}
        # confidence can be "confidence" or "conf" depending on emitters
        pos.confidence = float(pl.get("confidence") or pl.get("conf") or pl.get("conf_pct") or 0.0)
    except Exception:
        pass
    try:
        pl = signal.payload or {}
        # regime at entry (if present)
        if "entry_regime" in pl:
            pos.entry_regime = (pl.get("entry_regime") or "na")
        elif "regime" in pl:
            pos.entry_regime = (pl.get("regime") or "na")
    except Exception:
        pass
    try:
        pl = signal.payload or {}
        # AB metadata (V2)
        if "ab_arm" in pl:
             pos.ab_arm = (pl.get("ab_arm") or "A")
        # regime is usually entry_regime, but we capture the precise tag if present
        if "regime" in pl:
             pos.regime = (pl.get("regime") or "na")
    except Exception:
        pass

    # инициализация экскурсий (MFE/MAE)
    pos.max_price_seen = pos.entry_price
    pos.min_price_seen = pos.entry_price
    pos.max_favorable_price = pos.entry_price
    pos.max_favorable_ts = pos.entry_ts_ms

    # 1R (риск)
    try:
        pos.one_r_money = float(spec.risk_money(pos.entry_price, pos.sl, pos.lot, pos.direction, symbol=pos.symbol))
    except Exception:
        pos.one_r_money = 0.0

    # R в ценовых единицах (на контракт)
    try:
        risk_price = abs(pos.entry_price - pos.sl)
    except Exception:
        risk_price = 0.0

    # Расчет min_lock_price, если задан trailing_min_lock_r
    if pos.trailing_min_lock_r > 1e-9 and risk_price > 0:
        lock_r = float(pos.trailing_min_lock_r)
        if pos.is_long():
            pos.min_lock_price = pos.entry_price + lock_r * risk_price
        else:
            pos.min_lock_price = pos.entry_price - lock_r * risk_price
    else:
        pos.min_lock_price = 0.0

    # ------------------------------------------------------------------
    # NEW: Conditional trailing flags copied from signal payload.
    # The publisher (crypto handler) sets:
    #   trail_after_tp1: bool
    #   trail_after_tp1_reason: str
    #
    # Fail-open defaults:
    #   - If field missing -> True (legacy behavior preserved).
    # ------------------------------------------------------------------
    try:
        v = getattr(signal, "trail_after_tp1", None)
        if v is None and isinstance(signal.payload, dict):
            v = signal.payload.get("trail_after_tp1")
        if v is None:
            v = getattr(signal, "trail_cond_after_tp1", None)
        pos.trail_after_tp1 = _parse_boolish(v, True)
    except Exception:
        pos.trail_after_tp1 = True

    # is_virtual extraction for PositionState tracking
    try:
        v = payload.get("is_virtual")
        if v is None:
            v = getattr(signal, "is_virtual", False)
        pos.is_virtual = _parse_boolish(v, False)
    except Exception:
        pos.is_virtual = False
    try:
        r = getattr(signal, "trail_after_tp1_reason", None)
        if r is None and isinstance(signal.payload, dict):
            r = signal.payload.get("trail_after_tp1_reason")
        pos.trail_after_tp1_reason = (r or "")
    except Exception:
        pos.trail_after_tp1_reason = ""

    # ------------------------------------------------------------------
    # Autopilot fields: make sure the closed-trade stream has stable dims
    # (symbol/regime/scenario/tier/confirm flags) without requiring
    # schema migrations.
    # FAIL-OPEN: never break position creation due to analytics concerns.
    # ------------------------------------------------------------------
    try:
        if isinstance(pos.signal_payload, dict):
            from core.autopilot_fields import enrich_signal_payload_for_autopilot
            pos.signal_payload = enrich_signal_payload_for_autopilot(pos.signal_payload)
    except Exception:
        pass

    return pos





def apply_trailing_update(
    pos: PositionState,
    new_sl: float,
    ts_ms: int,
    trailing_distance: float = 0.0,
    point_size: float = 0.0,
    clear_future_tp_levels: bool = False,
) -> TradeEvent | None:
    if pos.closed:
        return None

    # --- R-lock логика до изменения pos.sl ---
    trail_profile = str(getattr(pos, "trail_profile", "") or (pos.signal_payload or {}).get("trail_profile", "")).lower()
    first = not pos.trailing_started

    # При первом старте трейла для rocket_v1 считаем/уточняем min_lock_price
    if first and trail_profile == "rocket_v1":
        try:
            if getattr(pos, "trailing_min_lock_r", 0.0) > 1e-9:
                risk_price = abs(pos.entry_price - pos.sl)
                if risk_price > 0:
                    lock_r = float(pos.trailing_min_lock_r)
                    if pos.is_long():
                        lock_price = pos.entry_price + lock_r * risk_price
                        # если раньше было что-то посчитано, берём максимум
                        old = getattr(pos, "min_lock_price", 0.0) or 0.0
                        pos.min_lock_price = max(old, lock_price)
                    else:
                        lock_price = pos.entry_price - lock_r * risk_price
                        old = getattr(pos, "min_lock_price", 0.0) or 0.0
                        # для SHORT — чем ниже, тем лучше (минимум)
                        pos.min_lock_price = lock_price if old == 0.0 else min(old, lock_price)
        except Exception:
            pass

    # Клампинг new_sl по min_lock_price, если он задан
    new_sl_float = float(new_sl)
    try:
        mlp = float(getattr(pos, "min_lock_price", 0.0) or 0.0)
        if mlp > 0 and trail_profile == "rocket_v1":
            if pos.is_long() and new_sl_float < mlp or not pos.is_long() and new_sl_float > mlp:
                new_sl_float = mlp
    except Exception:
        pass

    prev = pos.sl
    pos.sl = new_sl_float

    first = not pos.trailing_started
    if first:
        pos.trailing_started = True
    pos.trailing_active = True

    if trailing_distance > 0:
        pos.trailing_distance = float(trailing_distance)
    if point_size > 0:
        pos.trailing_point = float(point_size)

    if clear_future_tp_levels:
        # оставляем только уже "зачтённые" уровни
        keep = pos.tp_levels[:pos.tp_hits] if pos.tp_hits > 0 else []
        pos.tp_levels = keep

    return TradeEvent(
        event_type="TRAILING_SYNC",
        order_id=pos.id,
        sid=pos.sid,
        strategy=pos.strategy,
        source=pos.source,
        symbol=pos.symbol,
        tf=pos.tf,
        direction=pos.direction,
        ts_ms=ts_ms,
        payload={
            "previous_sl": prev,
            "new_sl": pos.sl,
            "trailing_distance": pos.trailing_distance,
            "point_size": pos.trailing_point,
            "clear_future_tp_levels": int(clear_future_tp_levels),
        },
    )


def process_tick(
    pos: PositionState,
    tick: Tick,
    spec,
    tp_ratios: Sequence[float],
    fill_policy: str = "level",  # "level" или "tick"
) -> tuple[list[TradeEvent], TradeClosed | None]:
    """
    Возвращает (events, closed_trade_or_none)
    """
    if pos.closed:
        return [], None

    events: list[TradeEvent] = []

    # 1) O(1) Update of excursions and adverse move probes (Expert Recommendation)
    _update_excursions_and_adverse(pos, tick)

    tp_px, sl_px, mid = trigger_prices(tick, pos.direction)
    ts_ms = int(tick.ts_ms)

    # Note: Money PnL (pos.mfe_pnl / pos.mae_pnl) is now calculated pure domain
    try:
        if pos.is_long():
            mfe_price = pos.max_favorable_price
            mae_price = pos.max_adverse_price
        else:
            mfe_price = pos.max_favorable_price
            mae_price = pos.max_adverse_price

        pos.mfe_pnl = float(spec.pnl_money(pos.entry_price, mfe_price, pos.lot, pos.direction, symbol=pos.symbol))
        pos.mae_pnl = float(spec.pnl_money(pos.entry_price, mae_price, pos.lot, pos.direction, symbol=pos.symbol))
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # NEW: Time-bucket snapshots for strict MFE@T / MAE@T calibration.
    # This is intentionally fail-open and must never affect trade execution.
    # -------------------------------------------------------------------------
    maybe_snapshot_time_buckets(pos, spec, int(tick.ts_ms))

    # 3) NEW: заморозка экскурсий при первом касании TP1
    # ПРИМЕЧАНИЕ:
    #   - Временные метки касания TP живут в pos.tp_fill_times (dict {level:int -> ts_ms}).
    #   - Мы также проставляем pos.tp1_hit_ts_ms для downstram-сервисов, ожидающих плоское поле.
    try:
        tpf = getattr(pos, "tp_fill_times", None)
        if isinstance(tpf, dict):
            ts1 = tpf.get(1)
            if ts1 and not getattr(pos, "tp1_hit_ts_ms", None):
                # Снимок экскурсий в точный момент тика, когда был впервые задет TP1.
                snapshot_tp1_excursions(pos, int(ts1))
    except Exception:
        pass

    # теневой baseline-выход (entry-edge)
    last_price = mid or tick.price or tick.last or tp_px
    _baseline_update(pos, last_price=last_price, now_ms=tick.ts_ms)

    # 1) TP loop (по порядку)
    # Берем trail_profile из pos.trail_profile (если есть) или signal_payload (fallback после recovery)
    trail_profile = str(getattr(pos, "trail_profile", "") or (pos.signal_payload or {}).get("trail_profile", "")).lower()
    is_rocket_trail = (trail_profile == "rocket_v1")

    # -------------------------------------------------------------------------
    # FIX: Direction-aware TP level sanitization.
    #
    # Inverted TP levels (below entry for LONG, above entry for SHORT) cause
    # immediate false "TP" closes at a loss. Root cause: upstream level
    # calculation bugs (e.g. negative stop_dist, config corruption).
    #
    # Impact before fix: 14,035 false TP closes → -$230K systematic loss.
    # This guard is fail-safe: removes bad levels, logs warning, continues.
    # -------------------------------------------------------------------------
    if pos.tp_levels and pos.entry_price > 0:
        _valid_tps = []
        _inverted_count = 0
        for _lp in pos.tp_levels:
            _lp_f = float(_lp)
            if _lp_f <= 0:
                continue
            if pos.is_long() and _lp_f <= pos.entry_price:
                _inverted_count += 1
                continue
            if (not pos.is_long()) and _lp_f >= pos.entry_price:
                _inverted_count += 1
                continue
            _valid_tps.append(_lp_f)
        if _inverted_count > 0:
            logger.warning(
                "⚠️ INVERTED_TP: %s %s removed %d inverted TP levels "
                "(entry=%.6f, original_tps=%s, valid_tps=%s)",
                pos.symbol, pos.direction, _inverted_count,
                pos.entry_price, pos.tp_levels, _valid_tps,
            )
            pos.tp_levels = _valid_tps

    while (not pos.closed) and pos.remaining_qty > EPS_QTY and pos.tp_hits < len(pos.tp_levels):
        idx = pos.tp_hits  # 0..2
        level_price = float(pos.tp_levels[idx])

        reached = (tp_px >= level_price) if pos.is_long() else (tp_px <= level_price)
        if not reached:
            break

        # Rocket v1: после TP1 трейлим без дальнейших частичных TP (но учитываем хиты для отчёта)
        if _rocket_trailing_only_mode(pos, is_rocket_trail=is_rocket_trail, idx=idx):
            # фиксируем hit без закрытия объёма
            pos.tp_hits = idx + 1
            tp_level = idx + 1
            if tp_level >= 2:
                pos.tp2_hit = True
            if tp_level >= 3:
                pos.tp3_hit = True
            events.append(TradeEvent(
                event_type="TP_HIT",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=tick.ts_ms,
                payload={
                    "tp_level": tp_level,
                    "tp_price": level_price,
                    "fill_price": level_price,
                    "closed_qty": 0.0,
                    "remaining_qty": pos.remaining_qty,
                    "pnl_part_gross": 0.0,
                    "tp_hits": pos.tp_hits,
                    "trailing_only": 1,
                    # audit: explain why rocket trailing-only is active
                    "trail_after_tp1": 1 if _should_start_trailing_after_tp1(pos) else 0,
                },
            ))
            continue

        ratio = float(tp_ratios[idx]) if idx < len(tp_ratios) else 0.0
        if ratio <= 0:
            # fallback: равными долями остатка
            rem_parts = max(1, len(pos.tp_levels) - pos.tp_hits)
            ratio = 1.0 / rem_parts

        is_last_tp = (idx == (len(pos.tp_levels) - 1))
        close_qty = pos.remaining_qty if is_last_tp else min(pos.remaining_qty, pos.lot * ratio)
        # fill_price нужен и в "нормальной" ветке, и в fail-safe ветке (close_qty слишком мал)
        fill_price = level_price if fill_policy == "level" else tp_px

        if close_qty <= EPS_QTY:
            # ------------------------------------------------------------------
            # Fail-safe: защита от зависания на TP уровне при микроскопическом остатке.
            #
            # Раньше здесь был только pos.tp_hits += 1 и continue:
            #   - TP1 мог быть "пропущен" без tp_fill_times/tp1_hit
            #   - trailing мог не армиться при TP1 (rocket_v1 тогда не переключается в trailing-only)
            #   - snapshot_tp1_excursions() мог не сработать (нет tp_fill_times[1])
            #
            # Теперь мы считаем это "TP_HIT без исполнения объёма":
            #   - фиксируем hit + времена/цены
            #   - для TP1 — пробуем армить трейл (conditional policy)
            #   - эмитим TP_HIT event с closed_qty=0
            # ------------------------------------------------------------------
            tp_level = idx + 1
            pos.tp_hits += 1
            try:
                pos.tp_fill_prices[tp_level] = float(fill_price)
                pos.tp_fill_times[tp_level] = int(tick.ts_ms)
            except Exception:
                pass
            if tp_level >= 1:
                pos.tp1_hit = True
            if tp_level >= 2:
                pos.tp2_hit = True
            if tp_level >= 3:
                pos.tp3_hit = True

            # Conditional trailing arm right after TP1 (even in fail-safe path)
            if tp_level == 1 and pos.tp1_hit and not pos.trailing_started:
                ev_tr = maybe_arm_trailing_after_tp1(pos, spec=spec, ts_ms=int(tick.ts_ms))
                if ev_tr is not None:
                    with contextlib.suppress(Exception):
                        events.append(ev_tr)

            events.append(TradeEvent(
                event_type="TP_HIT",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=tick.ts_ms,
                payload={
                    "tp_level": tp_level,
                    "tp_price": level_price,
                    "fill_price": float(fill_price),
                    "closed_qty": 0.0,  # fail-safe: no volume closed
                    "remaining_qty": float(pos.remaining_qty),
                    "pnl_part_gross": 0.0,
                    "tp_hits": int(pos.tp_hits),
                    # audit (doesn't affect execution)
                    "trail_after_tp1": 1 if bool(getattr(pos, "trail_after_tp1", True)) else 0,
                    "trail_after_tp1_reason": str(getattr(pos, "trail_after_tp1_reason", "") or "")[:256],
                },
            ))
            continue

        pnl_part = float(spec.pnl_money(pos.entry_price, fill_price, close_qty, pos.direction, symbol=pos.symbol))
        pos.realized_pnl_gross += pnl_part
        pos.remaining_qty -= close_qty

        tp_level = idx + 1
        pos.tp_hits += 1
        pos.tp_fill_prices[tp_level] = fill_price
        pos.tp_fill_times[tp_level] = tick.ts_ms

        if tp_level >= 1:
            pos.tp1_hit = True
        if tp_level >= 2:
            pos.tp2_hit = True
        if tp_level >= 3:
            pos.tp3_hit = True

        # ------------------------------------------------------------------
        # NEW: start trailing after TP1 (conditional).
        #
        # Previously this branch had "pass", so even when policy allowed trailing,
        # trailing never actually started here. That broke:
        #   - FORCE_TRAIL_AFTER_TP1 semantics
        #   - conditional trailing (trail_after_tp1)
        #   - rocket_v1 behavior that relies on trailing_started to switch to "trailing-only"
        #
        # We arm trailing via apply_trailing_update() without moving SL immediately.
        # Further SL updates remain in trade_monitor.py (existing behavior).
        # ------------------------------------------------------------------
        # ------------------------------------------------------------------
        # Conditional trailing arm right after TP1.
        #
        # Previously this block had 'pass' which meant:
        #   - _should_start_trailing_after_tp1() existed, but had no effect
        #   - rocket_v1 never switched into "trailing-only" TP2/TP3 mode
        #   - TradeMonitor trailing logic might never start (unless started elsewhere)
        #
        # Now we arm trailing safely:
        #   - No immediate SL move (new_sl == current sl)
        #   - Sets trailing_started/trailing_active and records audit fields
        #   - Emits TRAILING_SYNC or TRAILING_SKIPPED event
        # ------------------------------------------------------------------
        if tp_level == 1 and pos.tp1_hit and not pos.trailing_started:
            ev_tr = maybe_arm_trailing_after_tp1(pos, spec=spec, ts_ms=int(tick.ts_ms))
            if ev_tr is not None:
                with contextlib.suppress(Exception):
                    events.append(ev_tr)

        events.append(TradeEvent(
            event_type="TP_HIT",
            order_id=pos.id,
            sid=pos.sid,
            strategy=pos.strategy,
            source=pos.source,
            symbol=pos.symbol,
            tf=pos.tf,
            direction=pos.direction,
            ts_ms=tick.ts_ms,
            payload={
                "tp_level": tp_level,
                "tp_price": level_price,
                "fill_price": fill_price,
                "closed_qty": close_qty,
                "remaining_qty": pos.remaining_qty,
                "pnl_part_gross": pnl_part,
                "tp_hits": pos.tp_hits,
                # audit (safe for downstream; doesn't affect execution)
                "trail_after_tp1": 1 if bool(getattr(pos, "trail_after_tp1", True)) else 0,
                "trail_after_tp1_reason": str(getattr(pos, "trail_after_tp1_reason", "") or "")[:256],
            },
        ))

        # -----------------------------------------------------------------
        # NEW: execution snapshot at close tick (mid/spread).
        #
        # Эти поля нужны, чтобы finalize_trade мог вычислить:
        #   - realized_slippage_bps = |exit_price - mid| / mid * 10000
        #   - realized_spread_bps   = (ask-bid)/mid*10000 (если есть bid/ask)
        #
        # Fail-open:
        #   - если bid/ask нет -> spread_bps=0
        #   - если mid=0       -> slippage будет пропущен на этапе записи статистики
        # -----------------------------------------------------------------
        try:
            pos.exit_mid_price = float(mid or 0.0)
            pos.exit_spread_bps = float(_compute_spread_bps_from_tick(tick, float(mid or 0.0)))
        except Exception:
            pass

        # если закрыли всё — финал (кроме rocket_v1 после запуска трейлинга)
        if (pos.remaining_qty <= EPS_QTY or pos.tp_hits >= 3) and not (is_rocket_trail and pos.trailing_started):
            pos.closed = True
            pos.exit_ts_ms = tick.ts_ms
            pos.exit_price = fill_price
            closed = finalize_trade(
                pos, spec, exit_price=fill_price, exit_ts_ms=tick.ts_ms,
                close_reason_raw=f"TP{tp_level}",
                tp_ratios=tp_ratios,
            )
            events.append(TradeEvent(
                event_type="CLOSE",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=tick.ts_ms,
                payload={"reason": closed.close_reason, "reason_raw": closed.close_reason_raw},
            ))
            return events, closed

    # 2) Trailing move (если активен)
    if (not pos.closed) and pos.trailing_active and pos.trailing_distance > 0:
        new_sl = calc_trailing_sl(pos.direction, mid, pos.trailing_distance, pos.trailing_point, pos.sl)
        if new_sl is not None:
            prev = pos.sl
            pos.sl = float(new_sl)
            pos.trailing_moves_count += 1
            events.append(TradeEvent(
                event_type="TRAILING_MOVE",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=tick.ts_ms,
                payload={"previous_sl": prev, "new_sl": pos.sl, "price": mid, "moves": pos.trailing_moves_count},
            ))

    # -------------------------------------------------------------------------
    # NEW: TIME_BE_EXIT Policy Check
    # -------------------------------------------------------------------------
    if (not pos.closed) and pos.remaining_qty > EPS_QTY:
        _time_be_cfg = globals().get("_TIME_BE_EXIT_CFG")
        if _time_be_cfg is None:
            from services.time_be_exit_policy import load_time_be_exit_config
            _time_be_cfg = load_time_be_exit_config()
            globals()["_TIME_BE_EXIT_CFG"] = _time_be_cfg

        if _time_be_cfg.enabled and pos.entry_price > 0 and mid > 0:
            is_long = pos.is_long()
            if is_long:
                pnl_gross_bps = (mid - pos.entry_price) / pos.entry_price * 10000.0
            else:
                pnl_gross_bps = (pos.entry_price - mid) / pos.entry_price * 10000.0

            comm_rate = getattr(spec, "commission_rate", 0.0005)
            fees_bps = comm_rate * 2.0 * 10000.0
            slip_bps = 0.5  # conservative estimation
            pnl_net_bps = pnl_gross_bps - fees_bps - slip_bps

            from services.time_be_exit_policy import should_time_be_exit
            should_close, reason_code, mode = should_time_be_exit(
                pos, int(tick.ts_ms), pnl_net_bps, int(getattr(pos, "last_update_ts_ms", tick.ts_ms) or tick.ts_ms), _time_be_cfg
            )

            if should_close:
                exit_price = float(mid)
                pnl_rest = float(spec.pnl_money(pos.entry_price, exit_price, pos.remaining_qty, pos.direction, symbol=pos.symbol))
                pos.realized_pnl_gross += pnl_rest

                try:
                    pos.exit_mid_price = float(mid or 0.0)
                    pos.exit_spread_bps = float(_compute_spread_bps_from_tick(tick, float(mid or 0.0)))
                except Exception:
                    pass

                pos.closed = True
                pos.exit_ts_ms = tick.ts_ms
                pos.exit_price = exit_price

                closed = finalize_trade(
                    pos, spec, exit_price=exit_price, exit_ts_ms=tick.ts_ms,
                    close_reason_raw=reason_code,
                    tp_ratios=tp_ratios,
                )

                events.append(TradeEvent(
                    event_type="TIME_BE_EXIT",
                    order_id=pos.id,
                    sid=pos.sid,
                    strategy=pos.strategy,
                    source=pos.source,
                    symbol=pos.symbol,
                    tf=pos.tf,
                    direction=pos.direction,
                    ts_ms=tick.ts_ms,
                    payload={
                        "exit_price": exit_price,
                        "remaining_qty_closed": pos.remaining_qty,
                        "reason_raw": reason_code,
                        "pnl_net_bps": pnl_net_bps,
                    },
                ))
                events.append(TradeEvent(
                    event_type="CLOSE",
                    order_id=pos.id,
                    sid=pos.sid,
                    strategy=pos.strategy,
                    source=pos.source,
                    symbol=pos.symbol,
                    tf=pos.tf,
                    direction=pos.direction,
                    ts_ms=tick.ts_ms,
                    payload={"reason": closed.close_reason, "reason_raw": closed.close_reason_raw},
                ))
                return events, closed
            elif reason_code.endswith("_SHADOW"):
                events.append(TradeEvent(
                    event_type="TIME_BE_EXIT_SHADOW",
                    order_id=pos.id,
                    sid=pos.sid,
                    strategy=pos.strategy,
                    source=pos.source,
                    symbol=pos.symbol,
                    tf=pos.tf,
                    direction=pos.direction,
                    ts_ms=tick.ts_ms,
                    payload={
                        "reason_raw": reason_code,
                        "pnl_net_bps": pnl_net_bps,
                    },
                ))

    # 3) SL check (по trigger цене)
    if (not pos.closed) and pos.remaining_qty > EPS_QTY:
        hit_sl = (sl_px <= pos.sl) if pos.is_long() else (sl_px >= pos.sl)
        if hit_sl:
            exit_price = float(pos.sl)  # исполнение по уровню SL (консервативно)
            pnl_rest = float(spec.pnl_money(pos.entry_price, exit_price, pos.remaining_qty, pos.direction, symbol=pos.symbol))
            pos.realized_pnl_gross += pnl_rest

            # -----------------------------------------------------------------
            # NEW: execution snapshot at close tick (mid/spread) for SL.
            # -----------------------------------------------------------------
            try:
                pos.exit_mid_price = float(mid or 0.0)
                pos.exit_spread_bps = float(_compute_spread_bps_from_tick(tick, float(mid or 0.0)))
            except Exception:
                pass

            raw = "TRAILING_STOP" if pos.trailing_active else "SL"
            if pos.tp_hits > 0:
                raw = f"SL_AFTER_TP{pos.tp_hits}"

            pos.closed = True
            pos.exit_ts_ms = tick.ts_ms
            pos.exit_price = exit_price

            closed = finalize_trade(
                pos, spec, exit_price=exit_price, exit_ts_ms=tick.ts_ms,
                close_reason_raw=raw,
                tp_ratios=tp_ratios,
            )

            events.append(TradeEvent(
                event_type="SL_HIT",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=tick.ts_ms,
                payload={
                    "sl": pos.sl,
                    "exit_price": exit_price,
                    "remaining_qty_closed": pos.remaining_qty,
                    "reason_raw": raw,
                },
            ))
            events.append(TradeEvent(
                event_type="CLOSE",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=tick.ts_ms,
                payload={"reason": closed.close_reason, "reason_raw": closed.close_reason_raw},
            ))
            # после закрытия остаток 0
            pos.remaining_qty = 0.0
            return events, closed

    return events, None


def _build_close_reason_detail(
    *,
    close_reason_raw: str,
    bucket: str,
    trailing_started: bool,
    trailing_active: bool,
    tp_hits: int,
) -> str:
    """
    FIX: Build structured close_reason_detail so consumers have context.
    Previously this field was always "" (default from TradeClosed dataclass),
    making trades:closed stream entries useless for analytics segmentation.

    Examples:
      - TP1 hit, no trailing            -> "TP_PROFIT"
      - SL after TP2 with trailing      -> "TRAILING_STOP"
      - SL after TP1 with BE move       -> "SL_AFTER_TP_TRAILING_STARTED"
      - Pure SL                         -> "STOP_LOSS"
    """
    raw = (close_reason_raw or "").upper().strip()
    bkt = (bucket or "").upper().strip()

    # TP close (no further SL involved)
    if bkt.startswith("TP"):
        return "TP_PROFIT"

    # Trailing stop variants
    if trailing_active and bkt in ("SL", "TRAIL_SL", "MOVED_SL", "TRAILING_STOP"):
        if tp_hits > 0:
            return "TRAILING_STOP"
        return "TRAILING_STOP_NO_TP"

    # SL after TP (trailing armed but not necessarily active)
    if bkt in ("SL", "MOVED_SL") and tp_hits > 0 and trailing_started:
        return "SL_AFTER_TP_TRAILING_STARTED"

    # SL after TP (no trailing)
    if bkt in ("SL",) and tp_hits > 0:
        return "SL_AFTER_TP"

    # Pure stop loss
    if bkt in ("SL", "MOVED_SL"):
        return "STOP_LOSS"

    # Fallback: use raw string (truncated) for traceability
    if raw:
        return raw[:64]
    return bkt or ""


def finalize_trade(
    pos: PositionState,
    spec,
    exit_price: float,
    exit_ts_ms: int,
    close_reason_raw: str,
    tp_ratios: Sequence[float],
) -> TradeClosed:
    # --- Time Sync Defense (Expert Recommendation) ---
    # Ensure causality: entry must happen before or at the same time as exit.
    entry_ts = int(getattr(pos, "entry_ts_ms", 0) or 0)
    if exit_ts_ms < entry_ts:
        skew = entry_ts - exit_ts_ms
        logger.warning(f"🚨 [TIME_SYNC] exit_ts ({exit_ts_ms}) < entry_ts ({entry_ts}) for pos {pos.id} (skew={skew}ms). Clamping exit to entry.")
        exit_ts_ms = entry_ts

    # Calculate PnL for any remaining quantity if it hasn't been closed by process_tick
    # (e.g. for forced closures like orphan timeouts or manual closures)
    if pos.remaining_qty > EPS_QTY:
        try:
            pnl_rest = float(spec.pnl_money(pos.entry_price, exit_price, pos.remaining_qty, pos.direction, symbol=pos.symbol))
            pos.realized_pnl_gross += pnl_rest
            pos.remaining_qty = 0.0
        except Exception as e:
            logger.warning(f"Failed to calculate realization for pos {pos.id}: {e}")


    # ✅ Time contract: hold_ms + quarantine (fail-open)
    from common.trade_report_contract import (
        clamp_one_r_money,
        compute_hold_ms_with_quarantine,
        infer_trailing_started,
        normalize_close_bucket,
    )

    # Try to wire metrics/quarantine if your PositionState carries them (optional)
    metrics = getattr(pos, "metrics", None)
    quarantine = (
        getattr(pos, "time_quarantine", None)
        or getattr(pos, "bad_time_quarantine", None)
        or getattr(pos, "quarantine", None)
    )

    hold_ms, time_quarantined = compute_hold_ms_with_quarantine(
        entry_ts_ms=int(entry_ts),
        exit_ts_ms=int(exit_ts_ms),
        quarantine=quarantine,
        metrics=metrics,
        max_back_ms=int(getattr(spec, "max_time_back_ms", 0) or 0),
        unit_mismatch_guard=True,
    )

    # ------------------------------------------------------------------
    # User Request: Turnover Dual-View (Entry vs Roundtrip)
    # ------------------------------------------------------------------
    contract_size = float(getattr(spec, "contract_size", 1.0) or 1.0)
    notional_usd = float(abs(pos.entry_price * pos.lot * contract_size))
    turnover_entry = notional_usd
    exit_val_usd = float(abs(exit_price * pos.lot * contract_size))
    turnover_roundtrip = turnover_entry + exit_val_usd

    if hasattr(spec, 'calculate_fees'):
        fees = spec.calculate_fees(
            entry_price=pos.entry_price,
            exit_price=exit_price,
            lot=pos.lot,
            side=pos.direction,
            duration_ms=hold_ms,
        )
    else:
        # Fallback: используем pos.fees (если уже установлен) или 0.0
        fees = float(getattr(pos, "fees", 0.0) or 0.0)
        if fees <= 0.0:
            comm_rate = getattr(spec, "commission_rate", None)
            if comm_rate is None:
                comm_rate = 0.0005
            fees = turnover_roundtrip * float(comm_rate)

    # net = gross - fees
    pnl_gross = float(pos.realized_pnl_gross)
    pnl_net = pnl_gross - fees

    # baseline (entry-edge) — закрываем если не закрыт
    if not pos.baseline_closed:
        _baseline_force_close(pos, exit_price, exit_ts_ms, "BASELINE_FORCED_AT_REAL_CLOSE")

    # contract_size = getattr(spec, "contract_size", 1.0) or 1.0 (already defined above)
    baseline_exit_price = pos.baseline_exit_price or exit_price
    baseline_sign = 1.0 if pos.is_long() else -1.0
    baseline_pnl_gross = (baseline_exit_price - pos.entry_price) * baseline_sign * pos.lot * contract_size
    pnl_if_fixed_exit = baseline_pnl_gross - fees
    pos.pnl_if_fixed_exit = pnl_if_fixed_exit

    # группировка причины
    base_bucket = bucket_close_reason(close_reason_raw)

    # ✅ trailing profile (нужен до нормализации bucket)
    try:
        trailing_profile = str(
            getattr(pos, "trail_profile", "") or (pos.signal_payload or {}).get("trail_profile", "")
        )
    except Exception:
        trailing_profile = ""

    trailing_moves = int(getattr(pos, "trailing_moves_count", 0) or 0)
    trailing_active = bool(getattr(pos, "trailing_active", False)) or trailing_moves > 0
    trailing_started = infer_trailing_started(
        trailing_started=bool(getattr(pos, "trailing_started", False)),
        trailing_active=trailing_active,
        trailing_moves=trailing_moves,
        trailing_profile=trailing_profile,
    )

    # if SL was moved (best-effort)
    sl_moved_to_be = bool(getattr(pos, "sl_moved_to_be", False) or getattr(pos, "sl_moved", False))
    try:
        if float(getattr(pos, "min_lock_price", 0.0) or 0.0) > 0.0:
            sl_moved_to_be = True
    except Exception:
        pass

    bucket = normalize_close_bucket(
        close_reason_raw_bucket=(base_bucket or ""),
        pnl_net=float(pnl_net),
        tp_hits=int(pos.tp_hits or 0),
        trailing_started=trailing_started,
        trailing_active=trailing_active,
        sl_moved_to_be=sl_moved_to_be,
        time_quarantined=bool(time_quarantined),
    )

    # ✅ Fix: ensure MFE/MAE PnL are calculated if missing
    if float(pos.mfe_pnl) == 0.0 and float(pos.max_favorable_price or 0.0) != 0.0:
        with contextlib.suppress(Exception):
            pos.mfe_pnl = float(spec.pnl_money(
                float(pos.entry_price),
                float(pos.max_favorable_price),
                float(pos.lot),
                str(pos.direction),
                symbol=str(pos.symbol or "")
            ))

    if float(pos.mae_pnl) == 0.0 and float(getattr(pos, "max_adverse_price", 0.0) or 0.0) != 0.0:
        with contextlib.suppress(Exception):
            pos.mae_pnl = float(spec.pnl_money(
                float(pos.entry_price),
                float(getattr(pos, "max_adverse_price", 0.0)),
                float(pos.lot),
                str(pos.direction),
                symbol=str(pos.symbol or "")
            ))

    # giveback и missed_profit
    giveback = float(pos.mfe_pnl - pnl_gross)
    missed = 0.0
    # Missed(SL_AFTER_TP) логически относится к SL после TP/trailing
    if bucket in ("TRAIL_SL", "MOVED_SL") and int(pos.tp_hits or 0) > 0:
        missed = float(calc_missed_profit(pos, spec, tp_ratios))

    # DEBUG: Log finalized values to diagnose metric calculation issues
    logger.debug(
        f"📊 FINALIZED {pos.id[:8] if pos.id else 'unknown'}: "
        f"bucket={bucket}, pnl_gross={pnl_gross:.2f}, pnl_net={pnl_net:.2f}, "
        f"mfe_pnl={float(pos.mfe_pnl):.2f}, mae_pnl={float(pos.mae_pnl):.2f}, "
        f"giveback={giveback:.2f}, missed={missed:.2f}"
    )

    # ✅ Clamp one_r to avoid exploding R due to tiny risk_usd
    one_r_raw = float(getattr(pos, "one_r_money", 0.0) or 0.0)
    min_risk_usd = float(getattr(spec, "report_min_risk_usd", 1.0) or 1.0)
    fees_risk_mult = float(getattr(spec, "report_fees_risk_mult", 3.0) or 3.0)
    one_r, _clamped = clamp_one_r_money(
        one_r_money=one_r_raw,
        fees_usd=float(fees),
        min_risk_usd=min_risk_usd,
        fees_risk_mult=fees_risk_mult,
        metrics=metrics,
    )
    r_mult = (pnl_net / one_r) if one_r > 1e-12 else 0.0

    dur = int(hold_ms)  # уже вычислено выше
    pct = pnl_pct_simple(pos.direction, pos.entry_price, float(exit_price))

    # Turnover is already calculated above
    # notional_usd = float(abs(pos.entry_price * pos.lot * spec.contract_size))
    # turnover_entry = notional_usd
    # exit_val_usd = float(abs(exit_price * pos.lot * spec.contract_size))
    # turnover_roundtrip = turnover_entry + exit_val_usd

    # If "Fees" include roundtrip, we should see Fee/Turnover_Roundtrip ~ 0.02% (2bps)
    # If using Turnover_Entry, we see Fee/Turnover_Entry ~ 0.04% (4bps)
    # Storing both allows unambiguous reporting.

    closed = TradeClosed(
        # Identity
        order_id=pos.id,
        trade_id=pos.id,
        sid=pos.sid,
        strategy=pos.strategy,
        source=pos.source,
        symbol=pos.symbol,
        tf=pos.tf,
        direction=pos.direction,  # FIX: was missing → all trades defaulted to "LONG"
        side=str(pos.direction),  # FIX: mirror for TradeClosed.side field
        is_virtual=getattr(pos, "is_virtual", False),
        entry_regime=str(getattr(pos, "entry_regime", "na") or "na"),  # FIX: propagate regime

        # times/prices
        entry_ts_ms=pos.entry_ts_ms,
        exit_ts_ms=exit_ts_ms,
        entry_price=pos.entry_price,
        exit_price=exit_price,
        lot=pos.lot,
        notional_usd=notional_usd,

        # pnl
        pnl_net=pnl_net,
        pnl_gross=pnl_gross,
        fees=fees,
        fees_usd=fees,
        pnl_pct=pct,

        # excursions (money)
        mfe_pnl=float(pos.mfe_pnl),
        mae_pnl=float(pos.mae_pnl),
        giveback=giveback,
        missed_profit=missed,
        one_r_money=float(one_r),
        r_multiple=float(r_mult),
        risk_usd=float(one_r),
        r_mult=float(r_mult),
        duration_ms=dur,
        is_final_close=True,

        # User Req 4.3: Turnover variants
        turnover_entry=turnover_entry,
        turnover_roundtrip=turnover_roundtrip,

        pnl_net_baseline=pnl_if_fixed_exit,
        mgmt_edge=(pnl_net - pnl_if_fixed_exit),
        tp1_hit=bool(getattr(pos, "tp1_hit", False)),
        tp2_hit=bool(getattr(pos, "tp2_hit", False)),
        tp3_hit=bool(getattr(pos, "tp3_hit", False)),
        tp_hits=int(getattr(pos, "tp_hits", 0) or 0),
        tp_before_sl=int(pos.tp_hits),
        trailing_started=bool(trailing_started),
        trailing_active=bool(trailing_active),
        trailing_moves=int(trailing_moves),
        close_reason=bucket,
        close_reason_raw=(close_reason_raw or ""),
        # FIX: populate close_reason_detail so trades:closed consumers get structured info.
        # Previously always "", making downstream analytics blind to trailing/TP context.
        close_reason_detail=_build_close_reason_detail(
            close_reason_raw=close_reason_raw,
            bucket=bucket,
            trailing_started=bool(trailing_started),
            trailing_active=bool(trailing_active),
            tp_hits=int(getattr(pos, "tp_hits", 0) or 0),
        ),
        baseline_exit_reason=pos.baseline_exit_reason,
        baseline_exit_ts_ms=int(pos.baseline_exit_ts_ms or exit_ts_ms),
        baseline_exit_price=float(pos.baseline_exit_price or exit_price),
        atr=float(getattr(pos, "atr", 0.0)),
        sl=float(getattr(pos, "sl", 0.0)),
        tp_levels=list(getattr(pos, "tp_levels", [])),
        tp1_price=float(pos.tp_levels[0]) if pos.tp_levels else 0.0,  # #15: guard against empty tp_levels
        entry_tag=str(pos.entry_tag or ""),
        max_favorable_price=float(pos.max_favorable_price or 0.0),
        max_favorable_ts=int(pos.max_favorable_ts or 0),
        remaining_qty=0.0,
        status="CLOSED",
        trailing_profile=trailing_profile,
        trailing_min_lock_r=float(getattr(pos, "trailing_min_lock_r", 0.0)),
        min_lock_price=float(getattr(pos, "min_lock_price", 0.0) or 0.0),
        signal_payload=(pos.signal_payload or {}),
    )

    # -------------------------------------------------------------------------
    # NEW: persist dynamic dims into TradeClosed
    # -------------------------------------------------------------------------
    try:
        closed.kind = str(getattr(pos, "kind", "") or (pos.signal_payload or {}).get("kind") or pos.strategy or "na").lower()
        closed.venue = str(getattr(pos, "venue", "") or (pos.signal_payload or {}).get("venue") or "na").lower()
        closed.confidence = float(getattr(pos, "confidence", 0.0) or (pos.signal_payload or {}).get("confidence") or 0.0)
    except Exception: pass

    # Transfer TP1 hit ts and excursion snapshots
    for name in [
        "tp1_hit_ts_ms", "mfe_pnl_at_tp1", "mae_pnl_before_tp1", "mfe_price_at_tp1",
        "mfe_ts_at_tp1", "mae_price_before_tp1", "mae_ts_before_tp1", "ab_arm"
    ]:
        try:
            v = getattr(pos, name, None)
            if v is not None: setattr(closed, name, v)
        except Exception: pass

    # Retroactive TP touches
    try:
        mfe_price = float(pos.max_favorable_price or 0.0)
        if mfe_price > 0 and pos.tp_levels:
            side_l = pos.is_long()
            for idx, level_price in enumerate(pos.tp_levels[:3]):
                if level_price > 0:
                    touched = (mfe_price >= level_price) if side_l else (mfe_price <= level_price)
                    if touched: setattr(closed, f"tp{idx+1}_touched", True)
    except Exception: pass

    with contextlib.suppress(Exception):
        _attach_nosl_after_tp1_flags(pos, closed, exit_ts_ms=int(exit_ts_ms))

    # -------------------------------------------------------------------------
    # FIX: Populate selected_tp1_price / selected_sl_price from actual position
    # levels. Previously these were ONLY set in _stamp_closed_trade_meta()
    # which was called ONLY for orphan closures, leaving 100% of normal
    # TP/SL closes with selected_tp1_price = 0, selected_sl_price = 0.
    # -------------------------------------------------------------------------
    try:
        if not getattr(closed, "selected_tp1_price", 0.0):
            if pos.tp_levels and len(pos.tp_levels) > 0 and float(pos.tp_levels[0]) > 0:
                closed.selected_tp1_price = float(pos.tp_levels[0])
        if not getattr(closed, "selected_sl_price", 0.0):
            _pos_sl = float(getattr(pos, "sl", 0.0) or 0.0)
            if _pos_sl > 0:
                closed.selected_sl_price = _pos_sl
    except Exception:
        pass

    # ---- FINAL STEP: Expert P0 Attribution Enrichment ----
    return _enrich_closed_from_pos(closed, pos, exit_px=exit_price, now_ms=exit_ts_ms)


def _parse_int_list_env(name: str, default_csv: str) -> list[int]:
    raw = os.getenv(name, default_csv) or default_csv
    out: list[int] = []
    try:
        for p in str(raw).split(","):
            s = p.strip()
            if s:
                v = int(float(s))
                if v > 0: out.append(v)
    except Exception: out = []
    if not out:
        try:
            for p in str(default_csv).split(","):
                s = p.strip()
                if s:
                    v = int(float(s))
                    if v > 0: out.append(v)
        except Exception: return []
    with contextlib.suppress(Exception): out = sorted(set(out))
    return out

def _stop_like_close_reason(reason: str) -> bool:
    rs = (reason or "").strip().upper()
    try:
        allow = os.getenv("NOSL_AFTER_TP1_STOP_REASONS", "SL,TRAILING_STOP") or "SL,TRAILING_STOP"
        allow_set = {x.strip().upper() for x in str(allow).split(",") if x.strip()}
        return rs in allow_set
    except Exception:
        return rs in {"SL", "TRAILING_STOP"}

def _get_tp1_hit_ts_ms(pos: PositionState) -> int:
    ts1 = 0
    try:
        tpf = getattr(pos, "tp_fill_times", None)
        if isinstance(tpf, dict): ts1 = int(tpf.get(1) or 0)
    except Exception: pass
    if ts1 > 0: return ts1
    with contextlib.suppress(Exception): ts1 = int(getattr(pos, "tp1_hit_ts_ms", 0) or 0)
    return ts1

def _attach_nosl_after_tp1_flags(pos: PositionState, closed: Any, *, exit_ts_ms: int) -> None:
    buckets = _parse_int_list_env("NOSL_AFTER_TP1_BUCKETS_MS", "500,2000")
    if not buckets: return

    def _set_bucket_fields(sl_within: bool, *, applicable: bool) -> None:
        for b in buckets:
            setattr(closed, f"sl_within_tp1_t{int(b)}", 1 if sl_within and applicable else 0)
            setattr(closed, f"nosl_after_tp1_t{int(b)}", 1 if applicable and (not sl_within) else 0)

    tp1_hit = bool(getattr(pos, "tp1_hit", False))
    tp1_ts = int(_get_tp1_hit_ts_ms(pos) or 0)
    if tp1_ts > 0: closed.tp1_hit_ts_ms = int(tp1_ts)

    if (not tp1_hit) or tp1_ts <= 0 or int(exit_ts_ms or 0) <= 0:
        closed.nosl_after_tp1_applicable = 0
        closed.sl_after_tp1_elapsed_ms = 0
        _set_bucket_fields(False, applicable=False)
        return

    closed.nosl_after_tp1_applicable = 1
    close_bucket = str(getattr(closed, "close_reason", "") or "").strip()
    stop_like = _stop_like_close_reason(close_bucket)

    if not stop_like:
        closed.sl_after_tp1_elapsed_ms = 0
        for b in buckets:
            setattr(closed, f"sl_within_tp1_t{int(b)}", 0)
            setattr(closed, f"nosl_after_tp1_t{int(b)}", 1)
        return

    elapsed = int(exit_ts_ms) - int(tp1_ts)
    if elapsed < 0: elapsed = 0
    closed.sl_after_tp1_elapsed_ms = int(elapsed)
    for b in buckets:
        bb = int(b)
        within = bool(elapsed > 0 and elapsed <= bb)
        setattr(closed, f"sl_within_tp1_t{bb}", 1 if within else 0)



