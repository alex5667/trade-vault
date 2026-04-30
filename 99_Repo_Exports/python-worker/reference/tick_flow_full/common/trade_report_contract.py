from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def _safe_inc(metrics: Any, name: str, n: int = 1) -> None:
    try:
        if metrics is None:
            return
        inc = getattr(metrics, "inc", None)
        if callable(inc):
            inc(name, n)
            return
        # fail-open: some metrics implement .counter(name).inc()
        counter = getattr(metrics, "counter", None)
        if callable(counter):
            c = counter(name)
            if hasattr(c, "inc"):
                c.inc(n)
    except Exception:
        return


def _safe_quarantine_push(quarantine: Any, reason: str, data: Dict[str, Any]) -> None:
    try:
        if quarantine is None:
            return
        push = getattr(quarantine, "push", None)
        if callable(push):
            push(reason, data)
            return
        add = getattr(quarantine, "add", None)
        if callable(add):
            add(reason, data)
    except Exception:
        return


def compute_hold_ms_with_quarantine(
    *
    entry_ts_ms: int
    exit_ts_ms: int
    quarantine: Any = None
    metrics: Any = None
    max_back_ms: int = 0
    unit_mismatch_guard: bool = True
) -> Tuple[int, bool]:
    """
    Контракт:
      - всегда возвращаем hold_ms >= 0
      - если exit_ts_ms < entry_ts_ms (с учетом max_back_ms) -> quarantined=True
      - если похоже на unit mismatch (sec vs ms) -> quarantined=True с отдельной причиной
    """
    try:
        entry = int(entry_ts_ms)
        exit_ = int(exit_ts_ms)
    except Exception:
        _safe_inc(metrics, "trade.bad_time.quarantined", 1)
        _safe_quarantine_push(quarantine, "ts_not_int", {"entry_ts_ms": entry_ts_ms, "exit_ts_ms": exit_ts_ms})
        return 0, True

    raw = exit_ - entry
    if raw >= 0:
        return raw, False

    # raw < 0
    _safe_inc(metrics, "trade.bad_time.negative_raw", 1)

    # unit mismatch heuristic: exit looks like seconds, entry looks like ms
    # epoch ms ~ 1.7e12+, epoch sec ~ 1.7e9+
    if unit_mismatch_guard and (exit_ < 10_000_000_000 and entry > 100_000_000_000):
        _safe_inc(metrics, "trade.bad_time.unit_mismatch", 1)
        _safe_inc(metrics, "trade.bad_time.quarantined", 1)
        _safe_quarantine_push(
            quarantine
            "ts_unit_mismatch"
            {"entry_ts_ms": entry, "exit_ts_ms": exit_, "raw": raw}
        )
        return 0, True

    if abs(raw) > int(max_back_ms):
        _safe_inc(metrics, "trade.bad_time.exit_before_entry", 1)
        _safe_inc(metrics, "trade.bad_time.quarantined", 1)
        _safe_quarantine_push(
            quarantine
            "exit_before_entry"
            {"entry_ts_ms": entry, "exit_ts_ms": exit_, "raw": raw, "max_back_ms": int(max_back_ms)}
        )
        return 0, True

    # allow small backward jitter but clamp (still signal via metric)
    _safe_inc(metrics, "trade.bad_time.clamped_small_back", 1)
    return 0, False


def normalize_close_bucket(
    *
    close_reason_raw_bucket: str
    pnl_net: float
    tp_hits: int
    trailing_started: bool
    trailing_active: bool
    sl_moved_to_be: bool
    time_quarantined: bool = False
) -> str:
    """
    Нормализованный close_bucket:
      - SL после TP/trailing/moved SL -> TRAIL_SL (не INITIAL_SL)
      - bad-time quarantine -> UNKNOWN
      - прочие причины — pass-through (в верхнем регистре)
    """
    if time_quarantined:
        return "UNKNOWN"

    cr = (close_reason_raw_bucket or "").strip().upper()
    if not cr:
        return "UNKNOWN"

    # normalize common aliases
    if cr in ("STOP", "STOP_LOSS", "STOPLOSS"):
        cr = "SL"
    if cr in ("TAKE", "TAKE_PROFIT", "TAKEPROFIT"):
        cr = "TP"

    if cr == "SL":
        if tp_hits > 0 or trailing_started or trailing_active or sl_moved_to_be or float(pnl_net) > 0.0:
            return "TRAIL_SL"
        return "SL"  # initial SL (loss/BE in most cases)

    return cr


def extract_tp_flags_from_pos(pos: Any) -> Dict[str, Any]:
    return {
        "tp1_hit": bool(getattr(pos, "tp1_hit", False))
        "tp2_hit": bool(getattr(pos, "tp2_hit", False))
        "tp3_hit": bool(getattr(pos, "tp3_hit", False))
        "tp_hits": int(getattr(pos, "tp_hits", 0) or 0)
        "trailing_started": bool(getattr(pos, "trailing_started", False))
        "trailing_active": bool(getattr(pos, "trailing_active", False))
        "trailing_moves": int(getattr(pos, "trailing_moves_count", 0) or 0)
    }


def compute_baseline_pnl_net_usd(
    *
    entry_price: float
    baseline_exit_price: float
    is_long: bool
    lot: float
    contract_size: float
    fees_usd: float
) -> float:
    sign = 1.0 if bool(is_long) else -1.0
    cs = float(contract_size) if float(contract_size) != 0.0 else 1.0
    gross = (float(baseline_exit_price) - float(entry_price)) * sign * float(lot) * cs
    return float(gross) - float(fees_usd)


def clamp_one_r_money(
    *
    one_r_money: float
    fees_usd: float
    min_risk_usd: float
    fees_risk_mult: float
    metrics: Any = None
) -> Tuple[float, bool]:
    """
    one_r_money — ден. риск на 1R. Если он слишком мал, R-метрики становятся мусорными.
    Clamp: one_r_eff >= max(min_risk_usd, fees_usd * fees_risk_mult)
    """
    try:
        one_r = float(one_r_money)
    except Exception:
        one_r = 0.0
    try:
        fees = abs(float(fees_usd))
    except Exception:
        fees = 0.0

    floor = max(float(min_risk_usd), fees * float(fees_risk_mult))
    if one_r < floor:
        _safe_inc(metrics, "trade.risk.one_r_clamped", 1)
        return float(floor), True
    return one_r, False


def infer_trailing_started(
    *
    trailing_started: bool
    trailing_active: bool
    trailing_moves: int
    trailing_profile: str
) -> bool:
    """
    Fix для кейса: rocket_v1 заполнен, но pos.trailing_started=False => trailing_share=0.
    """
    if trailing_started or trailing_active:
        return True
    if int(trailing_moves or 0) > 0:
        return True
    if (trailing_profile or "").strip():
        return True
    return False
