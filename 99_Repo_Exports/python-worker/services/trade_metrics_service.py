from __future__ import annotations

import itertools
import math
import os
from typing import Any

# Module-level call counter for DEBUG TRADE log throttling.
# Using itertools.count avoids GIL-unsafe += on plain int.
_DEBUG_TRADE_CALL_COUNTER = itertools.count(start=1)

from common.log import setup_logger
from services.pnl_math import safe_div

logger = setup_logger("TradeMetricsService")


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return str(v)


def _si(v: Any) -> int:
    try:
        # Handle "True"/"False" strings common in Redis
        if isinstance(v, str):
            low = v.lower()
            if low == "true":
                return 1
            if low == "false":
                return 0
        return int(float(v))
    except Exception:
        return 0


def _sf(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    mid = n // 2
    if n % 2 == 1:
        return ys[mid]
    return ((ys[mid - 1] + ys[mid]) / 2.0)


def _trimmed_mean(xs: list[float], trim_ratio: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    k = int(n * max(0.0, min(0.49, trim_ratio)))
    core = ys[k: n - k] if (n - 2 * k) > 0 else ys
    return (sum(core) / max(1, len(core)))


def _normalize_ts_ms(ts: int) -> int:
    # seconds -> ms
    if ts <= 0:
        return ts
    if 0 < ts < 10_000_000_000:
        return ts * 1000
    # micros -> ms
    if ts > 100_000_000_000_000:
        return ts // 1000
    return ts


def _detect_ts_unit(ts_raw: int) -> str:
    """
    Возвращает:
      - "sec"  если похоже на секунды (1e9..1e10)
      - "us"   если похоже на микросекунды (>1e14)
      - ""     если выглядит как ms или 0/отсутствует
    """
    if ts_raw <= 0:
        return ""
    if 0 < ts_raw < 10_000_000_000:
        return "sec"
    if ts_raw > 100_000_000_000_000:
        return "us"
    return ""


def _var_cvar(xs: list[float], alpha: float) -> tuple[float, float]:
    """
    VaR/CVaR на левом хвосте:
    - VaR(alpha): квантиль alpha
    - CVaR(alpha): среднее worst alpha доли (Expected Shortfall)
    """
    if not xs:
        return 0.0, 0.0
    alpha = max(0.001, min(0.49, alpha))
    ys = sorted(xs)  # ascending (worst first)
    n = len(ys)
    k = max(1, math.ceil(alpha * n))
    tail = ys[:k]
    var = ys[k - 1]  # граница хвоста
    cvar = (sum(tail) / k)
    return var, cvar


class TradeMetricsService:
    """
    Сервис аккумулирования и финализации метрик окна сделок.
    Не ломает существующий формат m: добавляет новые поля, старые оставляет.
    """

    def __init__(self, eps: float = 1e-9):
        self.eps = eps
        self.fees_turnover_huge_threshold = float(
            os.getenv("PERIODIC_REPORT_FEES_TURNOVER_HUGE_THRESHOLD", "0.02")
        )
        self.kelly_cap = float(os.getenv("PERIODIC_REPORT_KELLY_CAP", "1.0"))
        self.enable_debug_series = os.getenv("PERIODIC_REPORT_DEBUG_SERIES", "false").lower() == "true"
        self.trim_ratio = float(os.getenv("PERIODIC_REPORT_TRIM_RATIO", "0.10"))
        self.es_alpha = float(os.getenv("PERIODIC_REPORT_ES_ALPHA", "0.05"))
        self.min_trades_for_es = int(os.getenv("PERIODIC_REPORT_MIN_TRADES_FOR_ES", "20"))
        self.virtual_tp_by_mfe = os.getenv("PERIODIC_REPORT_VIRTUAL_TP_BY_MFE", "false").lower() == "true"

    def new_metrics(self) -> dict[str, Any]:
        m: dict[str, Any] = {
            # --- existing (как у вас) ---
            "total_trades": 0,
            "wins": 0, "losses": 0, "breakeven": 0,
            "wins_strict": 0, "losses_strict": 0, "breakeven_strict": 0,
            "total_pnl": 0.0, "total_pnl_pct": 0.0, "total_fees": 0.0,
            "total_pnl_gross": 0.0,
            # Corrected aggregate (bug 2026-05-14): INITIAL_SL trades had pnl_gross = -2 × one_r
            # due to double-add of realized_pnl_gross. For historical data we approximate the
            # honest pnl_gross as -1 × one_r_money for INITIAL_SL bucket; other buckets unchanged.
            "total_pnl_gross_corrected": 0.0,
            "total_pnl_net_corrected": 0.0,
            "initial_sl_corrected_count": 0,
            "total_notional_usd": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0,
            "tp1_hits": 0, "tp2_hits": 0, "tp3_hits": 0,
            "cnt_tp_hit": 0, "cnt_sl_hit": 0, # count trades that hit ANY TP or ANY SL level
            "tp1_then_sl": 0, "tp2_then_sl": 0, "tp3_then_sl": 0,
            "trailing_started": 0, "trailing_stop_hits": 0,
            "closed_by_trail": 0,  # requested: 1 ⇔ close_bucket == TRAIL_SL
            "sum_duration_ms": 0.0,
            "reasons": {},
            "neg_pnl_count": 0, "min_pnl": float("inf"), "max_pnl": float("-inf"),
            "missing_fees_count": 0, "missing_duration_count": 0,

            # --- edge / returns accumulators ---
            "sum_win_net": 0.0, "cnt_win_net": 0,
            "sum_loss_net": 0.0, "cnt_loss_net": 0,  # loss хранится отрицательной
            "sum_r": 0.0, "sum_r2": 0.0, "cnt_r": 0,
            "sum_win_r": 0.0, "cnt_win_r": 0,
            "sum_loss_r": 0.0, "cnt_loss_r": 0,      # loss_r хранится отрицательной
            "sum_ret": 0.0, "sum_ret2": 0.0, "cnt_ret": 0,
            "sum_down_ret2": 0.0, "cnt_down_ret": 0,  # downside (ret<0) квадраты

            # --- NEW: PF(net) ---
            "profit_factor_net": 0.0,

            # --- NEW: robust center/dispersion ---
            "median_r": 0.0,
            "trimmed_mean_r": 0.0,

            # --- NEW: baseline (pnl_if_fixed_exit) metrics ---
            "total_pnl_if_fixed_exit": 0.0,
            "sum_r_fixed": 0.0, "sum_r_fixed2": 0.0, "cnt_r_fixed": 0,
            "sum_win_r_fixed": 0.0, "cnt_win_r_fixed": 0,
            "sum_loss_r_fixed": 0.0, "cnt_loss_r_fixed": 0,

            # --- execution / exits ---
            "sum_exit_eff_win": 0.0, "cnt_exit_eff_win": 0,
            "sum_giveback_ratio_win": 0.0, "cnt_giveback_ratio_win": 0,
            "sum_missed_profit_ratio": 0.0, "cnt_missed_profit_ratio": 0,

            # --- risk / path dependent ---
            "max_drawdown_usd": 0.0,
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0,

            # --- NEW: tail risk (VaR / CVaR) ---
            "var_ret": 0.0,
            "cvar_ret": 0.0,
            "var_r": 0.0,
            "cvar_r": 0.0,
            "var_pnl": 0.0,
            "cvar_pnl": 0.0,

            # --- data quality ---
            "bad_ts_sec": 0,
            "bad_ts_us": 0,
            "bad_time": 0,  # CRITICAL: exit < entry
            "negative_duration_count": 0,
            "tp_hit_but_zero_pnl": 0,
            "close_reason_inconsistent_with_pnl_sign": 0,
            "fees_huge_count": 0,
            "count_clamped_risk": 0,
            "count_missing_risk": 0,  # trades with one_r_money=0 (R-metrics excluded)
            "invariant_gross_net_fail": 0,
            "invariant_reason_pnl_fail": 0,
            "invariant_tp_sl_fail": 0,


            # trailing profiles aggregation
            "trailing_profiles": {},  # dict[str, int] — распределение профилей трейлинга
            "trailing_profile_stats": {},  # dict[str, dict] - count, wins, pnl
            "regime_stats": {},  # dict[str, dict] - count, wins, pnl
            # trade-profile (router output) aggregation — e.g. range_absorption_v1, trend_breakout_v1
            # Pairs with trailing_profile to debug "rocket_v1 not chosen" vs "rocket_v1 chose but TP1 wrong".
            "trade_profiles": {},  # dict[str, int]
            "trade_profile_stats": {},  # dict[str, dict] - count, wins, pnl, sum_tp1_atr

            # cross-product: pnl_sign x reason
            "wins_by_reason": {},
            "losses_by_reason": {},
            "breakeven_by_reason": {},

            # internal for finalize
            "_series": [],  # list[tuple(ts_ms, pnl_net)]
            "_r_values": [],  # list[float] для median/trimmed mean
            "_ret_values": [],  # list[float] for VaR/CVaR on returns
            "_pnl_values": [],  # list[float] for VaR/CVaR calculations

            # --- NEW: Setup Stats (ATR) ---
            "sum_sl_atr": 0.0, "cnt_sl_atr": 0,
            "sum_tp_atr": 0.0, "cnt_tp_atr": 0,   # TP1 distance (initial target)
            "sum_tp_final_atr": 0.0, "cnt_tp_final_atr": 0,  # furthest TP reached (TP3>TP2>TP1)

            # --- NEW: Scenarios & Gate Stats ---
            "cnt_scenario_reversal": 0, "sum_pnl_scenario_reversal": 0.0,
            "cnt_scenario_continuation": 0, "sum_pnl_scenario_continuation": 0.0,
            "cnt_scenario_none": 0,

            "cnt_gate_enforce": 0,
            "cnt_gate_shadow": 0,
            "cnt_gate_shadow_veto": 0,
            "sum_pnl_shadow_veto": 0.0,

            # --- NEW: Strong vs Weak Stats ---
            "cnt_strong_ok": 0, "sum_pnl_strong_ok": 0.0,
            "cnt_strong_fail": 0, "sum_pnl_strong_fail": 0.0,

            # --- NEW: OF Confirm Stats ---
            "of_confirm_stats": {},

            # --- NEW: ML Performance Stats ---
            "ml_stats": {
                "pass": {"count": 0, "wins": 0, "pnl": 0.0}, # Model allowed
                "veto": {"count": 0, "wins": 0, "pnl": 0.0}, # Model would have vetoed
            },

            # --- NEW: ML Condition Breakdown ---
            "ml_condition_stats": {
                # Per-threshold breakdown (what would pass at different p_edge thresholds)
                "by_threshold": {},  # {"0.50": {"count": 0, "wins": 0, "pnl": 0.0}, ...}
                # Per-scenario breakdown
                "by_scenario": {},  # {"continuation": {"count": 0, "wins": 0, "pnl": 0.0, "sum_p_edge": 0.0, "avg_p_edge": 0.0}, ...}
                # Distribution buckets
                "p_edge_distribution": {},  # {"0.0-0.3": {"count": 0, "wins": 0, "pnl": 0.0}, ...}
                # Summary stats
                "total_evaluated": 0,
                "avg_p_edge": 0.0,
                "median_p_edge": 0.0,
                "_p_edge_values": [],  # internal for median calculation
            },

            # --- NEW: Strong (High Conf) Stats ---
            "strong_high_conf_stats": {},  # {"70": {"count": 0, "wins": 0, "pnl": 0.0}, ...}

            # --- NEW: ok-soft Stats (requested) ---
            "ok_soft_stats": {"count": 0, "wins": 0, "pnl": 0.0},
            "ok_soft_reasons": {},  # Breakdown of soft_fail_reason
            "unmet_ok_reasons": {}, # Breakdown of strong_gate_missing

            # --- NEW: Filtration Stats ---
            "cnt_rejected_low_tp": 0,
            "cnt_veto_gate": 0,
        }
        return m

    def accumulate_trade(self, m: dict[str, Any], t: dict[str, Any]) -> bool:
        import json
        eps = self.eps

        pnl = _sf(t.get("pnl_net") or t.get("pnl") or 0.0)
        pnl_pct = _sf(t.get("pnl_pct") or 0.0)
        fees = _sf(t.get("fees") or 0.0)

        # FIX: Robust pnl_gross calculation
        pnl_gross_raw = t.get("pnl_gross")
        if pnl_gross_raw is not None:
             pnl_gross = _sf(pnl_gross_raw)
        else:
             # Fallback: Gross = Net + Fees (assuming fees are positive cost)
             pnl_gross = pnl + abs(fees)

        # notional
        lot = _sf(t.get("lot") or 0.0)
        notional_usd = _sf(t.get("notional_usd") or 0.0)
        if notional_usd <= 0:
            entry_price = _sf(t.get("entry_price") or 0.0)
            if entry_price > 0 and lot > 0:
                notional_usd = entry_price * lot

        # ts + bad_ts
        entry_ts_raw = _si(t.get("entry_ts_ms") or t.get("open_time") or 0)
        exit_ts_raw = _si(t.get("exit_ts_ms") or t.get("closed_time") or t.get("close_time") or 0)

        # Check units for metrics but do not normalize here for logic if using existing helper
        # Actually we need normalized values to compare
        entry_ts = _normalize_ts_ms(entry_ts_raw)
        exit_ts = _normalize_ts_ms(exit_ts_raw)

        for ts_raw in (entry_ts_raw, exit_ts_raw):
            u = _detect_ts_unit(ts_raw)
            if u == "sec":
                m["bad_ts_sec"] += 1
            elif u == "us":
                m["bad_ts_us"] += 1

        # 🛑 QUARANTINE: Time Travel Check
        # Если сделка "закрылась раньше чем открылась" (из-за кривых часов / лага тиков)
        # мы её полностью исключаем из фин. метрик, чтобы не портить матожидание.
        if exit_ts > 0 and entry_ts > 0 and exit_ts < entry_ts:
            m["bad_time"] += 1
            m["negative_duration_count"] += 1  # compatibility
            return False

        # duration
        dur_raw = _si(t.get("duration_ms") or (exit_ts - entry_ts) if (exit_ts and entry_ts) else 0)
        # Fix negative duration just in case (e.g. 0 diff)
        dur = max(0, dur_raw)
        if dur_raw < 0:
             # Should be caught by quarantine above usually, but if duration_ms was passed explicitly negative:
             m["negative_duration_count"] += 1



        # Resolve MFE early for virtual TP check
        mfe_pnl = _sf(t.get("mfe_pnl") or t.get("mfe_usd") or 0.0)
        mfe_raw = _sf(t.get("mfe") or 0.0)
        if abs(mfe_pnl) <= eps and abs(mfe_raw) > eps and lot > eps:
            mfe_pnl = mfe_raw * lot

        # TP / trailing flags
        # FIX: report "hits" if level was touched or executed
        tp1_hit = _si(t.get("tp1_hit") or 0)
        tp1_touched = _si(t.get("tp1_touched") or 0)
        tp1 = 1 if (tp1_hit > 0 or tp1_touched > 0) else 0

        tp2_hit = _si(t.get("tp2_hit") or 0)
        tp2_touched = _si(t.get("tp2_touched") or 0)
        tp2 = 1 if (tp2_hit > 0 or tp2_touched > 0) else 0

        tp3_hit = _si(t.get("tp3_hit") or 0)
        tp3_touched = _si(t.get("tp3_touched") or 0)
        tp3 = 1 if (tp3_hit > 0 or tp3_touched > 0) else 0
        tp_before_sl = _si(t.get("tp_before_sl") or 0)
        trailing_started = _si(t.get("trailing_started") or 0)

        # Virtual TP by MFE
        if self.virtual_tp_by_mfe:
            _tp2_p = _sf(t.get("tp2_price") or t.get("tp2") or 0.0)
            _tp3_p = _sf(t.get("tp3_price") or t.get("tp3") or 0.0)
            _ep = _sf(t.get("entry_price") or t.get("avg_entry_price") or 0.0)
            
            # Check by price distance if mfe_raw is available
            if abs(mfe_raw) > eps and _ep > 0:
                if tp2 == 0 and _tp2_p > 0:
                    if abs(mfe_raw) >= abs(_tp2_p - _ep) - eps:
                        tp2 = 1
                if tp3 == 0 and _tp3_p > 0:
                    if abs(mfe_raw) >= abs(_tp3_p - _ep) - eps:
                        tp3 = 1
            # Fallback to profit if mfe_raw missing but mfe_pnl available
            elif abs(mfe_pnl) > eps and lot > eps and _ep > 0:
                if tp2 == 0 and _tp2_p > 0:
                    expected_tp2_pnl = abs(_tp2_p - _ep) * lot
                    if mfe_pnl >= expected_tp2_pnl - eps:
                        tp2 = 1
                if tp3 == 0 and _tp3_p > 0:
                    expected_tp3_pnl = abs(_tp3_p - _ep) * lot
                    if mfe_pnl >= expected_tp3_pnl - eps:
                        tp3 = 1

        # Get bucket early
        bucket = _to_str(t.get("close_reason") or t.get("bucket_close_reason") or "UNKNOWN")
        close_reason_raw = _to_str(t.get("close_reason_raw") or bucket)

        # Fallback: if tp_before_sl not set but close_reason says SL_AFTER_TP
        if tp_before_sl == 0 and ("SL_AFTER_TP" in close_reason_raw or "SL_AFTER_TP" in bucket):
             tp_before_sl = 1

        # --- DATA QUALITY: tp_hit_but_zero_pnl / inconsistent close_reason ---
        is_tp_hit = (tp1 > 0 or tp2 > 0 or tp3 > 0 or "TP" in bucket)
        is_sl_hit = (bucket in ("SL", "TRAIL_SL", "TRAILING_STOP") and tp_before_sl == 0)

        if is_tp_hit:
            m["cnt_tp_hit"] += 1
        if is_sl_hit:
            m["cnt_sl_hit"] += 1

        if is_tp_hit and pnl <= eps:
            m["tp_hit_but_zero_pnl"] += 1

        # bucket vs pnl sign (до любых авто-фиксов)
        if bucket in ("TP1", "TP2", "TP3", "TP") and pnl <= eps:
            m["close_reason_inconsistent_with_pnl_sign"] += 1
        if bucket in ("SL", "TRAILING_STOP") and pnl > eps:
            m["close_reason_inconsistent_with_pnl_sign"] += 1


        # --- base aggregates (как у вас) ---
        m["total_trades"] += 1
        m["total_pnl"] += pnl
        m["total_pnl_pct"] += pnl_pct
        m["total_fees"] += fees
        m["total_pnl_gross"] += pnl_gross

        # Corrected pnl_gross for honest reporting (bug 2026-05-14: INITIAL_SL was -2R).
        # IMPORTANT: don't trust stored one_r_money as ground truth — it could be stale itself
        # (recovered position with one_r=0 → fees-clamp gives ~1.5). Use geometry: lot × |entry−sl|
        # is the SL distance × position size, which equals one_r by definition (assuming SL price
        # and lot were set correctly at entry — both of which are stored independently).
        _lot_corr = _sf(t.get("lot") or 0.0)
        _entry_corr = _sf(t.get("entry_price") or t.get("avg_entry_price") or 0.0)
        _sl_corr = _sf(t.get("sl_price") or t.get("stop_loss") or t.get("sl") or 0.0)
        _theoretical_loss = _lot_corr * abs(_entry_corr - _sl_corr) if (
            _lot_corr > eps and _entry_corr > eps and _sl_corr > eps
        ) else 0.0
        _gross_corrected = pnl_gross
        if (bucket == "INITIAL_SL"
            and _theoretical_loss >= 1.0   # ignore dust legacy trades
            and abs(pnl_gross) > 1.5 * _theoretical_loss):
            # Loss is wildly larger than geometric truth → double-count bug → correct to -1×theoretical
            _gross_corrected = -_theoretical_loss
            m["initial_sl_corrected_count"] += 1
        m["total_pnl_gross_corrected"] += _gross_corrected
        m["total_pnl_net_corrected"] += (_gross_corrected - fees)

        m["total_notional_usd"] += notional_usd
        m["sum_duration_ms"] += dur

        m["tp1_hits"] += tp1
        m["tp2_hits"] += tp2
        m["tp3_hits"] += tp3
        if trailing_started > 0:
            m["trailing_started"] += 1

        if bucket == "TRAIL_SL":
            m["closed_by_trail"] += 1
            m["trailing_stop_hits"] += 1  # compatibility
        elif bucket == "TRAILING_STOP": # legacy fallback if normalizer didn't catch it
            m["closed_by_trail"] += 1
            m["trailing_stop_hits"] += 1

        # ✅ агрегация по trailing_profile
        profile = _to_str(t.get("trailing_profile") or "").strip()
        if profile:
            profiles = m.get("trailing_profiles") or {}
            profiles[profile] = profiles.get(profile, 0) + 1
            m["trailing_profiles"] = profiles

            p_stats = m.get("trailing_profile_stats") or {}
            if profile not in p_stats:
                p_stats[profile] = {"count": 0, "wins": 0, "pnl": 0.0}
            p_stats[profile]["count"] += 1
            p_stats[profile]["pnl"] += pnl
            if pnl > eps:
                p_stats[profile]["wins"] += 1
            m["trailing_profile_stats"] = p_stats

        # PF (gross)
        if pnl_gross > eps:
            m["gross_profit"] += pnl_gross
        elif pnl_gross < -eps:
            m["gross_loss"] += abs(pnl_gross)

        # reasons
        k = bucket or "(EMPTY)"
        m["reasons"][k] = m["reasons"].get(k, 0) + 1

        # diagnostics
        if pnl < -eps:
            m["neg_pnl_count"] += 1
        m["min_pnl"] = min(m["min_pnl"], pnl)
        m["max_pnl"] = max(m["max_pnl"], pnl)
        if abs(fees) <= eps:
            m["missing_fees_count"] += 1
        if dur <= 0:
            m["missing_duration_count"] += 1

        # fees huge
        if notional_usd > eps:
            fees_ratio = abs(fees) / max(notional_usd, eps)
            if fees_ratio > self.fees_turnover_huge_threshold:
                m["fees_huge_count"] += 1

        # net W/L/BE
        if pnl > eps:
            m["wins"] += 1
            m["sum_win_net"] += pnl
            m["cnt_win_net"] += 1
            m["wins_by_reason"][k] = m["wins_by_reason"].get(k, 0) + 1
        elif pnl < -eps:
            m["losses"] += 1
            m["sum_loss_net"] += pnl
            m["cnt_loss_net"] += 1
            m["losses_by_reason"][k] = m["losses_by_reason"].get(k, 0) + 1
        else:
            m["breakeven"] += 1
            m["breakeven_by_reason"][k] = m["breakeven_by_reason"].get(k, 0) + 1

        # strict counters handled at reporter level with TRAILING_PROFIT special case

        # TP -> SL
        if bucket == "SL" or bucket == "TRAIL_SL":
            if tp_before_sl >= 1:
                m["tp1_then_sl"] += 1
            if tp_before_sl >= 2:
                m["tp2_then_sl"] += 1
            if tp_before_sl >= 3:
                m["tp3_then_sl"] += 1

        # --- R / returns (SAFE RISK) ---
        # 6.2 Protection against division by dust
        MIN_RISK_USD = 1.0
        FEES_RISK_MULT = 3.0

        one_r_raw = _sf(t.get("one_r_money") or t.get("risk_amount") or 0.0)

        # ──────────────────────────────────────────────────────────────
        # Defensive fallback: reconstruct risk from lot × |entry - sl| when stored
        # one_r_money looks wrong. Two distinct broken cases handled:
        #   (1) Legacy/dust case — one_r below MIN_RISK_USD floor (lot=0.01 etc).
        #   (2) Stale-recovery case (bug 2026-05-14) — PositionState restored from
        #       Redis hash without one_r_money → defaulted to 0 → clamp_one_r_money
        #       in finalize pushed it to fees×3 ≈ 1.5 USD, much smaller than real risk.
        #       Symptom: stored one_r is well above MIN_RISK_USD but still much smaller
        #       than expected = lot × |entry - sl|.
        # We recompute when stored < 0.5 × theoretical (50% tolerance for slippage/fees).
        # ──────────────────────────────────────────────────────────────
        lot_size = _sf(t.get("lot") or 0.0)
        ent_p = _sf(t.get("entry_price") or t.get("avg_entry_price") or 0.0)
        sl_p = _sf(t.get("sl_price") or t.get("stop_loss") or t.get("sl") or 0.0)
        calc_r = lot_size * abs(ent_p - sl_p) if (lot_size > eps and ent_p > eps and sl_p > eps) else 0.0

        if one_r_raw < MIN_RISK_USD and calc_r >= MIN_RISK_USD:
            # Case (1): dust → reconstruct
            one_r_raw = calc_r
        elif calc_r >= MIN_RISK_USD and one_r_raw > eps and one_r_raw < 0.5 * calc_r:
            # Case (2): stale recovery — stored one_r is ≥ floor but suspiciously
            # smaller than theoretical lot×|entry-sl|. Trust the geometry.
            one_r_raw = calc_r
            m["count_clamped_risk"] += 1  # also flag as "fixed" for diagnostics

        has_real_risk = one_r_raw >= MIN_RISK_USD
        risk_usd_eff = 0.0

        if has_real_risk:
            # Clamp Risk to avoid division by dust (fees-based floor)
            risk_floor = max(MIN_RISK_USD, abs(fees) * FEES_RISK_MULT)
            risk_usd_eff = max(one_r_raw, risk_floor)

            if risk_usd_eff > (one_r_raw + eps):
                m["count_clamped_risk"] += 1

            # Calculate R using Effective Risk
            r = pnl / risk_usd_eff if risk_usd_eff > eps else 0.0

            m["cnt_r"] += 1
            m["sum_r"] += r
            m["sum_r2"] += r * r
            if r > eps:
                m["sum_win_r"] += r
                m["cnt_win_r"] += 1
            elif r < -eps:
                m["sum_loss_r"] += r
                m["cnt_loss_r"] += 1
            m["_r_values"].append(r)
        else:
            # Risk data missing — skip R computation to avoid corrupting stats
            m["count_missing_risk"] += 1

        # --- NEW: baseline (pnl_if_fixed_exit) R ---
        pnl_if_fixed_exit = _sf(t.get("pnl_if_fixed_exit") or 0.0)
        m["total_pnl_if_fixed_exit"] += pnl_if_fixed_exit
        if has_real_risk and abs(pnl_if_fixed_exit) > eps:
            # Baseline also uses Risk Eff for consistency
            r_fixed = pnl_if_fixed_exit / risk_usd_eff if risk_usd_eff > eps else 0.0

            m["cnt_r_fixed"] += 1
            m["sum_r_fixed"] += r_fixed
            m["sum_r_fixed2"] += r_fixed * r_fixed
            if r_fixed > eps:
                m["sum_win_r_fixed"] += r_fixed
                m["cnt_win_r_fixed"] += 1
            elif r_fixed < -eps:
                m["sum_loss_r_fixed"] += r_fixed
                m["cnt_loss_r_fixed"] += 1

        ret = 0.0
        has_ret = False
        if notional_usd > eps:
            ret = pnl / notional_usd
            has_ret = True
        elif abs(pnl_pct) > eps:
            # pnl_pct is usually in % (e.g. 1.2), ret is ratio (0.012)
            ret = pnl_pct / 100.0
            has_ret = True

        if has_ret or abs(pnl) > eps:
            m["cnt_ret"] += 1
            m["sum_ret"] += ret
            m["sum_ret2"] += ret * ret
            if ret < -eps:
                m["cnt_down_ret"] += 1
                m["sum_down_ret2"] += ret * ret
            m["_ret_values"].append(ret)

        # --- exit quality (если есть поля) ---
        lot = _sf(t.get("lot") or 0.0)

        # 1. MFE: try USD, fallback to Price * Lot
        mfe_pnl = _sf(t.get("mfe_pnl") or t.get("mfe_usd") or 0.0)
        if abs(mfe_pnl) <= eps:
             mfe_raw = _sf(t.get("mfe") or 0.0)
             if abs(mfe_raw) > eps and lot > eps:
                 mfe_pnl = mfe_raw * lot

        # 2. Giveback — stored as USD (closed.giveback = pos.mfe_pnl - pnl_gross, see handlers.finalize_trade)
        giveback = _sf(t.get("giveback_pnl") or t.get("giveback") or 0.0)

        # 3. Missed Profit — stored as USD (closed.missed_profit from calc_missed_profit)
        missed_profit = _sf(t.get("missed_profit_pnl") or t.get("missed_profit") or 0.0)

        if mfe_pnl > eps:
            calc_diff = max(0.0, mfe_pnl - pnl_gross)
            # Only override if the existing value is suspiciously 0 or inconsistent
            if abs(giveback) < eps and calc_diff > eps:
                giveback = calc_diff

            if abs(missed_profit) < eps and calc_diff > eps:
                missed_profit = calc_diff

            # Fix: If missed_profit is negative (garbage) or inconsistent for TRAIL_SL, enforce logic
            if missed_profit < -eps and calc_diff > eps:
                missed_profit = calc_diff

        # DEBUG: Log MFE data every 10 000th accumulate_trade call (global counter,
        # not per-window, to avoid spam when same trade goes through multiple windows).
        if next(_DEBUG_TRADE_CALL_COUNTER) % 10_000 == 0:
            logger.info(
                f"📊 DEBUG TRADE {t.get('order_id', 'unknown')[:8]}: "
                f"pnl_gross={pnl_gross:.2f}, pnl_net={pnl:.2f}, "
                f"mfe_pnl={mfe_pnl:.2f}, giveback={giveback:.2f}, "
                f"missed={missed_profit:.2f}, close_reason={bucket}"
            )

        if mfe_pnl > eps:
            # Exit efficiency по победам (wins-only логичнее)
            if pnl_gross > eps:
                exit_eff = pnl_gross / max(mfe_pnl, eps)
                # clamp 0..1 для читабельности
                if exit_eff < 0:
                    exit_eff = 0.0
                if exit_eff > 1:
                    exit_eff = 1.0
                m["sum_exit_eff_win"] += exit_eff
                m["cnt_exit_eff_win"] += 1

                # Giveback ratio по победам
                gb_ratio = giveback / max(mfe_pnl, eps)
                if gb_ratio < 0:
                    gb_ratio = 0.0
                if gb_ratio > 1.5:
                    gb_ratio = 1.5
                m["sum_giveback_ratio_win"] += gb_ratio
                m["cnt_giveback_ratio_win"] += 1

            # Missed profit ratio (для SL_AFTER_TP* или TRAIL_SL)
            # Расширяем условие: если это TRAIL_SL или явно SL_AFTER_TP
            if (bucket == "TRAIL_SL" or
                (close_reason_raw and "SL_AFTER_TP" in close_reason_raw) or
                (bucket and "SL_AFTER_TP" in bucket)):

                mp_ratio = missed_profit / max(mfe_pnl, eps)
                if mp_ratio < 0:
                    mp_ratio = 0.0
                if mp_ratio > 2.0:
                    mp_ratio = 2.0
                m["sum_missed_profit_ratio"] += mp_ratio
                m["cnt_missed_profit_ratio"] += 1

        # --- Setup Stats (ATR) ---
        # Parse OrderIntentV1.meta (stored as JSON string in order hash by NestJS executor).
        # sl_atr, tp1_atr, atr_used_for_levels, sl_price, tp_levels all live there.
        _meta_raw = t.get("meta")
        if isinstance(_meta_raw, str):
            try:
                _meta = json.loads(_meta_raw)
            except Exception:
                _meta = {}
        elif isinstance(_meta_raw, dict):
            _meta = _meta_raw
        else:
            _meta = {}
        _meta_tp_levels = _meta.get("tp_levels") or []

        # 1. Try explicit fields (top-level first, then meta)
        sl_atr = _sf(t.get("sl_atr") or t.get("sl_dist_atr") or _meta.get("sl_atr") or 0.0)
        tp_atr = _sf(t.get("tp_atr") or t.get("tp1_atr") or t.get("tp_dist_atr") or _meta.get("tp1_atr") or 0.0)

        # 2. Try calculation if missing
        if sl_atr <= 0 and tp_atr <= 0:
            # Prefer ATR used for levels (= ATR TF: 5m/15m), NOT 1m ATR from tick stream.
            # Priority: atr_used_for_levels > atr_at_entry > atr (1m fallback)
            atr = _sf(
                t.get("atr_used_for_levels")
                or t.get("atr_at_entry")
                or t.get("atr")
                or _meta.get("atr_used_for_levels")
                or _meta.get("atr")
                or 0.0
            )
            entry_price = _sf(t.get("entry_price") or t.get("avg_entry_price") or 0.0)
            if atr > eps and entry_price > eps:
                # SL
                sl_price = _sf(t.get("sl_price") or t.get("stop_loss") or t.get("sl") or _meta.get("sl_price") or 0.0)
                if sl_price > 0:
                    sl_atr = abs(entry_price - sl_price) / atr
                tp_price = _sf(
                    t.get("tp1_price") or t.get("tp_price") or t.get("take_profit") or t.get("tp1")
                    or (_meta_tp_levels[0] if _meta_tp_levels else 0.0)
                    or 0.0
                )
                if tp_price > 0:
                    tp_atr = abs(tp_price - entry_price) / atr

        if sl_atr > eps:
            m["sum_sl_atr"] += sl_atr
            m["cnt_sl_atr"] += 1

        if tp_atr > eps:
            m["sum_tp_atr"] += tp_atr
            m["cnt_tp_atr"] += 1

        # --- tp_final_atr: ATR distance to the furthest TP level actually touched ---
        # Always re-fetch atr/entry to cover cases where explicit sl_atr/tp_atr was used
        # (skipping the calc block above). TP3 > TP2 > TP1 priority.
        _atr_final = _sf(
            t.get("atr_used_for_levels") or t.get("atr_at_entry") or t.get("atr")
            or _meta.get("atr_used_for_levels") or _meta.get("atr") or 0.0
        )
        _ep_final = _sf(t.get("entry_price") or t.get("avg_entry_price") or 0.0)
        if _atr_final > eps and _ep_final > eps:
            _tp3_p = _sf(t.get("tp3_price") or t.get("tp3") or 0.0)
            _tp2_p = _sf(t.get("tp2_price") or t.get("tp2") or 0.0)
            _tp1_p = _sf(
                t.get("tp1_price") or t.get("tp_price") or t.get("take_profit") or t.get("tp1")
                or (_meta_tp_levels[0] if _meta_tp_levels else 0.0) or 0.0
            )
            # Pick the furthest TP that was actually touched
            _tp_final_p = 0.0
            if tp3 > 0 and _tp3_p > 0:
                _tp_final_p = _tp3_p
            elif tp2 > 0 and _tp2_p > 0:
                _tp_final_p = _tp2_p
            elif tp1 > 0 and _tp1_p > 0:
                _tp_final_p = _tp1_p
            else:
                # No TP hit — use tp1 as intended target for RR context
                _tp_final_p = _tp1_p
            if _tp_final_p > 0:
                tp_final_atr = abs(_tp_final_p - _ep_final) / _atr_final
                if tp_final_atr > eps:
                    m["sum_tp_final_atr"] += tp_final_atr
                    m["cnt_tp_final_atr"] += 1

        # series for MDD/streaks
        m["_series"].append((exit_ts, pnl))

        # accumulate pnl for VaR/CVaR
        m["_pnl_values"].append(pnl)

        # --- NEW: Scenarios & Gate Accumulation ---
        try:
            # signal_payload is a JSON-string in Redis/dict
            sp_raw = t.get("signal_payload")
            if sp_raw:
                if isinstance(sp_raw, str):
                    try:
                        sp = json.loads(sp_raw)
                    except Exception:
                        sp = {}
                else:
                    sp = sp_raw
            else:
                sp = {}

            # Extract indicators
            indicators = sp.get("indicators") or {}

            # 1. Scenario
            scenario = str(t.get("scenario") or sp.get("scenario") or "").lower()
            # fallback to indicators if not top-level
            if not scenario:
                 # In crypto_orderflow_service, it's stored as "strong_gate_scn" in indicators
                 scenario = str(indicators.get("strong_gate_scn") or "").lower()

            # fallback: if we have of_confirm dictionary, it often has scenario
            if not scenario and "of_confirm" in indicators:
                 scenario = str(indicators["of_confirm"].get("scenario") or "").lower()

            # The signal_payload structure usually mimics OFContext/OFInputs
            if "reversal" in scenario:
                m["cnt_scenario_reversal"] += 1
                m["sum_pnl_scenario_reversal"] += pnl
            elif "continuation" in scenario:
                 m["cnt_scenario_continuation"] += 1
                 m["sum_pnl_scenario_continuation"] += pnl
            else:
                 m["cnt_scenario_none"] += 1

            # --- 1.0b Trade-profile (router output) aggregation ---
            # Captures the name of the router profile applied to this trade
            # (e.g. range_absorption_v1, trend_breakout_v1, high_vol_breakout_v1).
            # Source priority: explicit trade field → signal_payload meta → indicators.
            meta = sp.get("meta") or {}
            tprof = (
                t.get("trade_profile")
                or meta.get("trade_profile")
                or indicators.get("trade_profile")
                or ""
            )
            tprof = str(tprof).strip()
            if tprof:
                tprofiles = m.get("trade_profiles") or {}
                tprofiles[tprof] = tprofiles.get(tprof, 0) + 1
                m["trade_profiles"] = tprofiles

                tp_stats = m.get("trade_profile_stats") or {}
                if tprof not in tp_stats:
                    tp_stats[tprof] = {"count": 0, "wins": 0, "pnl": 0.0, "sum_tp1_atr": 0.0, "cnt_tp1_atr": 0}
                tp_stats[tprof]["count"] += 1
                tp_stats[tprof]["pnl"] += pnl
                if pnl > eps:
                    tp_stats[tprof]["wins"] += 1
                # Carry tp1_atr (initial target) per profile to expose «TP1 не расширяется» per profile
                _tp1_atr_per_trade = (
                    _sf(t.get("tp_atr") or t.get("tp1_atr"))
                    or _sf(meta.get("rocket_tp1_actual_atr_mult"))
                    or _sf(indicators.get("rocket_tp1_actual_atr_mult"))
                )
                if _tp1_atr_per_trade > eps:
                    tp_stats[tprof]["sum_tp1_atr"] += _tp1_atr_per_trade
                    tp_stats[tprof]["cnt_tp1_atr"] += 1
                m["trade_profile_stats"] = tp_stats

            # --- 1.1 Regime Stats ---
            # Source priority:
            #   1) trade dict entry_regime (set by create_position from payload.regime)
            #   2) indicators.regime (embedded in signal_payload.indicators)
            #   3) signal_payload.regime (top-level)
            #   4) fallback to "unknown"
            reg_raw = (
                t.get("entry_regime")
                or t.get("regime_bucket")
                or indicators.get("regime_bucket")
                or indicators.get("regime")
                or meta.get("regime_bucket")
                or sp.get("regime_bucket")
                or sp.get("regime")
            )
            
            # If reg_raw is a dict (e.g. serialized RegimeSnapshot), extract the bucket
            if isinstance(reg_raw, dict):
                reg_raw = reg_raw.get("regime_bucket") or reg_raw.get("market_mode")
                
            regime = str(reg_raw or "unknown").strip().lower()
            # Normalize: treat "na" as "unknown" for cleaner stats
            if regime in ("", "na", "none"):
                regime = "unknown"
            if regime:
                reg_stats = m.get("regime_stats") or {}
                if regime not in reg_stats:
                    reg_stats[regime] = {"count": 0, "wins": 0, "pnl": 0.0}
                reg_stats[regime]["count"] += 1
                reg_stats[regime]["pnl"] += pnl
                if pnl > eps:
                    reg_stats[regime]["wins"] += 1
                m["regime_stats"] = reg_stats

            # 2. Gate Mode
            # gate_mode usually comes from config, might be implicitly ENFORCE unless "of_gate_mode" says SHADOW
            # In crypto_orderflow_service, indicators["of_gate_mode"] is populated.
            gate_mode = (indicators.get("of_gate_mode") or "ENFORCE").upper()

            if gate_mode == "SHADOW":
                m["cnt_gate_shadow"] += 1
                # 3. Shadow Veto (strong_gate_shadow_veto=1 in indicators means it WOULD have been filtered)
                if _si(indicators.get("strong_gate_shadow_veto") or 0) > 0:
                    m["cnt_gate_shadow_veto"] += 1
                    m["sum_pnl_shadow_veto"] += pnl
            else:
                m["cnt_gate_enforce"] += 1

            # 3. Strong Gate OK/Fail (Universal)
            # Logic: strong_gate_ok=1 -> Strong, 0 -> Fail (Weak)
            # If explicit key missing, we might deduce from veto but cleaner to use explicit.
            # Fallback: if shadow_veto=1 -> Fail. if ENFORCE and passed -> OK.
            strong_ok = _si(indicators.get("strong_gate_ok"))

            # If key not present (old signals), try backward compat
            if "strong_gate_ok" not in indicators:
                # Some signals have "strong_gate_ok" at top level of indicators, others might have "of_confirm_ok"
                if "of_confirm_ok" in indicators:
                    strong_ok = _si(indicators["of_confirm_ok"])
                elif _si(indicators.get("strong_gate_shadow_veto")) > 0:
                     strong_ok = 0
                elif gate_mode == "ENFORCE":
                     # If it passed accumulation, it must be OK in ENFORCE mode
                     strong_ok = 1
                else:
                     # Default fallback if unknown
                     strong_ok = 0

            if strong_ok > 0:
                m["cnt_strong_ok"] += 1
                m["sum_pnl_strong_ok"] += pnl
            else:
                m["cnt_strong_fail"] += 1
                m["sum_pnl_strong_fail"] += pnl

                # --- Diagnostic: Unmet ok reasons ---
                missing_str = indicators.get("strong_gate_missing")
                if missing_str:
                    for leg in missing_str.split(","):
                        leg = leg.strip()
                        if leg:
                            m["unmet_ok_reasons"][leg] = m["unmet_ok_reasons"].get(leg, 0) + 1
                else:
                    # fallback to strong_gate_reason (e.g. "continuation_gate(0/2)")
                    reason = indicators.get("strong_gate_reason")
                    if reason:
                        m["unmet_ok_reasons"][reason] = m["unmet_ok_reasons"].get(reason, 0) + 1

            # --- 4. OK-SOFT Stats (Requested) ---
            # Indicators are extracted above around line 657
            if indicators.get("strong_gate_soft_pass") or indicators.get("is_soft_fail") or indicators.get("of_confirm_ok_soft"):
                m["ok_soft_stats"]["count"] += 1
                m["ok_soft_stats"]["pnl"] += pnl
                if pnl > eps:
                    m["ok_soft_stats"]["wins"] += 1

                soft_reason = _to_str(indicators.get("soft_fail_reason") or indicators.get("of_confirm_soft_reason") or "unknown")
                m["ok_soft_reasons"][soft_reason] = m["ok_soft_reasons"].get(soft_reason, 0) + 1

            # 5. OF Confirm Stats (Detailed Breakdown)
            of_confirm_raw = indicators.get("of_confirm")
            # Parse of_confirm if it's a JSON string (common in Redis storage)
            of_confirm = None
            if of_confirm_raw:
                if isinstance(of_confirm_raw, str):
                    try:
                        of_confirm = json.loads(of_confirm_raw)
                    except Exception:
                        of_confirm = None
                elif isinstance(of_confirm_raw, dict):
                    of_confirm = of_confirm_raw
                else:
                    of_confirm = None

            # --- FALLBACK for DecisionRecordV1 (virtual trades) ---
            # If of_confirm is missing but we have "rule" in top-level payload
            if not of_confirm and "rule" in sp:
                rule = sp["rule"]
                if isinstance(rule, dict):
                    # Construct minimal of_confirm dict from rule
                    of_confirm = {
                        "scenario": rule.get("scenario"),
                        "have": rule.get("have"),
                        "need": rule.get("need"),
                        "ok": rule.get("ok"),
                        "evidence": {} # Will be filled below if possible
                    }
                    if "ml" in sp and isinstance(sp["ml"], dict):
                        of_confirm["evidence"]["ml"] = sp["ml"]

            if of_confirm and isinstance(of_confirm, dict):
                # scenario is already normalized above, but let's take it from of_confirm to be precise for this block
                of_scenario = str(of_confirm.get("scenario") or "none").lower()
                # Clean up scenario name (remove potential suffixes if any, though usually clean)

                have = _si(of_confirm.get("have") or 0)
                need = _si(of_confirm.get("need") or 0)

                # Key format: continuation_gate(1/2)
                key = f"{of_scenario}_gate({have}/{need})"

                stats = m["of_confirm_stats"].setdefault(key, {"count": 0, "wins": 0, "pnl": 0.0})
                stats["count"] += 1
                stats["pnl"] += pnl
                if pnl > eps:
                    stats["wins"] += 1

                # 5. ML Stats (NEW)
                # Path: evidence -> ml -> allow
                # Extract evidence from of_confirm (now guaranteed to be a dict)
                evidence_raw = of_confirm.get("evidence") or {}
                # Parse evidence if it's a JSON string
                if isinstance(evidence_raw, str):
                    try:
                        evidence = json.loads(evidence_raw)
                    except Exception:
                        evidence = {}
                elif isinstance(evidence_raw, dict):
                    evidence = evidence_raw
                else:
                    evidence = {}

                ml_dec_raw = evidence.get("ml_decision") or evidence.get("ml")
                # Parse ml_dec if it's a JSON string
                ml_dec = None
                if ml_dec_raw:
                    if isinstance(ml_dec_raw, str):
                        try:
                            ml_dec = json.loads(ml_dec_raw)
                        except Exception:
                            ml_dec = None
                    elif isinstance(ml_dec_raw, dict):
                        ml_dec = ml_dec_raw
                    else:
                        ml_dec = None

                if ml_dec and isinstance(ml_dec, dict):
                    ml_allow = ml_dec.get("allow")
                    p_edge = _sf(ml_dec.get("p_edge", 0.0))
                    p_min = _sf(ml_dec.get("p_min", 0.52))


                     # Existing binary stats
                    ml_key = "pass" if ml_allow else "veto"
                    ml_group = m["ml_stats"][ml_key]
                    ml_group["count"] += 1
                    ml_group["pnl"] += pnl
                    if pnl > eps:
                        ml_group["wins"] += 1

                    # --- NEW: Detailed condition breakdown ---
                    if "p_edge" in ml_dec:
                        ml_cond = m["ml_condition_stats"]
                        ml_cond["total_evaluated"] += 1
                        ml_cond["_p_edge_values"].append(p_edge)

                        # Per-threshold breakdown (test multiple thresholds)
                        if p_min < 0.30:
                            thresholds = [0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30]
                        else:
                            thresholds = [0.50, 0.52, 0.55, 0.58, 0.60, 0.65, 0.70]
                        for thr in thresholds:
                            thr_key = f"{thr:.2f}"
                            if thr_key not in ml_cond["by_threshold"]:
                                ml_cond["by_threshold"][thr_key] = {"count": 0, "wins": 0, "pnl": 0.0}

                            if p_edge >= thr:
                                stats = ml_cond["by_threshold"][thr_key]
                                stats["count"] += 1
                                stats["pnl"] += pnl
                                if pnl > eps:
                                    stats["wins"] += 1

                        # Per-scenario breakdown
                        scenario_key = (of_scenario or "none").lower()
                        if scenario_key not in ml_cond["by_scenario"]:
                            ml_cond["by_scenario"][scenario_key] = {
                                "count": 0, "wins": 0, "pnl": 0.0,
                                "sum_p_edge": 0.0, "avg_p_edge": 0.0
                            }

                        scn_stats = ml_cond["by_scenario"][scenario_key]
                        scn_stats["count"] += 1
                        scn_stats["pnl"] += pnl
                        scn_stats["sum_p_edge"] += p_edge
                        if pnl > eps:
                            scn_stats["wins"] += 1

                        # P_edge distribution
                        if p_min < 0.30:
                            if p_edge < 0.05: p_edge_bucket = "0.00-0.05"
                            elif p_edge < 0.10: p_edge_bucket = "0.05-0.10"
                            elif p_edge < 0.15: p_edge_bucket = "0.10-0.15"
                            elif p_edge < 0.20: p_edge_bucket = "0.15-0.20"
                            elif p_edge < 0.25: p_edge_bucket = "0.20-0.25"
                            else: p_edge_bucket = "0.25+"
                        else:
                            if p_edge < 0.3: p_edge_bucket = "0.0-0.3"
                            elif p_edge < 0.4: p_edge_bucket = "0.3-0.4"
                            elif p_edge < 0.5: p_edge_bucket = "0.4-0.5"
                            elif p_edge < 0.6: p_edge_bucket = "0.5-0.6"
                            elif p_edge < 0.7: p_edge_bucket = "0.6-0.7"
                            else: p_edge_bucket = "0.7-1.0"

                        if p_edge_bucket not in ml_cond["p_edge_distribution"]:
                            ml_cond["p_edge_distribution"][p_edge_bucket] = {"count": 0, "wins": 0, "pnl": 0.0}

                        bucket_stats = ml_cond["p_edge_distribution"][p_edge_bucket]
                        bucket_stats["count"] += 1
                        bucket_stats["pnl"] += pnl
                        if pnl > eps:
                            bucket_stats["wins"] += 1

            # 6. Strong (High Conf) Stats
            # Source: of_confirm.score (0..1)
            # Thresholds: 70, 75, 80, 85, 90, 95, 100
            score = 0.0
            if of_confirm:
                score = _sf(of_confirm.get("score") or 0.0)

            score_pct = score * 100.0
            thresholds_high_conf = [40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]

            shc = m.get("strong_high_conf_stats")
            if shc is None:
                shc = {}
                m["strong_high_conf_stats"] = shc

            for thr in thresholds_high_conf:
                # Inclusive check: score >= threshold
                if score_pct >= (thr - 1e-9):
                    k = str(thr)
                    if k not in shc:
                        shc[k] = {"count": 0, "wins": 0, "pnl": 0.0}

                    st = shc[k]
                    st["count"] += 1
                    st["pnl"] += pnl
                    if pnl > eps:
                        st["wins"] += 1

            # 7. Filtration Stats (NEW)
            is_rejected = _si(t.get("is_rejected_signal") or 0) > 0 or _to_str(t.get("rejection_reason")) == "low_tp1_dist"
            val_status = _to_str(t.get("validation_status")).lower()

            if is_rejected:
                m["cnt_rejected_low_tp"] += 1
            elif val_status == "failed":
                m["cnt_veto_gate"] += 1


        except Exception as e:
            # fail-open for metrics: count but don't crash
            m["_scenario_exception_count"] = m.get("_scenario_exception_count", 0) + 1
            if m.get("_scenario_exception_count", 0) <= 3:
                logger.debug(f"⚠️ accumulate_trade scenario/ML metrics error (#{m['_scenario_exception_count']}): {e}")

        return True

    # ---------------------------------------------------------------------
    # ФИНАЛИЗАЦИЯ: ВЫЧИСЛЕНИЕ ПРОИЗВОДНЫХ МЕТРИК ПО ОКНУ
    # ---------------------------------------------------------------------
    def finalize(self, m: dict[str, Any]) -> None:
        eps = self.eps
        n = m.get("total_trades", 0)
        if n <= 0:
            # чистим internal
            m.pop("_series", None)
            return

        # --- MDD + streaks требуют хронологии: сортируем по ts ---
        series: list[tuple[int, float]] = list(m.get("_series") or [])
        series.sort(key=lambda x: (x[0] if x[0] > 0 else 9_999_999_999_999_999))

        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        cur_w = 0
        cur_l = 0
        max_w = 0
        max_l = 0

        for _, pnl in series:
            equity += pnl
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

            if pnl > eps:
                cur_w += 1
                cur_l = 0
            elif pnl < -eps:
                cur_l += 1
                cur_w = 0
            else:
                cur_w = 0
                cur_l = 0
            max_w = max(max_w, cur_w)
            max_l = max(max_l, cur_l)

        m["max_drawdown_usd"] = max_dd
        m["max_consecutive_wins"] = max_w
        m["max_consecutive_losses"] = max_l

        # derived edge metrics
        m["expectancy_usd"] = safe_div(m["total_pnl"], n)

        # --- NEW: Edge Split (Entry vs Management) ---
        m["expectancy_entry_usd"] = safe_div(m.get("total_pnl_if_fixed_exit", 0.0), n)
        m["expectancy_mgmt_usd"] = safe_div(m["total_pnl"] - m.get("total_pnl_if_fixed_exit", 0.0), n)

        # payoff (net)
        avg_win = safe_div(m["sum_win_net"], m["cnt_win_net"])
        avg_loss = safe_div(m["sum_loss_net"], m["cnt_loss_net"])  # отрицательный
        m["payoff_net"] = safe_div(avg_win, abs(avg_loss))

        # R stats
        nr = m.get("cnt_r", 0)
        mean_r = safe_div(m["sum_r"], nr)
        var_r = max(safe_div(m["sum_r2"], nr) - mean_r * mean_r, 0.0)
        std_r = math.sqrt(var_r)
        m["expectancy_r"] = mean_r
        m["std_r"] = std_r

        # ProfitFactor по NET (в отличие от PF по pnl_gross)
        sum_win_net = m.get("sum_win_net", 0.0)
        sum_loss_net = m.get("sum_loss_net", 0.0)  # отрицательный
        if abs(sum_loss_net) < eps:
            m["profit_factor_net"] = float("inf") if sum_win_net > eps else 0.0
        else:
            m["profit_factor_net"] = sum_win_net / abs(sum_loss_net)


        # Median/Trimmed mean по R (робастнее среднего)
        r_vals: list[float] = list(m.get("_r_values") or [])
        m["median_r"] = _median(r_vals)
        m["trimmed_mean_r"] = _trimmed_mean(r_vals, self.trim_ratio)

        # payoff (R)
        avg_win_r = safe_div(m["sum_win_r"], m["cnt_win_r"])
        avg_loss_r = safe_div(m["sum_loss_r"], m["cnt_loss_r"])  # отрицательный
        if abs(avg_loss_r) < eps:
            m["payoff_r"] = float("inf") if avg_win_r > eps else 0.0
        else:
            m["payoff_r"] = avg_win_r / abs(avg_loss_r)

        # --- NEW: baseline (fixed exit) R stats ---
        nr_fixed = m.get("cnt_r_fixed", 0)
        mean_r_fixed = safe_div(m["sum_r_fixed"], nr_fixed)
        var_r_fixed = max(safe_div(m["sum_r_fixed2"], nr_fixed) - mean_r_fixed * mean_r_fixed, 0.0)
        std_r_fixed = math.sqrt(var_r_fixed)
        m["expectancy_fixed_r"] = mean_r_fixed
        m["std_r_fixed"] = std_r_fixed

        # payoff (R fixed)
        avg_win_r_fixed = safe_div(m["sum_win_r_fixed"], m["cnt_win_r_fixed"])
        avg_loss_r_fixed = safe_div(m["sum_loss_r_fixed"], m["cnt_loss_r_fixed"])  # отрицательный
        m["payoff_fixed_r"] = safe_div(avg_win_r_fixed, abs(avg_loss_r_fixed))

        # payoff_fixed_usd
        avg_win_usd = safe_div(m["sum_win_net"], m["cnt_win_net"])
        avg_loss_usd = safe_div(m["sum_loss_net"], m["cnt_loss_net"])
        m["payoff_fixed_usd"] = safe_div(avg_win_usd, abs(avg_loss_usd))

        # Win rate for baseline (fixed exit)
        total_fixed = m["cnt_win_r_fixed"] + m["cnt_loss_r_fixed"]
        m["n_fixed"] = total_fixed  # добавлено для PeriodicReporter
        m["wr_fixed"] = safe_div(m["cnt_win_r_fixed"], total_fixed)

        # Kelly (по R)
        denom = m["cnt_win_r"] + m["cnt_loss_r"]
        w = safe_div(m["cnt_win_r"], denom)
        b = m["payoff_r"]
        if b > eps:
            k = w - (1.0 - w) / b
            # cap
            cap = max(0.0, self.kelly_cap)
            if cap > 0:
                k = max(-cap, min(cap, k))
            m["kelly_f_r"] = k
        else:
            m["kelly_f_r"] = 0.0

        nret = m.get("cnt_ret", 0)
        mean_ret = safe_div(m["sum_ret"], nret)

        # Probability check for mean_ret fallback
        notional = m.get("total_notional_usd", 0.0)
        if abs(mean_ret) <= eps and notional > eps:
            mean_ret = m["total_pnl"] / notional

        var_ret = max(safe_div(m["sum_ret2"], nret) - mean_ret * mean_ret, 0.0)
        std_ret = math.sqrt(var_ret)
        m["mean_ret"] = mean_ret
        m["std_ret"] = std_ret

        m["sharpe_like_trades"] = safe_div(mean_ret * math.sqrt(nret), std_ret)

        # downside std (по отрицательным)
        downside_std = math.sqrt(safe_div(m["sum_down_ret2"], m["cnt_down_ret"]))
        m["downside_std_ret"] = downside_std
        m["sortino_like_trades"] = safe_div(mean_ret * math.sqrt(nret), downside_std)

        # exit quality averages
        m["exit_eff_avg_win"] = safe_div(m["sum_exit_eff_win"], m["cnt_exit_eff_win"])
        m["giveback_ratio_avg_win"] = safe_div(m["sum_giveback_ratio_win"], m["cnt_giveback_ratio_win"])
        m["missed_profit_ratio_avg"] = safe_div(m["sum_missed_profit_ratio"], m["cnt_missed_profit_ratio"])

        # --- Setup Stats (ATR) averages ---
        m["avg_sl_atr"] = safe_div(m["sum_sl_atr"], m["cnt_sl_atr"])
        m["avg_tp_atr"] = safe_div(m["sum_tp_atr"], m["cnt_tp_atr"])  # TP1 / initial target
        m["avg_tp_final_atr"] = safe_div(m["sum_tp_final_atr"], m["cnt_tp_final_atr"])  # furthest TP touched


        # --- NEW: tail risk (VaR/CVaR) ---
        alpha = self.es_alpha
        if n >= self.min_trades_for_es:
            ret_vals: list[float] = list(m.get("_ret_values") or [])
            r_vals: list[float] = list(m.get("_r_values") or [])
            pnl_vals: list[float] = list(m.get("_pnl_values") or [])

            m["var_ret"], m["cvar_ret"] = _var_cvar(ret_vals, alpha)
            m["var_r"], m["cvar_r"] = _var_cvar(r_vals, alpha)
            m["var_pnl"], m["cvar_pnl"] = _var_cvar(pnl_vals, alpha)
        else:
            m["var_ret"] = m["cvar_ret"] = 0.0
            m["var_r"] = m["cvar_r"] = 0.0
            m["var_pnl"] = m["cvar_pnl"] = 0.0

        # ✅ NEW: Safe Denominator Percentages
        # WR = wins / total
        m["win_rate"] = safe_div(m["wins"], n)

        # Trailing WR = closed_by_trail / trailing_started
        m["trailing_wr"] = safe_div(m["closed_by_trail"], m["trailing_started"])

        # --- NEW: ok-soft stats finalization ---
        ok_soft = m.get("ok_soft_stats", {})
        if ok_soft.get("count", 0) > 0:
            ok_soft["win_rate"] = safe_div(ok_soft["wins"], ok_soft["count"])
            # Share of ok-soft trades relative to total trades
            ok_soft["share"] = safe_div(ok_soft["count"], n)

        # --- NEW: ML condition stats finalization ---
        ml_cond = m.get("ml_condition_stats", {})
        p_edge_vals = ml_cond.get("_p_edge_values", [])
        if p_edge_vals:
            ml_cond["avg_p_edge"] = (sum(p_edge_vals) / len(p_edge_vals))
            ml_cond["median_p_edge"] = _median(p_edge_vals)

            # Finalize per-scenario averages
            for scn_key, scn_stats in ml_cond.get("by_scenario", {}).items():
                if scn_stats["count"] > 0:
                    scn_stats["avg_p_edge"] = scn_stats["sum_p_edge"] / scn_stats["count"]

        # cleanup internal series unless debug enabled
        if not self.enable_debug_series:
            m.pop("_series", None)
            m.pop("_r_values", None)
            m.pop("_pnl_values", None)
            # Clean up ML internal arrays
            if "ml_condition_stats" in m:
                m["ml_condition_stats"].pop("_p_edge_values", None)


    # ---------------------------------------------------------------------
    # СЛУЖЕБНЫЙ МЕТОД: ОЧИСТКА ВНУТРЕННИХ МАССИВОВ
    # ---------------------------------------------------------------------
