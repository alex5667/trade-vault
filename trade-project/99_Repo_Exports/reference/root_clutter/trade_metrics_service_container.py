from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Tuple, Optional

from common.log import setup_logger
from domain.normalizers import bucket_close_reason
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


def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    mid = n // 2
    if n % 2 == 1:
        return float(ys[mid])
    return float((ys[mid - 1] + ys[mid]) / 2.0)


def _trimmed_mean(xs: List[float], trim_ratio: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    k = int(n * max(0.0, min(0.49, trim_ratio)))
    core = ys[k: n - k] if (n - 2 * k) > 0 else ys
    return float(sum(core) / max(1, len(core)))


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


def _var_cvar(xs: List[float], alpha: float) -> Tuple[float, float]:
    """
    VaR/CVaR на левом хвосте:
    - VaR(alpha): квантиль alpha
    - CVaR(alpha): среднее worst alpha доли (Expected Shortfall)
    """
    if not xs:
        return 0.0, 0.0
    alpha = max(0.001, min(0.49, float(alpha)))
    ys = sorted(xs)  # ascending (worst first)
    n = len(ys)
    k = max(1, int(math.ceil(alpha * n)))
    tail = ys[:k]
    var = ys[k - 1]  # граница хвоста
    cvar = float(sum(tail) / k)
    return float(var), float(cvar)


class TradeMetricsService:
    """
    Сервис аккумулирования и финализации метрик окна сделок.
    Не ломает существующий формат m: добавляет новые поля, старые оставляет.
    """

    def __init__(self, eps: float = 1e-9):
        self.eps = float(eps)
        self.fees_turnover_huge_threshold = float(
            os.getenv("PERIODIC_REPORT_FEES_TURNOVER_HUGE_THRESHOLD", "0.02")
        )
        self.kelly_cap = float(os.getenv("PERIODIC_REPORT_KELLY_CAP", "1.0"))
        self.enable_debug_series = os.getenv("PERIODIC_REPORT_DEBUG_SERIES", "false").lower() == "true"
        self.trim_ratio = float(os.getenv("PERIODIC_REPORT_TRIM_RATIO", "0.10"))
        self.es_alpha = float(os.getenv("PERIODIC_REPORT_ES_ALPHA", "0.05"))
        self.min_trades_for_es = int(os.getenv("PERIODIC_REPORT_MIN_TRADES_FOR_ES", "20"))

    def new_metrics(self) -> Dict[str, Any]:
        m: Dict[str, Any] = {
            # --- existing (как у вас) ---
            "total_trades": 0,
            "wins": 0, "losses": 0, "breakeven": 0,
            "wins_strict": 0, "losses_strict": 0, "breakeven_strict": 0,
            "total_pnl": 0.0, "total_pnl_pct": 0.0, "total_fees": 0.0,
            "total_pnl_gross": 0.0,
            "total_notional_usd": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0,
            "tp1_hits": 0, "tp2_hits": 0, "tp3_hits": 0,
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
            "invariant_gross_net_fail": 0,
            "invariant_reason_pnl_fail": 0,
            "invariant_tp_sl_fail": 0,


            # trailing profiles aggregation
            "trailing_profiles": {},  # dict[str, int] — распределение профилей трейлинга

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
            "sum_tp_atr": 0.0, "cnt_tp_atr": 0,
        }
        return m

    def accumulate_trade(self, m: Dict[str, Any], t: Dict[str, Any]) -> bool:
        eps = self.eps

        pnl = _sf(t.get("pnl_net") or t.get("pnl") or 0.0)
        pnl_pct = _sf(t.get("pnl_pct") or 0.0)
        fees = _sf(t.get("fees") or 0.0)
        pnl_gross = _sf(t.get("pnl_gross") or pnl)

        # notional
        notional_usd = _sf(t.get("notional_usd") or 0.0)
        if notional_usd <= 0:
            entry_price = _sf(t.get("entry_price") or 0.0)
            lot = _sf(t.get("lot") or 0.0)
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



        # TP / trailing flags
        tp1 = _si(t.get("tp1_hit") or 0)
        tp2 = _si(t.get("tp2_hit") or 0)
        tp3 = _si(t.get("tp3_hit") or 0)
        tp_before_sl = _si(t.get("tp_before_sl") or 0)
        trailing_started = _si(t.get("trailing_started") or 0)
        
        # Get bucket early
        bucket = _to_str(t.get("close_reason") or t.get("bucket_close_reason") or "UNKNOWN")
        close_reason_raw = _to_str(t.get("close_reason_raw") or bucket)

        # --- DATA QUALITY: tp_hit_but_zero_pnl / inconsistent close_reason ---
        if (tp1 > 0 or tp2 > 0 or tp3 > 0) and pnl <= eps:
            m["tp_hit_but_zero_pnl"] += 1

        # bucket vs pnl sign (до любых авто-фиксов)
        if bucket in ("TP1", "TP2", "TP3", "TP") and pnl <= eps:
            m["close_reason_inconsistent_with_pnl_sign"] += 1
        if bucket in ("SL", "TRAILING_STOP") and pnl > eps:
            m["close_reason_inconsistent_with_pnl_sign"] += 1

        # --- ваш фикс TP3 для консистентности отчета (сохраняем поведение) ---
        if tp3 > 0 and pnl <= eps:
            bucket = "TP3"
            adjusted = pnl_gross - fees
            if adjusted <= eps:
                adjusted = max(fees, eps)
            pnl = max(adjusted, eps)
            pnl_pct = max(pnl_pct, 0.0)

        # --- base aggregates (как у вас) ---
        m["total_trades"] += 1
        m["total_pnl"] += pnl
        m["total_pnl_pct"] += pnl_pct
        m["total_fees"] += fees
        m["total_pnl_gross"] += pnl_gross
        m["total_notional_usd"] += notional_usd
        m["sum_duration_ms"] += float(dur)

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
            profiles[profile] = int(profiles.get(profile, 0)) + 1
            m["trailing_profiles"] = profiles

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
        if bucket == "SL":
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
        
        # Clamp Risk to avoid division by dust
        # risk_eff = max(risk_usd, 1.0, fees * 3.0)
        risk_floor = max(MIN_RISK_USD, abs(fees) * FEES_RISK_MULT)
        risk_usd_eff = max(one_r_raw, risk_floor)
        
        if risk_usd_eff > (one_r_raw + eps) and one_r_raw > 0:
             m["count_clamped_risk"] += 1
        
        # Calculate R using Effective Risk
        r = pnl / risk_usd_eff if risk_usd_eff > eps else 0.0
        
        if abs(r) > 0 or one_r_raw > 0:
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

        # --- NEW: baseline (pnl_if_fixed_exit) R ---
        pnl_if_fixed_exit = _sf(t.get("pnl_if_fixed_exit") or 0.0)
        if pnl_if_fixed_exit != 0.0:
            # Baseline also uses Risk Eff for consistency
            r_fixed = pnl_if_fixed_exit / risk_usd_eff if risk_usd_eff > eps else 0.0
            
            m["cnt_r_fixed"] += 1
            m["sum_r_fixed"] += r_fixed
            m["sum_r_fixed2"] += r_fixed * r_fixed
            m["total_pnl_if_fixed_exit"] += pnl_if_fixed_exit
            if r_fixed > eps:
                m["sum_win_r_fixed"] += r_fixed
                m["cnt_win_r_fixed"] += 1
            elif r_fixed < -eps:
                m["sum_loss_r_fixed"] += r_fixed
                m["cnt_loss_r_fixed"] += 1

        if notional_usd > eps:
            ret = pnl / notional_usd
            m["cnt_ret"] += 1
            m["sum_ret"] += ret
            m["sum_ret2"] += ret * ret
            if ret < 0:
                m["cnt_down_ret"] += 1
                m["sum_down_ret2"] += ret * ret
            m["_ret_values"].append(ret)

        # --- exit quality (если есть поля) ---
        mfe_pnl = _sf(t.get("mfe_pnl") or t.get("mfe") or t.get("mfe_usd") or 0.0)
        giveback = _sf(t.get("giveback") or t.get("giveback_pnl") or 0.0)
        missed_profit = _sf(t.get("missed_profit") or t.get("missed_profit_pnl") or 0.0)

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

            # Missed profit ratio (для SL_AFTER_TP*)
            if close_reason_raw and "SL_AFTER_TP" in close_reason_raw:
                mp_ratio = missed_profit / max(mfe_pnl, eps)
                if mp_ratio < 0:
                    mp_ratio = 0.0
                if mp_ratio > 2.0:
                    mp_ratio = 2.0
                m["sum_missed_profit_ratio"] += mp_ratio
                m["cnt_missed_profit_ratio"] += 1

        # --- Setup Stats (ATR) ---
        # 1. Try explicit fields
        sl_atr = _sf(t.get("sl_atr") or t.get("sl_dist_atr") or 0.0)
        tp_atr = _sf(t.get("tp_atr") or t.get("tp1_atr") or t.get("tp_dist_atr") or 0.0)

        # 2. Try calculation if missing
        if sl_atr <= 0 and tp_atr <= 0:
            atr = _sf(t.get("atr") or t.get("atr_at_entry") or 0.0)
            entry_price = _sf(t.get("entry_price") or t.get("avg_entry_price") or 0.0)
            if atr > eps and entry_price > eps:
                # SL
                sl_price = _sf(t.get("sl_price") or t.get("stop_loss") or t.get("sl") or 0.0)
                if sl_price > 0:
                    sl_atr = abs(entry_price - sl_price) / atr
                tp_price = _sf(t.get("tp1_price") or t.get("tp_price") or t.get("take_profit") or t.get("tp1") or 0.0)
                if tp_price > 0:
                    tp_atr = abs(tp_price - entry_price) / atr

        if sl_atr > eps:
            m["sum_sl_atr"] += sl_atr
            m["cnt_sl_atr"] += 1
        
        if tp_atr > eps:
            m["sum_tp_atr"] += tp_atr
            m["cnt_tp_atr"] += 1

        # series for MDD/streaks
        m["_series"].append((exit_ts, pnl))

        # accumulate pnl for VaR/CVaR
        m["_pnl_values"].append(float(pnl))
        return True

    # ---------------------------------------------------------------------
    # ФИНАЛИЗАЦИЯ: ВЫЧИСЛЕНИЕ ПРОИЗВОДНЫХ МЕТРИК ПО ОКНУ
    # ---------------------------------------------------------------------
    def finalize(self, m: Dict[str, Any]) -> None:
        eps = self.eps
        n = int(m.get("total_trades", 0))
        if n <= 0:
            # чистим internal
            m.pop("_series", None)
            return

        # --- MDD + streaks требуют хронологии: сортируем по ts ---
        series: List[Tuple[int, float]] = list(m.get("_series") or [])
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

        m["max_drawdown_usd"] = float(max_dd)
        m["max_consecutive_wins"] = int(max_w)
        m["max_consecutive_losses"] = int(max_l)

        # derived edge metrics
        m["expectancy_usd"] = safe_div(m["total_pnl"], n)

        # payoff (net)
        avg_win = safe_div(m["sum_win_net"], m["cnt_win_net"])
        avg_loss = safe_div(m["sum_loss_net"], m["cnt_loss_net"])  # отрицательный
        m["payoff_net"] = safe_div(avg_win, abs(avg_loss))

        # R stats
        nr = int(m.get("cnt_r", 0))
        mean_r = safe_div(m["sum_r"], nr)
        var_r = max(safe_div(m["sum_r2"], nr) - mean_r * mean_r, 0.0)
        std_r = math.sqrt(var_r)
        m["expectancy_r"] = float(mean_r)
        m["std_r"] = float(std_r)

        # ProfitFactor по NET (в отличие от PF по pnl_gross)
        sum_win_net = float(m.get("sum_win_net", 0.0))
        sum_loss_net = float(m.get("sum_loss_net", 0.0))  # отрицательный
        m["profit_factor_net"] = safe_div(sum_win_net, abs(sum_loss_net))


        # Median/Trimmed mean по R (робастнее среднего)
        r_vals: List[float] = list(m.get("_r_values") or [])
        m["median_r"] = _median(r_vals)
        m["trimmed_mean_r"] = _trimmed_mean(r_vals, self.trim_ratio)

        # payoff (R)
        avg_win_r = safe_div(m["sum_win_r"], m["cnt_win_r"])
        avg_loss_r = safe_div(m["sum_loss_r"], m["cnt_loss_r"])  # отрицательный
        m["payoff_r"] = safe_div(avg_win_r, abs(avg_loss_r))

        # --- NEW: baseline (fixed exit) R stats ---
        nr_fixed = int(m.get("cnt_r_fixed", 0))
        mean_r_fixed = safe_div(m["sum_r_fixed"], nr_fixed)
        var_r_fixed = max(safe_div(m["sum_r_fixed2"], nr_fixed) - mean_r_fixed * mean_r_fixed, 0.0)
        std_r_fixed = math.sqrt(var_r_fixed)
        m["expectancy_fixed_r"] = float(mean_r_fixed)
        m["std_r_fixed"] = float(std_r_fixed)

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
            m["kelly_f_r"] = float(k)
        else:
            m["kelly_f_r"] = 0.0

        # returns stats + Sharpe/Sortino (trades)
        nret = int(m.get("cnt_ret", 0))
        mean_ret = safe_div(m["sum_ret"], nret)
        var_ret = max(safe_div(m["sum_ret2"], nret) - mean_ret * mean_ret, 0.0)
        std_ret = math.sqrt(var_ret)
        m["mean_ret"] = float(mean_ret)
        m["std_ret"] = float(std_ret)

        m["sharpe_like_trades"] = safe_div(mean_ret * math.sqrt(nret), std_ret)

        # downside std (по отрицательным)
        downside_std = math.sqrt(safe_div(m["sum_down_ret2"], m["cnt_down_ret"]))
        m["downside_std_ret"] = float(downside_std)
        m["sortino_like_trades"] = safe_div(mean_ret * math.sqrt(nret), downside_std)

        # exit quality averages
        m["exit_eff_avg_win"] = safe_div(m["sum_exit_eff_win"], m["cnt_exit_eff_win"])
        m["giveback_ratio_avg_win"] = safe_div(m["sum_giveback_ratio_win"], m["cnt_giveback_ratio_win"])
        m["missed_profit_ratio_avg"] = safe_div(m["sum_missed_profit_ratio"], m["cnt_missed_profit_ratio"])

        # --- Setup Stats (ATR) averages ---
        m["avg_sl_atr"] = safe_div(m["sum_sl_atr"], m["cnt_sl_atr"])
        m["avg_tp_atr"] = safe_div(m["sum_tp_atr"], m["cnt_tp_atr"])


        # --- NEW: tail risk (VaR/CVaR) ---
        alpha = self.es_alpha
        if n >= self.min_trades_for_es:
            ret_vals: List[float] = list(m.get("_ret_values") or [])
            r_vals: List[float] = list(m.get("_r_values") or [])
            pnl_vals: List[float] = list(m.get("_pnl_values") or [])

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

        # cleanup internal series unless debug enabled
        if not self.enable_debug_series:
            m.pop("_series", None)
            m.pop("_r_values", None)
            m.pop("_pnl_values", None)

    # ---------------------------------------------------------------------
    # СЛУЖЕБНЫЙ МЕТОД: ОЧИСТКА ВНУТРЕННИХ МАССИВОВ
    # ---------------------------------------------------------------------
