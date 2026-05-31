# -*- coding: utf-8 -*-
"""services/orderflow/liqmap_features.py

Модуль извлечения фичей из снапшота (liquidation heatmap).
Считает анкоры для TP1/SL (в bps) на основе пиков ликвидаций.
"""

import json
import logging
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

def try_parse_liqmap_snapshot_json(raw: Optional[bytes | str]) -> Optional[Dict]:
    """Parse JSON payload securely."""
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception as e:
        logger.debug(f"liqmap json parse err: {e}")
        return None

def compute_liqmap_features_from_snapshot(
    payload: Dict,
    mid_px: float,
    now_ms: int,
    max_stale_ms: int,
    peak_range_bps: float,
    front_run_bps: float,
    sl_buffer_bps: float,
) -> Dict[str, float]:
    """
    Извлекает пики ликвидаций (long/short USD) и возвращает словарь метрик.

    Контекст:
      - Long liquidations happen when price DROPS. Their peaks are BELOW mid_px.
      - Short liquidations happen when price RISES. Their peaks are ABOVE mid_px.

    Args:
        payload: {"ts_ms": int, "levels": [{"price": str, "long_usd": str, "short_usd": str}, ...]}
        mid_px: текущая цена (из flow)
        now_ms: текущее время
        max_stale_ms: порог устаревания
        peak_range_bps: дистанция поиска кластеров вокруг текущей цены
        front_run_bps: не доходя до кластера
        sl_buffer_bps: отступ за пределы кластера

    Returns:
        Словарь, где ключи:
        is_stale, stale_ms, levels_n, squeeze_bias,
        long_peak_usd, long_peak_price, short_peak_usd, short_peak_price,
        tp1_anchor_bps_long, sl_reco_bps_long,
        tp1_anchor_bps_short, sl_reco_bps_short
    """
    if not (isinstance(mid_px, (int, float)) and mid_px > 0):
        return {}

    ts_ms = payload.get("ts_ms", 0)
    stale_ms = max(0, now_ms - ts_ms)
    is_stale = 1 if stale_ms > max_stale_ms else 0
    levels = payload.get("levels", [])

    feats = {
        "stale_ms": float(stale_ms),
        "is_stale": float(is_stale),
        "levels_n": float(len(levels)),
        "squeeze_bias": 0.0,
    }

    if not levels:
        return feats

    max_price_offset = mid_px * (peak_range_bps / 10000.0)
    lower_bound = mid_px - max_price_offset
    upper_bound = mid_px + max_price_offset

    best_long_liq_usd = -1.0
    best_long_liq_px = -1.0

    best_short_liq_usd = -1.0
    best_short_liq_px = -1.0

    tot_long_usd = 0.0
    tot_short_usd = 0.0

    # 1. Сбор пиков
    for lvl in levels:
        try:
            p = float(lvl.get("price", 0))
            l_usd = float(lvl.get("long_usd", 0))
            s_usd = float(lvl.get("short_usd", 0))

            # Считаем bias по *всем* уровням внутри диапазона (или даже глобально, но возьмем глобально из снапшота)
            tot_long_usd += l_usd
            tot_short_usd += s_usd

            if not (lower_bound <= p <= upper_bound):
                continue

            # Longs liquidate down
            if p < mid_px and l_usd > best_long_liq_usd:
                best_long_liq_usd = l_usd
                best_long_liq_px = p

            # Shorts liquidate up
            if p > mid_px and s_usd > best_short_liq_usd:
                best_short_liq_usd = s_usd
                best_short_liq_px = p
        except (ValueError, TypeError):
            continue

    total_usd = tot_long_usd + tot_short_usd
    if total_usd > 0:
        # bias > 0.5 means more shorts are liquidated (bullish overall positioning)
        feats["squeeze_bias"] = float(tot_short_usd / total_usd)

    feats["long_peak_price"] = best_long_liq_px if best_long_liq_px > 0 else 0.0
    feats["long_peak_usd"] = best_long_liq_usd if best_long_liq_usd > 0 else 0.0
    feats["short_peak_price"] = best_short_liq_px if best_short_liq_px > 0 else 0.0
    feats["short_peak_usd"] = best_short_liq_usd if best_short_liq_usd > 0 else 0.0

    # 2. Расчет анкоров и рекомендаций для LONG сделки
    # (покупаем сейчас, TP наверху, SL внизу)
    if best_short_liq_px > mid_px:
        # TP до пика шортов (забираем ликвидность до них)
        dist_bps = ((best_short_liq_px - mid_px) / mid_px) * 10000.0
        feats["tp1_anchor_bps_long"] = max(0.0, dist_bps - front_run_bps)
    
    if best_long_liq_px > 0 and best_long_liq_px < mid_px:
        # SL ставим за скоплением лонгистов, которых будут ликвидировать вниз
        dist_bps = ((mid_px - best_long_liq_px) / mid_px) * 10000.0
        feats["sl_reco_bps_long"] = dist_bps + sl_buffer_bps

    # 3. Расчет анкоров и рекомендаций для SHORT сделки
    # (продаем сейчас, TP внизу, SL наверху)
    if best_long_liq_px > 0 and best_long_liq_px < mid_px:
        # TP перед пиком лонгов (снизу)
        dist_bps = ((mid_px - best_long_liq_px) / mid_px) * 10000.0
        feats["tp1_anchor_bps_short"] = max(0.0, dist_bps - front_run_bps)

    if best_short_liq_px > mid_px:
        # SL прячем за ликвидацию шортистов (наверху)
        dist_bps = ((best_short_liq_px - mid_px) / mid_px) * 10000.0
        feats["sl_reco_bps_short"] = dist_bps + sl_buffer_bps

    return feats

# --- apply_liqmap_tp_sl_adjustment v1 (MIRROR SYNC D1) BEGIN ---
def apply_liqmap_tp_sl_adjustment(
    *,
    entry: float,
    side: str,  # "LONG"/"SHORT" (also accepts "BUY"/"SELL")
    base_sl: float,
    base_tp1: float,
    indicators: Dict[str, Any],  # already contains liqmap_{w}_* keys from injection
    window: str = "1h",
    min_usd: float,
    buffer_bps: float,
    max_sl_widen_bps: float,
    enable_tp1: bool,
    enable_sl: bool,
) -> Tuple[float, float, Dict[str, Any]]:
    """Apply liquidation-map TP1/SL overlay (safe v1).

    Deterministic, side-effect-minimal helper for _calculate_levels().
    It reads *already injected* liqmap features from `indicators` and proposes
    adjusted TP1/SL levels using a "front-run / behind peak" policy.

    Inputs:
      - entry: trade entry price
      - side: "LONG"/"SHORT" (aliases: "BUY"/"SELL")
      - base_sl/base_tp1: levels produced by the base system (ATR/structure/etc.)
      - indicators: runtime indicators dict (must contain liqmap features)
      - window: liqmap window routing key (default "1h")
      - min_usd: minimum peak USD to consider it as an anchor
      - buffer_bps: safety buffer in bps (10 bps = 0.10%)
      - max_sl_widen_bps: maximum additional widening allowed for SL (risk control)
      - enable_tp1/enable_sl: feature flags for incremental rollout

    Contract:
      - Returns (new_sl, new_tp1, out_patch).
      - `out_patch` is safe to do `indicators.update(out_patch)` by the caller.

    Notes:
      - We support two naming styles in indicators:
          A) explicit peak price keys (new):
             liqmap_<w>_peak_up_price / peak_dn_price
             liqmap_<w>_peak_up_usd   / peak_dn_usd
             liqmap_<w>_peak_up_dist_bps / peak_dn_dist_bps
          B) compact core.liqmap_features_v1 keys (current injection):
             liqmap_<w>_dist_up_bps / dist_dn_bps
             liqmap_<w>_peak_up1_usd / peak_dn1_usd
        If price is missing we derive it from entry and dist_bps.

      - "Safe v1" policy is intentionally conservative:
        * TP1: avoid a strong peak if it sits between entry and base TP1.
        * SL: hide behind a strong adverse peak if it sits between base SL and entry,
              but cap any widening (max_sl_widen_bps) to avoid uncontrolled risk.
    """

    def _f(x: Any, default: float = 0.0) -> float:
        try:
            v = float(x)
            # avoid NaN/inf silently contaminating levels
            if v != v or v == float("inf") or v == float("-inf"):
                return float(default)
            return v
        except Exception:
            return float(default)

    def _norm_side(s: str) -> str:
        ss = (s or "").strip().upper()
        if ss in ("BUY", "LONG"):
            return "LONG"
        if ss in ("SELL", "SHORT"):
            return "SHORT"
        return ss  # keep as-is for debugging / fallback

    def _tp_bps(entry_px: float, tp_px: float, s: str) -> float:
        if entry_px <= 0:
            return 0.0
        if s == "LONG":
            return max(0.0, (tp_px - entry_px) / entry_px * 10000.0)
        # SHORT
        return max(0.0, (entry_px - tp_px) / entry_px * 10000.0)

    def _sl_bps(entry_px: float, sl_px: float, s: str) -> float:
        if entry_px <= 0:
            return 0.0
        if s == "LONG":
            return max(0.0, (entry_px - sl_px) / entry_px * 10000.0)
        # SHORT
        return max(0.0, (sl_px - entry_px) / entry_px * 10000.0)

    # ---- validate inputs ----
    entry_px = _f(entry, 0.0)
    base_sl_px = _f(base_sl, 0.0)
    base_tp1_px = _f(base_tp1, 0.0)
    s_side = _norm_side(side)

    out: Dict[str, Any] = {
        "liqmap_levels_applied": 0.0,
        "liqmap_tp1_adj_bps": 0.0,
        "liqmap_sl_adj_bps": 0.0,
        "liqmap_tp1_anchor_price": 0.0,
        "liqmap_sl_anchor_price": 0.0,
        "liqmap_tp1_anchor_usd": 0.0,
        "liqmap_sl_anchor_usd": 0.0,
        "liqmap_levels_reason": "no_peak",
    }

    # hard fail-open: if inputs are invalid, return base levels and zero patch
    if entry_px <= 0.0 or base_sl_px <= 0.0 or base_tp1_px <= 0.0 or s_side not in ("LONG", "SHORT"):
        return base_sl_px, base_tp1_px, out

    w = (window or "1h").strip()
    pref = f"liqmap_{w}_"

    buf = max(0.0, float(buffer_bps)) / 10000.0
    min_usd_f = max(0.0, float(min_usd))
    max_widen = max(0.0, float(max_sl_widen_bps))

    # Helper: fetch peak meta; derive price from dist if not provided.
    def _get_peak(direction: str) -> Tuple[float, float, float]:
        # price, usd, dist_bps
        dir_up = direction == "up"
        # explicit keys (preferred if present)
        px = _f(indicators.get(f"{pref}peak_{direction}_price", 0.0), 0.0)
        usd = _f(indicators.get(f"{pref}peak_{direction}_usd", 0.0), 0.0)
        dist = _f(indicators.get(f"{pref}peak_{direction}_dist_bps", 0.0), 0.0)

        # v1 core keys
        if usd <= 0.0:
            usd = _f(indicators.get(f"{pref}peak_{direction}1_usd", 0.0), 0.0)
            if dir_up and usd <= 0.0:
                usd = _f(indicators.get(f"{pref}peak_up1_usd", 0.0), 0.0)
            if (not dir_up) and usd <= 0.0:
                usd = _f(indicators.get(f"{pref}peak_dn1_usd", 0.0), 0.0)

        if dist <= 0.0:
            # core dist keys: dist_up_bps / dist_dn_bps
            dist = _f(indicators.get(f"{pref}dist_{direction}_bps", 0.0), 0.0)
            if dir_up and dist <= 0.0:
                dist = _f(indicators.get(f"{pref}dist_up_bps", 0.0), 0.0)
            if (not dir_up) and dist <= 0.0:
                dist = _f(indicators.get(f"{pref}dist_dn_bps", 0.0), 0.0)

        if px <= 0.0 and dist > 0.0:
            # derive peak price from entry and distance (deterministic)
            if dir_up:
                px = entry_px * (1.0 + dist / 10000.0)
            else:
                px = entry_px * (1.0 - dist / 10000.0)

        return px, usd, dist

    up_px, up_usd, _ = _get_peak("up")
    dn_px, dn_usd, _ = _get_peak("dn")

    # Defaults: keep base
    new_sl_px = base_sl_px
    new_tp1_px = base_tp1_px

    # Compute base distances in bps (for adjustment reporting)
    base_tp_bps = _tp_bps(entry_px, base_tp1_px, s_side)
    base_stop_bps = _sl_bps(entry_px, base_sl_px, s_side)

    reasons = []

    # ────────────────────────────────────────────────
    # TP1 overlay
    # ────────────────────────────────────────────────
    if enable_tp1:
        if s_side == "LONG":
            # LONG: favorable peak is ABOVE entry.
            # Apply only if it sits between entry and base TP1.
            if up_usd >= min_usd_f and up_px > entry_px and up_px < base_tp1_px:
                cand = up_px * (1.0 - buf)  # front-run: TP just before the peak
                if cand > entry_px:
                    new_tp1_px = cand
                    out["liqmap_tp1_anchor_price"] = float(up_px)
                    out["liqmap_tp1_anchor_usd"] = float(up_usd)
                    reasons.append("tp1_before_peak")
        else:
            # SHORT: favorable peak is BELOW entry.
            # Apply only if it sits between base TP1 and entry.
            if dn_usd >= min_usd_f and dn_px < entry_px and dn_px > base_tp1_px:
                cand = dn_px * (1.0 - buf)  # "after" cluster: go a bit beyond (lower)
                if cand < entry_px:
                    new_tp1_px = cand
                    out["liqmap_tp1_anchor_price"] = float(dn_px)
                    out["liqmap_tp1_anchor_usd"] = float(dn_usd)
                    reasons.append("tp1_after_peak")

    # ────────────────────────────────────────────────
    # SL overlay
    # ────────────────────────────────────────────────
    cap_applied = False
    if enable_sl:
        if s_side == "LONG":
            # LONG: adverse peak is BELOW entry (liquidation cluster of longs).
                # Peak may be between (base_sl, entry) or beyond base_sl; widening is capped.
            if dn_usd >= min_usd_f and dn_px < entry_px and dn_px > 0.0:
                cand = dn_px * (1.0 - buf)  # hide SL behind the peak (lower)
                if cand < entry_px:
                    cand_stop_bps = _sl_bps(entry_px, cand, s_side)
                    # cap widening to control risk
                    if cand_stop_bps - base_stop_bps > max_widen:
                        cap_stop_bps = base_stop_bps + max_widen
                        new_sl_px = entry_px * (1.0 - cap_stop_bps / 10000.0)
                        cap_applied = True
                    else:
                        new_sl_px = cand
                    out["liqmap_sl_anchor_price"] = float(dn_px)
                    out["liqmap_sl_anchor_usd"] = float(dn_usd)
                    reasons.append("sl_behind_peak")
        else:
            # SHORT: adverse peak is ABOVE entry.
                # Peak may be between (entry, base_sl) or beyond base_sl; widening is capped.
            if up_usd >= min_usd_f and up_px > entry_px and up_px > 0.0:
                cand = up_px * (1.0 + buf)  # hide SL behind the peak (higher)
                if cand > entry_px:
                    cand_stop_bps = _sl_bps(entry_px, cand, s_side)
                    if cand_stop_bps - base_stop_bps > max_widen:
                        cap_stop_bps = base_stop_bps + max_widen
                        new_sl_px = entry_px * (1.0 + cap_stop_bps / 10000.0)
                        cap_applied = True
                    else:
                        new_sl_px = cand
                    out["liqmap_sl_anchor_price"] = float(up_px)
                    out["liqmap_sl_anchor_usd"] = float(up_usd)
                    reasons.append("sl_behind_peak")

    # ---- export adjustments (bps) ----
    new_tp_bps = _tp_bps(entry_px, new_tp1_px, s_side)
    new_stop_bps = _sl_bps(entry_px, new_sl_px, s_side)

    if abs(new_tp1_px - base_tp1_px) > 1e-12:
        out["liqmap_tp1_adj_bps"] = float(new_tp_bps - base_tp_bps)
    if abs(new_sl_px - base_sl_px) > 1e-12:
        out["liqmap_sl_adj_bps"] = float(new_stop_bps - base_stop_bps)

    applied = (abs(new_tp1_px - base_tp1_px) > 1e-12) or (abs(new_sl_px - base_sl_px) > 1e-12)
    out["liqmap_levels_applied"] = 1.0 if applied else 0.0

    if cap_applied:
        out["liqmap_levels_reason"] = "cap_sl_widen"
    elif applied and len(reasons) >= 2:
        out["liqmap_levels_reason"] = "tp1_sl"
    elif applied and len(reasons) == 1:
        out["liqmap_levels_reason"] = reasons[0]
    else:
        out["liqmap_levels_reason"] = "no_peak"

    return new_sl_px, new_tp1_px, out
# --- apply_liqmap_tp_sl_adjustment v1 (MIRROR SYNC D1) END ---
