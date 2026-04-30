from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from typing import Any, Dict

from common.math_safe import clamp
from services.slq_store import fetch_slq


def _envf(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default) or default)
    except Exception:
        return float(default)


def _envi(name: str, default: str) -> int:
    try:
        return int(float(os.getenv(name, default) or default))
    except Exception:
        return int(float(default))


def _env_on(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _side_str(side: Any) -> str:
    try:
        s = int(side)
        return "LONG" if s > 0 else "SHORT"
    except Exception:
        ss = str(side or "").strip().lower()
        if ss in {"long", "buy", "1"}:
            return "LONG"
        if ss in {"short", "sell", "-1"}:
            return "SHORT"
        return "NA"


def _bucket_from_ctx(ctx: Any) -> str:
    # 1) если у вас уже есть “regime label” — используйте
    try:
        b = getattr(ctx, "regime", None)
        if b:
            return str(b)
    except Exception:
        pass
    # 2) можно добавить catr-куантиль, если есть (catr_q75 etc)
    try:
        b = getattr(ctx, "catr_bucket", None)
        if b:
            return str(b)
    except Exception:
        pass
    return "na"


def _tp1_prob_from_ctx(ctx: Any) -> float:
    try:
        v = getattr(ctx, "tp1_hit_prob", None)
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def maybe_apply_slq_to_risk_cfg(
    *
    redis: Any
    ctx: Any
    symbol: str
    side: Any
    cfg: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Returns effective cfg (may be same as input).
    Idempotent: if cfg already has slq_used=1 => returns as-is.
    Fail-open on any errors.
    """
    cfgd = dict(cfg or {})
    if cfgd.get("slq_used") == 1:
        return cfgd
    if not _env_on("SLQ_ENABLE", "0"):
        return cfgd

    # --- gates / knobs ---
    min_n = _envi("SLQ_MIN_N", "200")
    max_age_sec = _envi("SLQ_MAX_AGE_SEC", "3600")
    k = _envf("SLQ_K", "0.7")
    bump_cap_atr = _envf("SLQ_BUMP_ATR_CAP", "0.40")
    stop_atr_min = _envf("SLQ_STOP_ATR_MIN", "0.50")
    stop_atr_max = _envf("SLQ_STOP_ATR_MAX", "1.50")
    tp1_prob_min = _envf("SLQ_TP1_PROB_MIN", "0.55")
    postsl_tp1_min = _envf("SLQ_POSTSL_TP1_MIN", "0.25")

    side_s = _side_str(side)
    bucket = _bucket_from_ctx(ctx)
    key = f"slq:{str(symbol).upper()}:{side_s}:{bucket}"

    snap = fetch_slq(redis, key=key)
    if snap is None:
        return cfgd
    if snap.n < min_n:
        return cfgd
    if snap.ts_ms > 0 and max_age_sec > 0:
        age = (get_ny_time_millis() - int(snap.ts_ms)) / 1000.0
        if age > float(max_age_sec):
            return cfgd

    # “имеет смысл расширять”, только если часто после SL рынок всё же доходил до TP1
    if float(snap.post_sl_tp1_hit_rate) < float(postsl_tp1_min):
        return cfgd

    tp1_prob = _tp1_prob_from_ctx(ctx)
    if tp1_prob < float(tp1_prob_min):
        return cfgd

    # --- apply only for ATR-mode (ваш основной кейс) ---
    stop_mode = str(cfgd.get("STOP_MODE") or cfgd.get("stop_mode") or "atr").lower()
    if stop_mode not in {"atr", "atr_mult", "atr-mult"}:
        return cfgd

    base = float(cfgd.get("STOP_ATR_MULT") or cfgd.get("stop_atr_mult") or 0.0)
    if base <= 0:
        return cfgd

    # Preservation of the base multiplier before any adjustments
    if "STOP_ATR_MULT_BASE" not in cfgd:
        cfgd["STOP_ATR_MULT_BASE"] = base
        cfgd["stop_atr_mult_base"] = base

    bump = float(k) * float(snap.sl_buffer_atr_q90)
    bump = clamp(bump, 0.0, float(bump_cap_atr))
    val = clamp(base + bump, float(stop_atr_min), float(stop_atr_max))

    # write both: compute_levels/test use UPPERCASE, some runtime might use lowercase
    cfgd["STOP_ATR_MULT"] = float(val)
    cfgd["stop_atr_mult"] = float(val)
    
    # keep mode consistent
    if "STOP_MODE" in cfgd:
        cfgd["STOP_MODE"] = "ATR"
    cfgd["stop_mode"] = "atr"
    
    # meta для наблюдаемости/аналитики
    cfgd["slq_used"] = 1
    cfgd["slq_key"] = key
    cfgd["slq_n"] = int(snap.n)
    cfgd["slq_q90"] = float(snap.sl_buffer_atr_q90)
    cfgd["slq_postsl_tp1"] = float(snap.post_sl_tp1_hit_rate)
    cfgd["slq_tp1_prob"] = float(tp1_prob)
    cfgd["slq_bump_atr"] = float(bump)
    cfgd["slq_original_mult"] = float(base)
    
    # --- Dynamic TP1 Scaling ---
    # Если мы расширили SL, нужно пропорционально отодвинуть TP1, 
    # чтобы сохранить Risk/Reward отношение (обычно ~1.3 для ROCKET V1).
    if val > base and base > 0:
        ratio = val / base
        
        # Пытаемся найти базовый TP1 mult (из конфига или env)
        # 1. Из конфига
        base_tp1 = float(cfgd.get("ROCKET_TP1_ATR_MULT") or 0.0)
        
        # 2. Если нет в конфиге, пробуем ENV (с учетом символа)
        if base_tp1 <= 0:
             # Пробуем symbol specific (e.g. BNB_ROCKET_TP1_ATR_MULT)
             sym_prefix = str(symbol).split("USD")[0].upper() # Упрощенно
             base_tp1 = _envf(f"{sym_prefix}_ROCKET_TP1_ATR_MULT", "0.0")
        
        # 3. Fallback на глобальный ENV или дефолт 0.78
        if base_tp1 <= 0:
             base_tp1 = _envf("ROCKET_TP1_ATR_MULT", "0.78")
             
        # Scale
        new_tp1 = base_tp1 * ratio
        
        # Безопасный кэп для TP1 (не отодвигать слишком далеко, например не дальше 2.0 ATR)
        new_tp1 = clamp(new_tp1, 0.5, 3.0)
        
        cfgd["ROCKET_TP1_ATR_MULT"] = new_tp1
        cfgd["slq_tp1_mult"] = new_tp1
        cfgd["slq_tp1_ratio"] = ratio

    return cfgd
