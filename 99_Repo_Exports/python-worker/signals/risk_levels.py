"""
Модуль Уровней Риска - вычисление SL/TP для сигналов XAUUSD.

Предоставляет гибкое вычисление SL/TP с множественными режимами:
- Stop Loss: на основе ATR, Процента или Фиксированных пунктов
- Take Profit: соотношение Risk-Reward или мультипликаторы ATR

Поддерживает несколько уровней TP для частичной фиксации прибыли.
"""

import os
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

def parse_floats(csv: str) -> List[float]:
    """
    Парсит разделенные запятыми float-числа.
    """
    vals = []
    for x in (csv or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            vals.append(float(x))
        except ValueError:
            pass
    return vals


def _f(v, d=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


def _cfg_get(cfg: dict, *names, default=None):
    for n in names:
        if n in cfg and cfg[n] is not None:
            return cfg[n]
    # Check lowercase variant
    for n in names:
        ln = n.lower()
        if ln in cfg and cfg[ln] is not None:
            return cfg[ln]
    return default


def _cfg_str(cfg: dict, *names, default="") -> str:
    v = _cfg_get(cfg, *names, default=default)
    return str(v or default)


def _should_strict_rr(cfg: dict, stop_dist_override: Optional[float], eps: float = 1e-6) -> bool:
    # 1) SLQ flag
    try:
        if int(_cfg_get(cfg, "slq_used", "SLQ_USED", default=0) or 0) == 1:
            return True
    except Exception:
        pass

    # 2) any explicit stop override => not default
    try:
        if stop_dist_override is not None and _f(stop_dist_override, 0.0) > 0.0:
            return True
    except Exception:
        pass

    # 3) compare effective vs base if present
    base = _f(_cfg_get(cfg, "STOP_ATR_MULT_BASE", "stop_atr_mult_base", default=0.0), 0.0)
    eff = _f(_cfg_get(cfg, "STOP_ATR_MULT", "stop_atr_mult", default=0.0), 0.0)
    if base > 0.0 and eff > 0.0:
        # Significant expansion threshold (ratio >= 1.10)
        # We don't activate for narrowing (ratio < 1.0)
        ratio = eff / base
        if ratio >= 1.10:
            return True

    return False


# Simple counter for sampled logging if LEVELS_DEBUG=0
_COMPUTE_LEVELS_N = 0

def compute_levels(
    entry: float,
    atr: float,
    side: str,
    cfg: Dict,
    *,
    symbol: str = "",
    stop_dist_override: Optional[float] = None,
    tp1_dist_override: Optional[float] = None,
) -> Dict:
    """
    Вычисляет уровни SL и TP для торгового сигнала.
    """
    global _COMPUTE_LEVELS_N
    _COMPUTE_LEVELS_N += 1
    
    # Знак направления: +1 для LONG, -1 для SHORT
    sgn = 1 if side.upper() == "LONG" else -1
    atr = max(atr, 1e-9)  # Предотвращение деления на ноль
    
    # ═══════════════════════════════════════════════════════════════
    # Вычисление Stop Loss (с поддержкой переопределения)
    # ═══════════════════════════════════════════════════════════════
    
    stop_mode = _cfg_str(cfg, "STOP_MODE", "stop_mode", default="ATR").upper()
    
    stop_dist = 0.0
    if stop_dist_override is not None:
        try:
            od = float(stop_dist_override)
            if od > 0.0:
                stop_dist = od
        except Exception:
            stop_dist = 0.0

    if stop_dist <= 0.0:
        if stop_mode == "ATR":
            # Стоп на основе ATR
            stop_atr_mult = float(_cfg_get(cfg, "STOP_ATR_MULT", "stop_atr_mult", default=0.0) or 0.0)
            if stop_atr_mult > 0:
                stop_dist = stop_atr_mult * atr
        
        elif stop_mode == "PCT":
            # Стоп на основе процента
            stop_pct = float(_cfg_get(cfg, "STOP_PCT", "stop_pct", default=0.2))
            stop_dist = abs(entry) * stop_pct / 100.0
        
        else:  # POINTS
            # Стоп на основе фиксированных пунктов
            stop_points = float(_cfg_get(cfg, "STOP_POINTS", "stop_points", default=1.0))
            stop_dist = stop_points
    
    # Если stop_dist всё ещё 0/invalid (например, missing config), fail-open (пустой словарь)
    if stop_dist <= 1e-12:
         return {}

    # Цена SL
    sl = entry - sgn * stop_dist
    
    # ═══════════════════════════════════════════════════════════════
    # Вычисление уровней Take Profit
    # ═══════════════════════════════════════════════════════════════
    
    tp_mode_target = _cfg_str(cfg, "TP_MODE", "tp_mode", default="RR").upper()
    trail_profile = _cfg_str(cfg, "trail_profile", "TRAIL_PROFILE", default="")

    strict_rr = False
    tp_mode = tp_mode_target
    
    if tp_mode_target == "RR":
        strict_rr = _should_strict_rr(cfg, stop_dist_override=stop_dist_override)
        # Если стоп дефолтный -> считаем TP как раньше (ATR/rocket), 
        # НЕ игнорируя мультипликаторы
        if not strict_rr:
            tp_mode = "ATR"

    # For telemetry
    tp_mode_used = "RR_STRICT" if strict_rr else ("ATR_LEGACY" if tp_mode == "ATR" else tp_mode)

    # Sampled diagnostic or debug
    should_log = (os.getenv("LEVELS_DEBUG", "0") == "1") or (_COMPUTE_LEVELS_N % 100 == 1)
    if should_log:
        logger.info("levels: sym=%s mode=%s target=%s slq=%s stop_mult=%.3f base=%.3f trail=%s",
            symbol, tp_mode_used, tp_mode_target,
            _cfg_get(cfg, "slq_used", "SLQ_USED", default=0),
            _f(_cfg_get(cfg, "STOP_ATR_MULT", "stop_atr_mult", default=0.0), 0.0),
            _f(_cfg_get(cfg, "STOP_ATR_MULT_BASE", "stop_atr_mult_base", default=0.0), 0.0),
            trail_profile,
        )
    
    tps = []
    rr_list = []
    
    if tp_mode == "RR":
        # TP на основе соотношений Risk-Reward
        # Check for rocket_v1 compromise: TP1 remains ATR-based
        # lock_and_trail also uses ROCKET_TP1_ATR_MULT for TP1 positioning
        is_rocket_v1 = trail_profile in ("rocket_v1", "lock_and_trail")
        
        rrs_raw = _cfg_get(cfg, "TP_RR", "tp_rr", "tp_rr_levels", default="1,2,3")
        rrs = []
        if isinstance(rrs_raw, (list, tuple)):
             for x in rrs_raw:
                 try: rrs.append(float(x))
                 except: pass
        else:
            rrs = parse_floats(str(rrs_raw))

        if not rrs:
            rrs = [1.0, 2.0, 3.0]
        
        if is_rocket_v1:
            # Compromise: TP1 = ROCKET_TP1_ATR_MULT ATR, TP2+ = RR scaling
            tp1_mult = float(_cfg_get(cfg, "ROCKET_TP1_ATR_MULT", "rocket_tp1_atr_mult", default=0.78))
            tp1_dist = tp1_mult * atr
            # ── FIX: enforce min R:R floor so TP1 is never closer than SL ──
            _min_rr_floor = _f(_cfg_get(cfg, "TP1_MIN_RR_FLOOR", "tp1_min_rr_floor", default=1.0), 1.0)
            if _min_rr_floor > 0 and stop_dist > 0 and tp1_dist < stop_dist * _min_rr_floor:
                tp1_dist = stop_dist * _min_rr_floor
            tp1_price = entry + sgn * tp1_dist
            tps.append(tp1_price)
            rr_list.append(tp1_dist / stop_dist if stop_dist > 0 else 0.0)
            
            for rr in rrs[1:]:
                tp_price = entry + sgn * (rr * stop_dist)
                tps.append(tp_price)
                rr_list.append(rr)
        else:
            # Regular Strict RR: all levels scale by RR
            for rr in rrs:
                tp_price = entry + sgn * (rr * stop_dist)
                tps.append(tp_price)
                rr_list.append(rr)

        # Опционально: переопределить только дистанцию TP1 (оставить TP2+ как настроено).
        if tp1_dist_override is not None:
            try:
                d = float(tp1_dist_override)
                if d > 0.0 and len(tps) > 0:
                    tps[0] = entry + sgn * d
                    # Сохранять RR[0] согласованным со stop_dist (полезно для гейтов/телеметрии).
                    rr_list[0] = (d / stop_dist) if stop_dist > 0 else rr_list[0]
            except Exception:
                pass
    
    else:  # ATR / OTHER
        # TP на основе мультипликаторов ATR
        # Проверяем, используется ли профиль rocket_v1 или lock_and_trail
        trail_profile = _cfg_str(cfg, "trail_profile", "TRAIL_PROFILE", default="")
        is_rocket_v1 = trail_profile in ("rocket_v1", "lock_and_trail")
        
        if is_rocket_v1:
            # Для rocket_v1: TP1 = ROCKET_TP1_ATR_MULT (def 0.78) ATR, остальные через RR
            tp1_mult = float(_cfg_get(cfg, "ROCKET_TP1_ATR_MULT", "rocket_tp1_atr_mult", default=0.78))
            tp1_dist = tp1_mult * atr
            # ── FIX: enforce min R:R floor so TP1 is never closer than SL ──
            _min_rr_floor = _f(_cfg_get(cfg, "TP1_MIN_RR_FLOOR", "tp1_min_rr_floor", default=1.0), 1.0)
            if _min_rr_floor > 0 and stop_dist > 0 and tp1_dist < stop_dist * _min_rr_floor:
                tp1_dist = stop_dist * _min_rr_floor
            tp1 = entry + sgn * tp1_dist
            tps.append(tp1)
            rr1 = tp1_dist / stop_dist if stop_dist > 0 else 0.0
            rr_list.append(rr1)
            
            # TP2 и TP3 через RR от stop_dist
            rrs_raw = _cfg_get(cfg, "TP_RR", "tp_rr", "tp_rr_levels", default="1,2,3")
            rrs = parse_floats(str(rrs_raw))
            if not rrs:
                rrs = [1.0, 2.0, 3.0]
                
            # Пропускаем первый RR (он уже использован для TP1 через ATR)
            for rr in rrs[1:]:
                tp_price = entry + sgn * (rr * stop_dist)
                tps.append(tp_price)
                rr_list.append(rr)
        else:
            # Обычный режим ATR
            raw_mults = _cfg_get(cfg, "TP_ATR_MULTS", "tp_atr_mults", default="0.6,1.0,1.5")
            mults = parse_floats(str(raw_mults))
            if not mults:
                mults = [0.6, 1.0, 1.5]
            
            for m in mults:
                tp_price = entry + sgn * (m * atr)
                tps.append(tp_price)
                # Вычислить эффективный RR относительно stop_dist
                rr = (m * atr) / stop_dist if stop_dist > 0 else 0.0
                rr_list.append(rr)

            # Опциональное переопределение TP1 даже в режиме ATR:
            if tp1_dist_override is not None:
                try:
                    d = float(tp1_dist_override)
                    if d > 0.0 and len(tps) > 0:
                        tps[0] = entry + sgn * d
                        rr_list[0] = (d / stop_dist) if stop_dist > 0 else rr_list[0]
                except Exception:
                    pass
    
    return {
        "sl": sl,
        "tp_levels": tps,
        "stop_dist": stop_dist,
        "rr": rr_list,
        "tp_mode_used": tp_mode_used,
        "mode": {
            "stop": stop_mode,
            "tp": tp_mode,
            "tp_mode_used": tp_mode_used
        }
    }


def format_sltp_text(
    entry: float,
    levels: Dict,
    side: str
) -> str:
    """
    Форматирует уровни SL/TP в человекочитаемый текст.
    
    Args:
        entry: Цена входа
        levels: Словарь уровней из compute_levels()
        side: Направление сделки
        
    Returns:
        Отформатированная строка
    
    Example:
        >>> text = format_sltp_text(1875.0, levels, 'LONG')
        >>> print(text)
        Entry: 1875.00
        SL: 1874.22 (-0.78, 0.60 ATR)
        TP1: 1875.78 (+0.78, RR 1.0)
        TP2: 1876.56 (+1.56, RR 2.0)
        TP3: 1878.12 (+3.12, RR 3.0)
    """
    lines = []
    
    # Entry
    lines.append(f"Entry: {entry:.2f}")
    
    # Stop Loss
    sl_dist = abs(levels['sl'] - entry)
    sl_sign = "+" if levels['sl'] > entry else "-"
    lines.append(
        f"SL: {levels['sl']:.2f} ({sl_sign}{sl_dist:.2f}, "
        f"{levels['stop_dist'] / (levels.get('atr', 1.0)):.2f} ATR)"
    )
    
    # Take Profits
    for i, (tp, rr) in enumerate(zip(levels['tp_levels'], levels['rr']), 1):
        tp_dist = abs(tp - entry)
        tp_sign = "+" if tp > entry else "-"
        lines.append(
            f"TP{i}: {tp:.2f} ({tp_sign}{tp_dist:.2f}, RR {rr:.1f})"
        )
    
    return "\n".join(lines)

