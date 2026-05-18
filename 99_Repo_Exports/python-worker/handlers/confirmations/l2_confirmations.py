from __future__ import annotations

from dataclasses import dataclass
from typing import Any

"""
Легаси функциональный API, сохранен для совместимости.
Внутри делегирует вызовы class-валидаторам без привязки к хендлеру, чтобы избежать двойной поддержки.
"""

from common.reason_codes import ReasonCode
from signal_scoring.reason_registry import normalize_reason

from .l2_confirm_absorption import AbsorptionConfirmCfg, L2ConfirmAbsorption
from .l2_confirm_breakout import BreakoutConfirmCfg, L2ConfirmBreakout

# Стабильные структурированные коды (5.2)
OK = ReasonCode.OK.value
VETO_SPREAD_WIDE = ReasonCode.VETO_SPREAD_WIDE.value
VETO_WALL_NEAR = ReasonCode.VETO_WALL_NEAR.value
VETO_MP_CONTRA = ReasonCode.VETO_MP_CONTRA.value
VETO_TAKER_RATE_LOW = ReasonCode.VETO_TAKER_RATE_LOW.value
VETO_NO_WALL_OR_REFILL = ReasonCode.VETO_NO_WALL_OR_REFILL.value
VETO_NO_BLOCKING_CONFIRM = ReasonCode.VETO_NO_BLOCKING_CONFIRM.value


@dataclass(frozen=True)
class L2ConfirmResult:
    veto: bool
    score01: float
    reason: str
    parts: dict[str, Any]
    # 5.2 wire ABI
    reason_code: str = OK
    reason_u16: int = 0


def l2_confirm_breakout(
    *,
    ctx: Any,
    l2: Any,
    side: str,  # "buy" => breakout up, "sell" => breakout down
    level_price: float | None = None,
    max_spread_bps: float = 8.0,
    wall_near_bps: float = 6.0,
    min_wall_notional: float = 50_000.0,
    mp_contra_bps: float = 2.0,
) -> L2ConfirmResult:
    """
    Логика минимальная и детерминированная:
      - spread слишком широкий => veto (это общий quality veto)
      - wall near на стороне пробоя => НЕ veto (жёсткая унификация вынесена в class L2ConfirmBreakout)
      - microprice_shift contra (против направления пробоя) => veto
      - иначе score01 из грубой "чистоты" (1.0 - wall_dist_ratio)

    ВАЖНО (после унификации Variant B):
      - источником истины по veto "VETO_WALL_NEAR" является class-валидатор:
           handlers/confirmations/l2_confirm_breakout.py :: L2ConfirmBreakout.confirm()
      - эта functional-функция остаётся:
           * чистым детерминированным "feature/scoring" компонентом,
           * без самостоятельного veto по wall_dist (чтобы не было раздвоения поведения).

    Если вам нужно включить wall_near veto обратно (например, для legacy пайплайна),
    делайте это фичефлагом/обёрткой, но НЕ держите два разных решения одновременно.
    """
    parts: dict[str, Any] = {}
    # spread veto живет здесь (глобальное качество)
    spread_bps = getattr(ctx, "spread_bps", None)
    try:
        if spread_bps is not None:
            sb = float(spread_bps)
            parts["spread_bps"] = sb
            if sb > float(max_spread_bps):
                r, rc, u16 = normalize_reason(reason="spread_wide", reason_code=VETO_SPREAD_WIDE)
                return L2ConfirmResult(True, 0.0, r, parts, rc, u16)
    except Exception:
        pass

    # microprice contra veto живет здесь (сохраняет легаси поведение)
    mp = getattr(ctx, "microprice_shift_bps_20", None)
    try:
        if mp is not None:
            m = float(mp)
            parts["microprice_shift_bps_20"] = m
            if side == "buy" and m <= -float(mp_contra_bps):
                r, rc, u16 = normalize_reason(reason="mp_contra", reason_code=VETO_MP_CONTRA)
                return L2ConfirmResult(True, 0.0, r, parts, rc, u16)
            if side != "buy" and m >= float(mp_contra_bps):
                r, rc, u16 = normalize_reason(reason="mp_contra", reason_code=VETO_MP_CONTRA)
                return L2ConfirmResult(True, 0.0, r, parts, rc, u16)
    except Exception:
        pass

    # делегируем class-валидатору для stale + near_big_wall soft quality
    v = L2ConfirmBreakout(
        BreakoutConfirmCfg(
            l2_stale_ms=int(getattr(getattr(ctx, "cfg", None), "l2_stale_ms", 1500) or 1500),
            min_wall_notional=float(min_wall_notional),
            max_near_wall_bps=float(wall_near_bps),
        )
    )
    _lvl = level_price if level_price is not None else (getattr(ctx, "level_price", None) or 100.0)
    res = v.confirm(ctx=ctx, side=side, level_price=_lvl, l2=l2)
    parts.update(res.parts or {})
    parts.update(res.flags or {})
    # Унификация Variant B: VETO_WALL_NEAR из class-валидатора НЕ пробрасывается как veto.
    # Вместо этого эмитируем near_wall=1 и wall_dist_bps для downstream scoring/analytics.
    if res.veto and res.reason_code != VETO_WALL_NEAR:
        return L2ConfirmResult(True, 0.0, res.reason_code or "veto", parts, res.reason_code, res.reason_u16)
    # После wall_near: добавляем аналитические фичи в parts
    if "near_wall_bps" in parts:
        parts["near_wall"] = 1
        parts.setdefault("wall_dist_bps", parts["near_wall_bps"])
    # soft fail => понижаем скор, но не вето
    score01 = float(getattr(res, "score01", 1.0))
    score01 = max(0.0, min(1.0, score01))
    r, rc, u16 = normalize_reason(reason="ok", reason_code=OK)
    return L2ConfirmResult(False, score01, r, parts, rc, u16)

def l2_confirm_absorption(
    *,
    ctx: Any,
    l2: Any,
    side: str,  # "buy" => absorption of buys, "sell" => absorption of sells
    min_taker_rate: float = 0.05,
    refill_min: float = 0.3,
    wall_near_bps: float = 8.0,
    min_wall_notional: float = 75_000.0,
    mp_contra_bps: float = 1.5,
) -> L2ConfirmResult:
    """
    Абсорб — более строгий (ваш 3.3):
      нужно минимум ДВА независимых подтверждения:
        A) (wall_here OR refill)
        B) (mp_contra OR micro_proxy)
      плюс минимальный taker_rate (иначе "абсорбить нечего")
    """
    parts: dict[str, Any] = {}

    # 1) taker-rate veto (детерминированный)
    taker = getattr(ctx, "taker_rate_ema", None)
    if taker is None:
        taker = getattr(ctx, "taker_rate", None)
    try:
        if taker is not None:
            t = float(taker)
            parts["taker_rate"] = t
            if t < float(min_taker_rate):
                r, rc, u16 = normalize_reason(reason="taker_rate_low", reason_code=VETO_TAKER_RATE_LOW)
                return L2ConfirmResult(True, 0.0, r, parts, rc, u16)
    except Exception:
        pass

    # 2) делегируем class-валидатору для флагов wall_here/micro_proxy/mp_contra (и stale тоже)
    v = L2ConfirmAbsorption(
        AbsorptionConfirmCfg(
            l2_stale_ms=int(getattr(getattr(ctx, "cfg", None), "l2_stale_ms", 1500) or 1500),
            min_wall_notional=float(min_wall_notional),
            level_band_bps=1.5,  # узкая полоса вокруг уровня
            min_taker_rate=float(min_taker_rate),
            min_refill_ratio=float(refill_min),
        )
    )
    res = v.confirm(ctx=ctx, side=side, level_price=ctx.level_price if hasattr(ctx, 'level_price') else 100.0)
    parts.update(res.parts or {})
    parts.update(res.flags or {})
    if res.veto:
        return L2ConfirmResult(True, 0.0, res.reason_code or "veto", parts, res.reason_code, res.reason_u16)

    # 3) энфорсим 3.3: два независимых подтверждения
    refill = getattr(ctx, "refill_ratio", None)
    try:
        if refill is not None:
            parts["refill_ratio"] = float(refill)
    except Exception:
        pass

    wall_here = bool(parts.get("wall_here") or False)
    mp_contra = bool(parts.get("mp_contra") or False)
    micro_proxy = bool(parts.get("micro_proxy") or False)
    src_a = bool(wall_here or (refill is not None and float(refill) >= float(refill_min)))
    src_b = bool(mp_contra or micro_proxy)
    parts["src_a_wall_or_refill"] = int(src_a)
    parts["src_b_mp_or_proxy"] = int(src_b)
    if not src_a:
        r, rc, u16 = normalize_reason(reason="no_wall_or_refill", reason_code=VETO_NO_WALL_OR_REFILL)
        return L2ConfirmResult(True, 0.0, r, parts, rc, u16)
    if not src_b:
        r, rc, u16 = normalize_reason(reason="no_blocking_confirm", reason_code=VETO_NO_BLOCKING_CONFIRM)
        return L2ConfirmResult(True, 0.0, r, parts, rc, u16)

    # 4) score01 детерминированная агрегация
    score01 = 0.5
    if wall_here:
        score01 += 0.25
    try:
        if refill is not None and float(refill) >= float(refill_min):
            score01 += 0.15
    except Exception:
        pass
    if mp_contra:
        score01 += 0.10
    if micro_proxy:
        score01 += 0.10
    score01 = max(0.0, min(1.0, float(score01)))
    parts["score01"] = score01
    r, rc, u16 = normalize_reason(reason="ok", reason_code=OK)
    return L2ConfirmResult(False, score01, r, parts, rc, u16)
