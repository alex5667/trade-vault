from __future__ import annotations

"""layer_enforce_gate.py

Применяет правила Layer A/B/C к сигналу на entry-decision time.
Использует LayerEnforceReader для чтения mode+symbols+bundle из Redis.

Layer A (hard veto) — pre-trade доступные метрики:
  - slippage_bps_est ≥ THRESHOLD_SLIP  → VETO
  - spread_bps_at_entry ≥ THRESHOLD_SPR → VETO
  (adverse_microspike@100ms — НЕ доступен pre-trade; требует delay-and-confirm)

Layer B (soft clamp) — возвращает clamp_factor ∈ [MIN_CLAMP, 1.0]:
  - slippage ∈ [SLIP_LO, SLIP_HI) → ×SLIP_CLAMP
  - spread   ∈ [SPR_LO, SPR_HI)   → ×SPR_CLAMP
  - LONG && regime ∉ CONFIRM_LONG → ×LONG_CLAMP

Layer C (confluence) — требует ≥ MIN_LEGS active legs.

Все правила читаются из ENV (по дефолту = те же, что в shadow).
Default mode для каждого слоя: shadow-after-promote (логирует, не блокирует).
"""

import logging
import math
import os
from dataclasses import dataclass
from typing import Any

from .layer_enforce_reader import LayerEnforceReader

logging.getLogger(__name__).addHandler(logging.NullHandler())
log = logging.getLogger(__name__)


@dataclass
class EnforceInputs:
    symbol: str
    side: str                        # "LONG" / "SHORT"
    slippage_bps_est: float | None
    spread_bps_at_entry: float | None
    regime: str | None
    features: dict[str, Any]         # features payload (qimb_wmean, lob_dw_obi_z, ...)


@dataclass
class EnforceResult:
    veto: bool = False
    veto_reasons: tuple[str, ...] = ()
    clamp_factor: float = 1.0
    clamp_reasons: tuple[str, ...] = ()
    layer_a_active: bool = False
    layer_b_active: bool = False
    layer_c_active: bool = False
    legs_active_c: int = 0
    legs_present_c: int = 0
    notes: dict[str, Any] | None = None


def _env_f(k: str, d: float) -> float:
    try: return float(os.environ.get(k, str(d)))
    except Exception: return d

def _env_i(k: str, d: int) -> int:
    try: return int(os.environ.get(k, str(d)))
    except Exception: return d

def _env_csv(k: str, d: str) -> tuple[str, ...]:
    raw = os.environ.get(k, d)
    return tuple(s.strip().lower() for s in raw.split(",") if s.strip())


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


# =================== Layer A ===================
def _is_default_fallback_slip(slip: float) -> bool:
    """estimate_slippage_bps возвращает default_bps когда EMA не загрузилась
    (redis down, empty key, bad TS). В этом случае значение НЕ информативно
    и rule на slippage должна skip."""
    default_bps = _env_f("EDGE_SLIPPAGE_BPS_DEFAULT", 4.0)
    tolerance   = _env_f("OF_LAYER_ENFORCE_SLIP_DEFAULT_TOL_BPS", 0.05)
    return abs(slip - default_bps) <= tolerance


def _eval_layer_a(inp: EnforceInputs) -> tuple[bool, list[str]]:
    """VETO если pre-trade slippage/spread выше порога.

    Safeguard: если slippage равен EDGE_SLIPPAGE_BPS_DEFAULT (EMA fallback),
    то это "no data" — slip rule skip (избегаем массового VETO при redis-проблеме).
    """
    veto_reasons: list[str] = []
    thr_slip = _env_f("OF_LAYER_A_ENFORCE_SLIPPAGE_BPS", 2.0)
    thr_spr  = _env_f("OF_LAYER_A_ENFORCE_SPREAD_BPS", 1.5)
    if (inp.slippage_bps_est is not None
            and inp.slippage_bps_est >= thr_slip
            and not _is_default_fallback_slip(inp.slippage_bps_est)):
        veto_reasons.append("la_slippage")
    if inp.spread_bps_at_entry is not None and inp.spread_bps_at_entry >= thr_spr:
        veto_reasons.append("la_spread")
    return (bool(veto_reasons), veto_reasons)


# =================== Layer B ===================
def _eval_layer_b(inp: EnforceInputs) -> tuple[float, list[str]]:
    """Возвращает (clamp_factor, reasons). 1.0 = без clamp."""
    factor = 1.0
    reasons: list[str] = []

    slip_lo    = _env_f("OF_LAYER_B_ENFORCE_SLIP_LO", 1.0)
    slip_hi    = _env_f("OF_LAYER_B_ENFORCE_SLIP_HI", 2.0)
    slip_clamp = _env_f("OF_LAYER_B_ENFORCE_SLIP_CLAMP", 0.5)
    spr_lo     = _env_f("OF_LAYER_B_ENFORCE_SPR_LO", 0.8)
    spr_hi     = _env_f("OF_LAYER_B_ENFORCE_SPR_HI", 1.5)
    spr_clamp  = _env_f("OF_LAYER_B_ENFORCE_SPR_CLAMP", 0.5)
    long_clamp = _env_f("OF_LAYER_B_ENFORCE_LONG_CLAMP", 0.7)
    confirm_long = _env_csv("OF_LAYER_B_ENFORCE_CONFIRM_LONG", "uptrend,trend_up")
    min_clamp  = _env_f("OF_LAYER_B_ENFORCE_MIN_CLAMP", 0.2)

    if (inp.slippage_bps_est is not None
            and slip_lo <= inp.slippage_bps_est < slip_hi
            and not _is_default_fallback_slip(inp.slippage_bps_est)):
        factor *= slip_clamp
        reasons.append("lb_slip_mid")
    if inp.spread_bps_at_entry is not None and spr_lo <= inp.spread_bps_at_entry < spr_hi:
        factor *= spr_clamp
        reasons.append("lb_spr_mid")

    regime = (inp.regime or "").lower()
    if inp.side == "LONG" and regime not in confirm_long:
        factor *= long_clamp
        reasons.append("lb_long_no_htf")

    if factor < min_clamp:
        factor = min_clamp
    if factor > 1.0:
        factor = 1.0
    return (factor, reasons)


# =================== Layer C ===================
def _leg_active_numeric(value: float | None, threshold: float, side: str) -> bool | None:
    if value is None:
        return None
    if side == "LONG":
        return value >= threshold
    if side == "SHORT":
        return value <= -threshold
    return None


def _eval_layer_c(
    inp: EnforceInputs,
    reader: LayerEnforceReader | None = None,
) -> tuple[bool, list[str], int, int]:
    """VETO если active legs < MIN_LEGS. Возвращает (veto, reasons, active, present).

    Hot-overrides leg keys из Redis (записываются autotuner'ом без рестарта)
    имеют приоритет над ENV. Если не установлены — используются ENV defaults.
    """
    min_legs = _env_i("OF_LAYER_C_ENFORCE_MIN_LEGS", 2)

    def _key(leg_n: int, env_name: str, default: str) -> str:
        if reader is not None:
            ov = reader.get_leg_key_override("C", leg_n)
            if ov:
                return ov
        return os.environ.get(env_name, default)

    leg1_key = _key(1, "OF_LAYER_C_ENFORCE_LEG1_KEY", "qimb_wmean")
    leg1_thr = _env_f("OF_LAYER_C_ENFORCE_LEG1_THR", 1.0)
    leg2_key = _key(2, "OF_LAYER_C_ENFORCE_LEG2_KEY", "lob_dw_obi_z")
    leg2_thr = _env_f("OF_LAYER_C_ENFORCE_LEG2_THR", 1.5)
    # liq_pressure_z НЕ существует в реальном payload. Канонический ключ —
    # liq_pressure_boost из ctx.indicators (см. core/of_confirm_engine.py:1957).
    # Sign-aware: boost > 0 = bullish, < 0 = bearish.
    leg3_key = _key(3, "OF_LAYER_C_ENFORCE_LEG3_KEY", "liq_pressure_boost")
    leg3_thr = _env_f("OF_LAYER_C_ENFORCE_LEG3_THR", 1.0)
    confirm_long  = _env_csv("OF_LAYER_C_ENFORCE_CONFIRM_LONG", "uptrend,trend_up")
    confirm_short = _env_csv("OF_LAYER_C_ENFORCE_CONFIRM_SHORT", "downtrend,trend_down")
    leg1_en = _env_i("OF_LAYER_C_ENFORCE_LEG1_ENABLED", 1)
    leg2_en = _env_i("OF_LAYER_C_ENFORCE_LEG2_ENABLED", 1)
    leg3_en = _env_i("OF_LAYER_C_ENFORCE_LEG3_ENABLED", 1)
    leg4_en = _env_i("OF_LAYER_C_ENFORCE_LEG4_ENABLED", 1)

    feats = inp.features or {}
    active = 0
    present = 0

    for key, thr, enabled in (
        (leg1_key, leg1_thr, leg1_en),
        (leg2_key, leg2_thr, leg2_en),
        (leg3_key, leg3_thr, leg3_en),
    ):
        if not enabled:
            continue
        v = _f(feats.get(key))
        if v is None:
            continue
        present += 1
        r = _leg_active_numeric(v, thr, inp.side)
        if r:
            active += 1

    if leg4_en:
        regime = (inp.regime or "").lower()
        if regime and regime not in ("", "na", "none", "null"):
            present += 1
            if inp.side == "LONG" and regime in confirm_long:
                active += 1
            elif inp.side == "SHORT" and regime in confirm_short:
                active += 1

    veto = active < min_legs
    reasons = [f"lc_legs_lt_min({active}<{min_legs})"] if veto else []
    return (veto, reasons, active, present)


def evaluate(reader: LayerEnforceReader, inp: EnforceInputs) -> EnforceResult:
    """Главная точка вызова. Возвращает решение к применению.
    Если все слои off для символа — result.veto=False, clamp=1.0.
    """
    res = EnforceResult()
    state_a = reader.get_for_symbol("A", inp.symbol)
    state_b = reader.get_for_symbol("B", inp.symbol)
    state_c = reader.get_for_symbol("C", inp.symbol)

    veto_reasons: list[str] = []
    clamp_reasons: list[str] = []
    clamp_factor = 1.0

    if state_a is not None:
        res.layer_a_active = True
        v, r = _eval_layer_a(inp)
        if v:
            veto_reasons.extend(r)

    if state_c is not None:
        res.layer_c_active = True
        v, r, active, present = _eval_layer_c(inp, reader=reader)
        res.legs_active_c = active
        res.legs_present_c = present
        if v:
            veto_reasons.extend(r)

    if state_b is not None:
        res.layer_b_active = True
        f, r = _eval_layer_b(inp)
        clamp_factor *= f
        clamp_reasons.extend(r)
        if clamp_factor < _env_f("OF_LAYER_B_ENFORCE_MIN_CLAMP", 0.2):
            clamp_factor = _env_f("OF_LAYER_B_ENFORCE_MIN_CLAMP", 0.2)

    res.veto = bool(veto_reasons)
    res.veto_reasons = tuple(veto_reasons)
    res.clamp_factor = clamp_factor
    res.clamp_reasons = tuple(clamp_reasons)
    res.notes = {
        "layer_a_mode": getattr(state_a, "mode", "off") if state_a else "off",
        "layer_b_mode": getattr(state_b, "mode", "off") if state_b else "off",
        "layer_c_mode": getattr(state_c, "mode", "off") if state_c else "off",
    }
    return res
