from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any

from common.qf_codes import QF


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


@dataclass(frozen=True)
class MicroQualityOut:
    veto: bool
    mult01: float
    flags: list[int] = field(default_factory=list)  # uint16 codes
    reason: str = ""
    parts: dict[str, float] = field(default_factory=dict)


class MicroQualityValidator:
    """
    "самый последний слой жёсткости":
      - всё, что относится к micro/liquidity/spread/taker/cancel — здесь
      - ConfirmationsEngine = оркестратор: L2 + deps policies + geo + micro_quality
    """
    def __init__(self) -> None:
        # spread scaling
        self._spread_soft_bps = float(os.getenv("SPREAD_SOFT_BPS", "18.0"))
        self._spread_hard_bps = float(os.getenv("SPREAD_HARD_BPS", "45.0"))

        # скейлинг спреда
        self._bo_taker_min = float(os.getenv("BO_TAKER_MIN", "0.35"))
        self._bo_taker_veto_min = float(os.getenv("BO_TAKER_VETO_MIN", "0.18"))
        self._bo_cancel_veto = float(os.getenv("BO_CANCEL_TO_TRADE_VETO", "2.8"))
        self._bo_micro_shift_bps = float(os.getenv("BO_MICROSHIFT_BPS", "1.2"))
        self._bo_continuation_penalty = float(os.getenv("BO_CONTINUATION_PENALTY", "0.60"))

        # абсорб: требуем 2 источника + taker_min
        self._ab_taker_min = float(os.getenv("AB_TAKER_MIN", "0.22"))

        # OBI anti-spoof
        self._obi_cancel_bad = float(os.getenv("OBI_CANCEL_TO_TRADE_BAD", "2.5"))

        # штраф за экстремальный micro spoof
        self._ext_spoof_cancel_mult = float(os.getenv("EXT_SPOOF_CANCEL_MULT", "1.2"))

        # Penalty multipliers for OBI/extreme (were hardcoded magic numbers)
        self._obi_not_sustained_mult = float(os.getenv("OBI_NOT_SUSTAINED_MULT", "0.75"))
        self._obi_spoof_cancel_mult = float(os.getenv("OBI_SPOOF_CANCEL_MULT", "0.65"))
        self._ext_spoofy_micro_mult = float(os.getenv("EXT_SPOOFY_MICRO_MULT", "0.75"))

    def _get_cancel_to_trade(self, ctx: Any) -> float:
        # берём максимум доступных метрик (5s/20s, bid/ask)
        vals = [
            _f(getattr(ctx, "cancel_to_trade_bid_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_bid_20s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_20s", None), 0.0),
            _f(getattr(ctx, "l3_cancel_to_trade", None), 0.0),
        ]
        return float(max(vals) if vals else 0.0)

    def _get_taker_rate(self, ctx: Any) -> float:
        return float(_clamp01(_f(getattr(ctx, "taker_rate_ema", None), 0.5)))

    def _get_microshift(self, ctx: Any) -> float:
        return float(_f(getattr(ctx, "microprice_shift_bps_20", None), 0.0))

    def _spread_mult(self, spread_bps: float, flags: list[QualityFlag], parts: dict[str, float]) -> float:
        sp = float(spread_bps)
        parts["spread_bps_ctx"] = sp
        if sp <= self._spread_soft_bps:
            return 1.0
        if sp >= self._spread_hard_bps:
            flags.append(int(QF.SPREAD_HARD_VETO))
            parts["spread_hard_veto"] = 1.0
            return 0.0
        k = (sp - self._spread_soft_bps) / max(self._spread_hard_bps - self._spread_soft_bps, 1e-9)
        flags.append(int(QF.SPREAD_SOFT_PENALTY))
        mult = float(1.0 - 0.5 * _clamp01(k))
        parts["spread_penalty_mult01"] = mult
        return mult

    def validate(self, *, kind: str, ctx: Any) -> MicroQualityOut:
        flags: list[QualityFlag] = []
        parts: dict[str, float] = {}
        mult = 1.0

        spread_bps = _f(getattr(ctx, "spread_bps", None), 0.0)
        if spread_bps > 0:
            sm = self._spread_mult(spread_bps, flags, parts)
            if sm <= 0.0:
                return MicroQualityOut(True, 0.0, flags, "spread_hard_veto", parts)
            mult *= sm

        taker = self._get_taker_rate(ctx)
        cancel_to_trade = self._get_cancel_to_trade(ctx)
        microshift = self._get_microshift(ctx)
        parts["taker_rate_ema"] = float(taker)
        parts["cancel_to_trade"] = float(cancel_to_trade)
        parts["microprice_shift_bps_20"] = float(microshift)

        k = (kind or "")

        if k == "breakout":
            if cancel_to_trade >= self._bo_cancel_veto and taker <= self._bo_taker_veto_min:
                flags.append(int(QF.BO_FAKE_BREAKOUT_VETO))
                return MicroQualityOut(True, 0.0, flags, "bo_fake_breakout_veto", parts)
            if microshift >= self._bo_micro_shift_bps and taker < self._bo_taker_min:
                flags.append(int(QF.BO_CONTINUATION_PENALTY))
                mult *= float(self._bo_continuation_penalty)
                parts["bo_continuation_mult01"] = float(self._bo_continuation_penalty)

        elif k == "absorption":
            wall_or_refill = bool(getattr(ctx, "wall_here", False) or getattr(ctx, "refill", False))
            mp_or_proxy = bool(getattr(ctx, "mp_contra", False) or getattr(ctx, "micro_proxy", False))
            parts["ab_wall_or_refill"] = 1.0 if wall_or_refill else 0.0
            parts["ab_mp_or_proxy"] = 1.0 if mp_or_proxy else 0.0
            if not (wall_or_refill and mp_or_proxy):
                flags.append(int(QF.AB_NEED_2OF2_VETO))
                return MicroQualityOut(True, 0.0, flags, "ab_need_2of2_veto", parts)
            if taker < self._ab_taker_min:
                flags.append(int(QF.AB_LOW_TAKER_VETO))
                return MicroQualityOut(True, 0.0, flags, "ab_low_taker_veto", parts)

        elif k == "obi_spike":
            sust = bool(getattr(ctx, "obi_sustained", False))
            if not sust:
                flags.append(int(QF.OBI_NOT_SUSTAINED_PENALTY))
                mult *= self._obi_not_sustained_mult
                parts["obi_sustained_mult01"] = self._obi_not_sustained_mult
            if cancel_to_trade >= self._obi_cancel_bad:
                flags.append(int(QF.OBI_SPOOF_CANCEL_PENALTY))
                mult *= self._obi_spoof_cancel_mult
                parts["obi_cancel_mult01"] = self._obi_spoof_cancel_mult

        elif k == "extreme":
            if cancel_to_trade >= (self._bo_cancel_veto * self._ext_spoof_cancel_mult) and taker <= self._bo_taker_veto_min:
                flags.append(int(QF.EXT_SPOOFY_MICRO_PENALTY))
                mult *= self._ext_spoofy_micro_mult
                parts["ext_spoofy_mult01"] = self._ext_spoofy_micro_mult

        return MicroQualityOut(False, float(_clamp01(mult)), flags, "ok", parts)
