from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import os
import math


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


@dataclass(frozen=True)
class Adjustment:
    veto: bool
    mult01: float
    flags: list[str]
    reason: str
    parts: dict[str, float]


class KindValidator:
    def adjust(self, *, kind: str, ctx: Any, side: int, level_price: Optional[float]) -> Adjustment:
        return Adjustment(False, 1.0, [], "noop", {})


class BreakoutValidator(KindValidator):
    """
    Жёстче максимум (3.3):
      - fake breakout: есть microprice_shift, но нет taker continuation -> штраф
      - cancel_to_trade высокий + taker_rate низкий -> veto
    """
    def __init__(self) -> None:
        self.min_taker = float(os.getenv("BO_MIN_TAKER_RATE", "0.55"))
        self.fake_shift_bps = float(os.getenv("BO_FAKE_SHIFT_BPS", "1.2"))
        self.fake_penalty = float(os.getenv("BO_FAKE_PENALTY_MULT", "0.60"))
        self.c2t_bad = float(os.getenv("BO_CANCEL_TO_TRADE_BAD", "4.0"))
        self.c2t_veto_taker = float(os.getenv("BO_CANCEL_TO_TRADE_VETO_TAKER", "0.35"))

    def adjust(self, *, kind: str, ctx: Any, side: int, level_price: Optional[float]) -> Adjustment:
        flags: list[str] = []
        parts: dict[str, float] = {}
        veto = False
        mult = 1.0

        taker = _f(getattr(ctx, "taker_rate_ema", None), 0.0)
        mp_shift = abs(_f(getattr(ctx, "microprice_shift_bps_20", None), 0.0))
        c2t = max(
            _f(getattr(ctx, "cancel_to_trade_bid_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_bid_20s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_20s", None), 0.0),
        )

        parts["bo_taker_rate"] = taker
        parts["bo_microprice_shift_bps"] = mp_shift
        parts["bo_cancel_to_trade"] = c2t

        # штраф за фейковый пробой (fake breakout penalty)
        if mp_shift >= self.fake_shift_bps and taker < self.min_taker:
            flags.append("BO_CONTINUATION_PENALTY")
            mult *= self.fake_penalty

        # спуф / cancel wall: veto если экстремальный cancel-to-trade + нет продолжения тейкерами
        if c2t >= self.c2t_bad and taker <= self.c2t_veto_taker:
            flags.append("BO_FAKE_BREAKOUT_VETO")
            veto = True

        return Adjustment(veto, _clamp01(mult), flags, "breakout_adjust", parts)


class AbsorptionValidator(KindValidator):
    """
    Жёстче максимум (3.3):
      было OR-heavy -> теперь "минимум два независимых источника"
        (wall_here OR refill) + (mp_contra OR micro_proxy) + taker_rate >= min
    """
    def __init__(self) -> None:
        self.min_taker = float(os.getenv("AB_MIN_TAKER_RATE", "0.45"))
        self.fail_mult = float(os.getenv("AB_FAIL_MULT", "0.0"))  # 0.0 => veto

    def adjust(self, *, kind: str, ctx: Any, side: int, level_price: Optional[float]) -> Adjustment:
        flags: list[str] = []
        parts: dict[str, float] = {}

        # источники подтверждений (ожидаем что engine/handler уже проставляет эти булевы флаги)
        wall_here = bool(getattr(ctx, "wall_here", False))
        refill = bool(getattr(ctx, "refill", False))
        mp_contra = bool(getattr(ctx, "mp_contra", False))
        micro_proxy = bool(getattr(ctx, "micro_proxy", False))
        taker = _f(getattr(ctx, "taker_rate_ema", None), 0.0)

        g1 = wall_here or refill
        g2 = mp_contra or micro_proxy

        parts["ab_g1_wall_or_refill"] = 1.0 if g1 else 0.0
        parts["ab_g2_mp_contra_or_proxy"] = 1.0 if g2 else 0.0
        parts["ab_taker_rate"] = taker

        if not g1 or not g2:
            flags.append("AB_NEED_2OF2_VETO")
        if taker < self.min_taker:
            flags.append("AB_LOW_TAKER_VETO")

        if (not g1) or (not g2) or (taker < self.min_taker):
            return Adjustment(True, 0.0, flags, "absorption_veto_two_sources", parts)

        return Adjustment(False, 1.0, flags, "absorption_ok", parts)


class OBISpikeValidator(KindValidator):
    """
    Жёстче максимум (3.3):
      - анти-спуф: требуем obi_sustained + низкий cancel_to_trade, иначе штраф/в крайних случаях veto
      - spread scaling (не только hard reject)
    """
    def __init__(self) -> None:
        self.c2t_bad = float(os.getenv("OBI_CANCEL_TO_TRADE_BAD", "4.0"))
        self.c2t_veto = float(os.getenv("OBI_CANCEL_TO_TRADE_VETO", "7.0"))
        self.sustained_required = bool(int(os.getenv("OBI_SUSTAINED_REQUIRED", "1")))
        self.sustained_penalty = float(os.getenv("OBI_SUSTAINED_PENALTY_MULT", "0.65"))
        self.c2t_penalty = float(os.getenv("OBI_C2T_PENALTY_MULT", "0.55"))
        self.spread_scale_bps = float(os.getenv("OBI_SPREAD_SCALE_BPS", "40.0"))

    def adjust(self, *, kind: str, ctx: Any, side: int, level_price: Optional[float]) -> Adjustment:
        flags: list[str] = []
        parts: dict[str, float] = {}
        mult = 1.0
        veto = False

        obi_sust = bool(getattr(ctx, "obi_sustained", False))
        c2t = max(
            _f(getattr(ctx, "cancel_to_trade_bid_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_bid_20s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_20s", None), 0.0),
        )
        sp = _f(getattr(ctx, "spread_bps", None), 0.0)

        parts["obi_sustained"] = 1.0 if obi_sust else 0.0
        parts["obi_cancel_to_trade"] = c2t
        parts["obi_spread_bps"] = sp

        if self.sustained_required and not obi_sust:
            flags.append("OBI_NOT_SUSTAINED_PENALTY")
            mult *= self.sustained_penalty

        if c2t >= self.c2t_bad:
            flags.append("OBI_HIGH_CANCEL_TO_TRADE_PENALTY") # fallback
            mult *= self.c2t_penalty

        if c2t >= self.c2t_veto and (not obi_sust):
            flags.append("OBI_SPOOF_CANCEL_PENALTY")
            veto = True

        # скейлинг спреда (spread scaling, fail-open)
        if sp > 0:
            scale = max(0.35, 1.0 - min(1.0, sp / max(self.spread_scale_bps, 1e-9)) * 0.30)
            parts["obi_spread_scale"] = float(scale)
            mult *= scale

        return Adjustment(veto, _clamp01(mult), flags, "obi_adjust", parts)
