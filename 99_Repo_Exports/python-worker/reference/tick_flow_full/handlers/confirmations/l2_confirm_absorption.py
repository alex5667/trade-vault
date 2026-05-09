from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot

from .reason_utils import normalize_and_u16
from .result import ConfirmResult


def _f(x: Any, default: float | None = None) -> float | None:
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return v

@dataclass
class AbsorptionConfirmCfg:
    l2_stale_ms: int = 1500
    min_wall_notional: float = 30_000.0
    level_band_bps: float = 1.5

class L2ConfirmAbsorption:
    def __init__(self, cfg: AbsorptionConfirmCfg | None = None, **kwargs: Any) -> None:
        if cfg is None and kwargs:
            cfg = AbsorptionConfirmCfg(
                min_wall_notional=kwargs.get("min_wall_notional", 30_000.0),
                level_band_bps=kwargs.get("wall_within_bps", 1.5),
            )
        self.cfg = cfg or AbsorptionConfirmCfg()

    def _get_l2(self, ctx: Any) -> L2Snapshot | None:
        return getattr(ctx, "l2", None) or getattr(ctx, "l2_snapshot", None) or getattr(ctx, "book", None)

    def _is_stale(self, ctx: Any) -> bool:
        pre = getattr(ctx, "l2_is_stale", None)
        if pre is not None:
            return bool(pre)
        ts = _f(getattr(ctx, "ts_ms", None))
        l2_ts = _f(getattr(ctx, "l2_ts_ms", None))
        if ts is None or l2_ts is None:
            return False
        return (ts - l2_ts) > float(self.cfg.l2_stale_ms)

    def confirm(
        self,
        *,
        ctx: Any,
        side: int | str,
        level_price: float,
        l2: L2Snapshot | None = None,
        require_2ofn: bool = True,
        **_: Any,
    ) -> ConfirmResult:
        if isinstance(side, int):
            side = "buy" if side > 0 else "sell"
        side = side.lower()
        """
        Ручка раскатки (Rollout knob):
          - require_2ofn=True  -> строго (strict): (wall/refill) И (mp_contra/micro_proxy) + taker_rate min
          - require_2ofn=False -> легаси мягче: допускаем 1 источник, но downscale score01 и пишем flags
        """
        flags: dict[str, Any] = {}
        reasons: list[str] = []
        parts: dict[str, Any] = {}

        if self._is_stale(ctx):
            flags["l2_stale"] = True
            reasons.append("l2_stale")
            parts["l2_stale_ms"] = float(self.cfg.l2_stale_ms)
            rc, u16 = normalize_and_u16("VETO_L2_STALE")
            return ConfirmResult(
                passed=False,
                veto=True,
                score01=0.0,
                reason_code=rc,
                reason_u16=u16,
                parts=parts,
                flags=flags,
                reasons=reasons,
            )

        if l2 is None:
            l2 = self._get_l2(ctx)
        if l2 is None:
            flags["l2_missing"] = True
            parts["l2_missing"] = 1
            rc, u16 = normalize_and_u16("OK")
            return ConfirmResult(passed=True, veto=False, score01=0.5, reason_code=rc, reason_u16=u16, parts=parts, flags=flags, reasons=reasons)

        lvl = _f(level_price)
        if lvl is None or lvl <= 0:
            flags["bad_level"] = True
            parts["bad_level"] = 1
            rc, u16 = normalize_and_u16("OK")
            return ConfirmResult(passed=True, veto=False, score01=0.5, reason_code=rc, reason_u16=u16, parts=parts, flags=flags, reasons=reasons)

        band = float(self.cfg.level_band_bps)
        band_abs = lvl * band / 10_000.0

        # Детекция стены "здесь": сумма notional в узкой полосе вокруг уровня на защищающейся стороне.
        wall_notional = 0.0
        if side.lower() in ("buy", "up", "long"):
            # absorption buy: защищающаяся стена в асках
            asks = getattr(l2, "asks", None) or []
            for lv in asks:
                if not isinstance(lv, L2Level) or lv.price is None:
                    continue
                if abs(lv.price - lvl) <= band_abs:
                    wall_notional += _f(getattr(lv, "notional", None)) or (_f(getattr(lv, "price", 0.0)) or 0.0) * (_f(getattr(lv, "size", 0.0)) or 0.0)
        else:
            # absorption sell: защищающаяся стена в бидах
            bids = getattr(l2, "bids", None) or []
            for lv in bids:
                if not isinstance(lv, L2Level) or lv.price is None:
                    continue
                if abs(lv.price - lvl) <= band_abs:
                    wall_notional += _f(getattr(lv, "notional", None)) or (_f(getattr(lv, "price", 0.0)) or 0.0) * (_f(getattr(lv, "size", 0.0)) or 0.0)

        if wall_notional >= self.cfg.min_wall_notional:
            flags["wall_here"] = True
            reasons.append("wall_here")

        # Строгие 2-из-N источников:
        # A) wall_here ИЛИ refill
        # B) mp_contra ИЛИ micro_proxy
        refill = _f(getattr(ctx, "refill_ratio", None), None)
        if refill is not None:
            flags["refill_ratio"] = float(refill)
        refill_ok = bool(refill is not None and refill >= 0.6)

        mp_contra = bool(flags.get("mp_contra", False))
        micro_proxy = bool(flags.get("micro_proxy", False))

        src_a = bool(flags.get("wall_here", False) or refill_ok)
        src_b = bool(mp_contra or micro_proxy)
        flags["src_a_wall_or_refill"] = bool(src_a)
        flags["src_b_mp_or_proxy"] = bool(src_b)

        # Минимальная разумность taker-rate (если нет taker'ов — абсорбить нечего)
        taker = _f(getattr(ctx, "taker_rate_ema", None), None)
        if taker is None:
            taker = _f(getattr(ctx, "taker_rate", None), None)
        if taker is not None:
            flags["taker_rate"] = float(taker)
            if taker < 0.05:
                rc, u16 = normalize_and_u16("VETO_TAKER_RATE_LOW")
                return ConfirmResult(
                    passed=False, veto=True,
                    parts={}, flags=flags, reasons=reasons,
                    score01=0.0,
                    reason_code=rc, reason_u16=u16,
                )

        if require_2ofn:
            if not src_a:
                rc, u16 = normalize_and_u16("VETO_NO_WALL_OR_REFILL")
                return ConfirmResult(
                    passed=False, veto=True,
                    parts={}, flags=flags, reasons=reasons,
                    score01=0.0,
                    reason_code=rc, reason_u16=u16,
                )
            if not src_b:
                rc, u16 = normalize_and_u16("VETO_NO_BLOCKING_CONFIRM")
                return ConfirmResult(
                    passed=False, veto=True,
                    parts={}, flags=flags, reasons=reasons,
                    score01=0.0,
                    reason_code=rc, reason_u16=u16,
                )

        # score01: детерминированная агрегация
        score01 = 0.50
        if flags.get("wall_here", False):
            score01 += 0.25
        if refill_ok:
            score01 += 0.15
        if mp_contra:
            score01 += 0.10
        if micro_proxy:
            score01 += 0.10
        # legacy мягче: если нет src_a или src_b — downscale вместо veto
        if not require_2ofn:
            if not src_a:
                flags["legacy_missing_src_a"] = True
                score01 *= 0.70
            if not src_b:
                flags["legacy_missing_src_b"] = True
                score01 *= 0.70
        score01 = max(0.0, min(1.0, float(score01)))
        rc, u16 = normalize_and_u16("OK")
        return ConfirmResult(
            passed=True, veto=False,
            parts={}, flags=flags, reasons=reasons,
            score01=float(score01),
            reason_code=rc, reason_u16=u16,
        )
