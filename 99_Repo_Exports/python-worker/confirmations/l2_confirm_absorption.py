from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

from .result import ConfirmResult
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level


def _f(x: Any, default: Optional[float] = None) -> Optional[float]:
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
    """
    Handler-free L2 absorption validator.
    Produces structured flags for 3.3 absorption gating (two independent confirmations).
    """

    def __init__(self, cfg: Optional[AbsorptionConfirmCfg] = None) -> None:
        self.cfg = cfg or AbsorptionConfirmCfg()

    def _get_l2(self, ctx: Any) -> Optional[L2Snapshot]:
        return getattr(ctx, "l2", None) or getattr(ctx, "l2_snapshot", None) or getattr(ctx, "book", None)

    def _is_stale(self, ctx: Any) -> bool:
        ts = _f(getattr(ctx, "ts_ms", None))
        l2_ts = _f(getattr(ctx, "l2_ts_ms", None))
        if ts is None or l2_ts is None:
            return False
        return (ts - l2_ts) > float(self.cfg.l2_stale_ms)

    def confirm(self, *, ctx: Any, side: str, level_price: float) -> ConfirmResult:
        flags = {}
        reasons = []

        if self._is_stale(ctx):
            flags["l2_stale"] = True
            reasons.append("l2_stale")
            return ConfirmResult(passed=False, veto=True, flags=flags, reasons=reasons)

        l2 = self._get_l2(ctx)
        if l2 is None:
            flags["l2_missing"] = True
            return ConfirmResult(passed=True, veto=False, flags=flags, reasons=reasons)

        lvl = _f(level_price)
        if lvl is None or lvl <= 0:
            flags["bad_level"] = True
            return ConfirmResult(passed=True, veto=False, flags=flags, reasons=reasons)

        band = float(self.cfg.level_band_bps)
        band_abs = lvl * band / 10_000.0

        # Wall detection "here": sum notional in narrow band around level on the defending side.
        wall_notional = 0.0
        if side.lower() in ("sell", "down", "short"):
            # absorption on sell side -> defending bids at/near level
            bids = getattr(l2, "bids", None) or []
            for lv in bids:
                if not isinstance(lv, L2Level):
                    continue
                if lv.price is None:
                    continue
                if abs(lv.price - lvl) <= band_abs:
                    n = _f(getattr(lv, "notional", None))
                    if n is None:
                        p = _f(getattr(lv, "price", None)) or 0.0
                        s = _f(getattr(lv, "size", None)) or 0.0
                        n = p * s
                    if n is not None:
                        wall_notional += max(0.0, n)
        else:
            # absorption on buy side -> defending asks at/near level
            asks = getattr(l2, "asks", None) or []
            for lv in asks:
                if not isinstance(lv, L2Level):
                    continue
                if lv.price is None:
                    continue
                if abs(lv.price - lvl) <= band_abs:
                    n = _f(getattr(lv, "notional", None))
                    if n is None:
                        p = _f(getattr(lv, "price", None)) or 0.0
                        s = _f(getattr(lv, "size", None)) or 0.0
                        n = p * s
                    if n is not None:
                        wall_notional += max(0.0, n)

        flags["wall_notional_here"] = wall_notional
        if wall_notional >= self.cfg.min_wall_notional:
            flags["wall_here"] = True
            reasons.append("wall_here")

        # Refill / micro-proxy flags are typically computed elsewhere; accept from ctx if present.
        refill = getattr(ctx, "refill", None)
        if refill is not None:
            flags["refill"] = bool(refill)

        # Micro contra: microprice shift against direction (or explicit flag)
        if getattr(ctx, "mp_contra", None) is not None:
            flags["mp_contra"] = bool(getattr(ctx, "mp_contra"))
        else:
            mps = _f(getattr(ctx, "microprice_shift_bps", None), None)
            if mps is None:
                mps = _f(getattr(ctx, "microprice_shift", None), None)
            if mps is not None:
                # heuristic: for sell-absorption we want microprice shift up (contra sells), and vice versa
                if side.lower() in ("sell", "down", "short"):
                    flags["mp_contra"] = (mps > 0.0)
                else:
                    flags["mp_contra"] = (mps < 0.0)

        # Micro proxy: accept explicit (progress_blocked / adverse_ratio) if present
        if getattr(ctx, "micro_proxy", None) is not None:
            flags["micro_proxy"] = bool(getattr(ctx, "micro_proxy"))
        else:
            adverse = _f(getattr(ctx, "adverse_ratio_ema", None), None)
            if adverse is not None:
                flags["micro_proxy"] = (adverse >= 0.62)

        # Note: this confirm itself does NOT veto "OR-heavy"; 3.3 gating is enforced in kind_rules.apply_kind_rules.
        return ConfirmResult(passed=True, veto=False, flags=flags, reasons=reasons)
