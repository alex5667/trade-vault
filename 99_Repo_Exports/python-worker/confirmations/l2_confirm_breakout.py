from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

from .result import ConfirmResult
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level


def _f(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


@dataclass
class BreakoutConfirmCfg:
    l2_stale_ms: int = 1500
    min_wall_notional: float = 25_000.0
    max_near_wall_bps: float = 4.0


class L2ConfirmBreakout:
    """
    Handler-free L2 breakout validator.
    Returns structured flags; kind_rules.py consumes flags for fake-breakout heuristics.
    """

    def __init__(self, cfg: Optional[BreakoutConfirmCfg] = None) -> None:
        self.cfg = cfg or BreakoutConfirmCfg()

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
            # fail-open: no L2
            flags["l2_missing"] = True
            return ConfirmResult(passed=True, veto=False, flags=flags, reasons=reasons)

        px = _f(getattr(ctx, "price", None) or getattr(ctx, "last_price", None))
        lvl = _f(level_price)
        if px is None or lvl is None or lvl <= 0:
            flags["bad_inputs"] = True
            return ConfirmResult(passed=True, veto=False, flags=flags, reasons=reasons)

        # Detect "near wall" right after breakout: big opposite wall too close reduces quality.
        near_wall_bps = None
        wall_notional = None

        if side.lower() in ("buy", "up", "long"):
            # after up-breakout, nearest ask wall above level
            asks = getattr(l2, "asks", None) or []
            best = None
            for lv in asks:
                if not isinstance(lv, L2Level):
                    continue
                if lv.price is None:
                    continue
                if lv.price >= lvl:
                    if best is None or lv.price < best.price:
                        best = lv
            if best is not None:
                wall_notional = _f(getattr(best, "notional", None)) or _f(getattr(best, "price", 0.0)) * (_f(getattr(best, "size", 0.0)) or 0.0)
                near_wall_bps = abs(best.price - lvl) / lvl * 10_000.0
        else:
            # after down-breakout, nearest bid wall below level
            bids = getattr(l2, "bids", None) or []
            best = None
            for lv in bids:
                if not isinstance(lv, L2Level):
                    continue
                if lv.price is None:
                    continue
                if lv.price <= lvl:
                    if best is None or lv.price > best.price:
                        best = lv
            if best is not None:
                wall_notional = _f(getattr(best, "notional", None)) or _f(getattr(best, "price", 0.0)) * (_f(getattr(best, "size", 0.0)) or 0.0)
                near_wall_bps = abs(lvl - best.price) / lvl * 10_000.0

        if near_wall_bps is not None:
            flags["near_wall_bps"] = near_wall_bps
        if wall_notional is not None:
            flags["near_wall_notional"] = wall_notional

        if (near_wall_bps is not None and wall_notional is not None) and (wall_notional >= self.cfg.min_wall_notional) and (near_wall_bps <= self.cfg.max_near_wall_bps):
            flags["near_big_wall"] = True
            reasons.append("near_big_wall")
            # soft fail (not veto): downstream scoring will downscale
            return ConfirmResult(passed=False, veto=False, flags=flags, reasons=reasons)

        return ConfirmResult(passed=True, veto=False, flags=flags, reasons=reasons)
