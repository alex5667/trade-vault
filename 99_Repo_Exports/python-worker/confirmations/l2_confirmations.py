from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level


def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _as_int(x: Any, default: int = 0) -> int:
    try:
        v = int(float(x))
    except Exception:
        return default
    return v


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return default
    if not _is_finite(v):
        return default
    return v


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _bps(a: float, b: float) -> float:
    # abs(a-b)/a * 10_000
    if a <= 0.0:
        return float("inf")
    return abs(a - b) / a * 10_000.0


@dataclass(frozen=True)
class ConfirmationResult:
    ok: bool
    reason_code: str
    parts: Dict[str, Any]

    @property
    def veto(self) -> bool:
        return not self.ok


def _levels_near_price(levels: Sequence[L2Level], price: float, near_bps: float) -> Sequence[L2Level]:
    if price <= 0.0:
        return []
    out = []
    for lv in levels:
        p = _safe_float(getattr(lv, "price", None), default=float("nan"))
        if not _is_finite(p) or p <= 0.0:
            continue
        if _bps(price, p) <= near_bps:
            out.append(lv)
    return out


def _sum_notional(levels: Sequence[L2Level]) -> float:
    s = 0.0
    for lv in levels:
        n = getattr(lv, "notional", None)
        if n is None:
            # fallback if only size is present
            p = _safe_float(getattr(lv, "price", None), 0.0)
            sz = _safe_float(getattr(lv, "size", None), 0.0)
            n = p * sz
        n = _safe_float(n, 0.0)
        s += n
    return s


def _min_wall_dist_bps(levels: Sequence[L2Level], price: float, min_wall_notional: float) -> float:
    if price <= 0.0:
        return float("inf")
    best = float("inf")
    for lv in levels:
        p = _safe_float(getattr(lv, "price", None), default=float("nan"))
        if not _is_finite(p) or p <= 0.0:
            continue
        n = getattr(lv, "notional", None)
        if n is None:
            sz = _safe_float(getattr(lv, "size", None), 0.0)
            n = p * sz
        n = _safe_float(n, 0.0)
        if n < min_wall_notional:
            continue
        d = _bps(price, p)
        if d < best:
            best = d
    return best


class L2ConfirmBreakout:
    """
    Handler-free L2 breakout confirmation.

    Contract:
      - returns pass/fail + stable reason_code + parts
      - never throws on NaN/Inf / malformed books (fail-closed)
    """

    def __init__(self, cfg: Any, now_ms: Optional[Callable[[], int]] = None) -> None:
        self.cfg = cfg
        self.now_ms = now_ms or (lambda: 0)

    def check(self, snap: Optional[L2Snapshot], side: int, price: float) -> ConfirmationResult:
        # side: +1 bull breakout, -1 bear breakout
        if snap is None:
            return ConfirmationResult(False, "no_l2", {"where": "breakout"})
        price = _safe_float(price, default=float("nan"))
        if not _is_finite(price) or price <= 0.0:
            return ConfirmationResult(False, "bad_price", {"where": "breakout"})

        ts_ms = _as_int(getattr(snap, "ts_ms", None), default=0)
        max_stale_ms = int(getattr(self.cfg, "L2_STALE_MS", getattr(self.cfg, "l2_stale_ms", 1500)))
        now = int(self.now_ms())
        if ts_ms > 0 and now > 0 and (now - ts_ms) > max_stale_ms:
            return ConfirmationResult(False, "stale_l2", {"age_ms": now - ts_ms, "max_stale_ms": max_stale_ms})

        near_bps = float(getattr(self.cfg, "L2_NEAR_BPS", getattr(self.cfg, "l2_near_bps", 8.0)))
        min_near_notional = float(getattr(self.cfg, "L2_MIN_NEAR_NOTIONAL", getattr(self.cfg, "l2_min_near_notional", 5000.0)))
        min_wall_notional = float(getattr(self.cfg, "L2_MIN_WALL_NOTIONAL", getattr(self.cfg, "l2_min_wall_notional", 15000.0)))
        max_opp_wall_bps = float(getattr(self.cfg, "L2_MAX_OPP_WALL_DIST_BPS", getattr(self.cfg, "l2_max_opp_wall_dist_bps", 12.0)))

        bids = getattr(snap, "bids", []) or []
        asks = getattr(snap, "asks", []) or []

        # Pick "support side" near price: for bull expect bids near price; for bear expect asks near price.
        support_levels = _levels_near_price(bids if side > 0 else asks, price, near_bps)
        support_notional = _sum_notional(support_levels)
        if support_notional < min_near_notional:
            return ConfirmationResult(
                False
                "low_support_near"
                {"support_notional": support_notional, "min_near_notional": min_near_notional, "near_bps": near_bps}
            )

        # Opposing wall too close is bad (resistance for bull -> asks wall; support wall for bear -> bids wall)
        opp_levels = asks if side > 0 else bids
        opp_wall_dist = _min_wall_dist_bps(opp_levels, price, min_wall_notional)
        if _is_finite(opp_wall_dist) and opp_wall_dist <= max_opp_wall_bps:
            return ConfirmationResult(
                False
                "opp_wall_too_close"
                {"opp_wall_dist_bps": opp_wall_dist, "max_opp_wall_dist_bps": max_opp_wall_bps, "min_wall_notional": min_wall_notional}
            )

        return ConfirmationResult(
            True
            "ok"
            {
                "support_notional": support_notional
                "opp_wall_dist_bps": opp_wall_dist
                "near_bps": near_bps
            }
        )


class L2ConfirmAbsorption:
    """
    Handler-free L2 absorption confirmation.

    Minimal semantics:
      - need a "wall" on the opposing side within a distance threshold
      - and *some* near-liquidity on the defensive side (to avoid empty books)
    """

    def __init__(self, cfg: Any, now_ms: Optional[Callable[[], int]] = None) -> None:
        self.cfg = cfg
        self.now_ms = now_ms or (lambda: 0)

    def check(self, snap: Optional[L2Snapshot], side: int, price: float) -> ConfirmationResult:
        # side: +1 absorption of sellers (bullish), -1 absorption of buyers (bearish)
        if snap is None:
            return ConfirmationResult(False, "no_l2", {"where": "absorption"})
        price = _safe_float(price, default=float("nan"))
        if not _is_finite(price) or price <= 0.0:
            return ConfirmationResult(False, "bad_price", {"where": "absorption"})

        ts_ms = _as_int(getattr(snap, "ts_ms", None), default=0)
        max_stale_ms = int(getattr(self.cfg, "L2_STALE_MS", getattr(self.cfg, "l2_stale_ms", 1500)))
        now = int(self.now_ms())
        if ts_ms > 0 and now > 0 and (now - ts_ms) > max_stale_ms:
            return ConfirmationResult(False, "stale_l2", {"age_ms": now - ts_ms, "max_stale_ms": max_stale_ms})

        near_bps = float(getattr(self.cfg, "L2_NEAR_BPS", getattr(self.cfg, "l2_near_bps", 8.0)))
        min_def_notional = float(getattr(self.cfg, "L2_MIN_DEF_NOTIONAL", getattr(self.cfg, "l2_min_def_notional", 3000.0)))
        min_wall_notional = float(getattr(self.cfg, "L2_MIN_WALL_NOTIONAL", getattr(self.cfg, "l2_min_wall_notional", 15000.0)))
        max_wall_dist_bps = float(getattr(self.cfg, "L2_MAX_WALL_DIST_BPS", getattr(self.cfg, "l2_max_wall_dist_bps", 10.0)))

        bids = getattr(snap, "bids", []) or []
        asks = getattr(snap, "asks", []) or []

        # Absorption expects an opposing wall close: for bullish absorption -> ask wall close (sellers absorbed)
        # for bearish absorption -> bid wall close (buyers absorbed).
        wall_levels = asks if side > 0 else bids
        wall_dist = _min_wall_dist_bps(wall_levels, price, min_wall_notional)
        if (not _is_finite(wall_dist)) or wall_dist > max_wall_dist_bps:
            return ConfirmationResult(
                False
                "no_close_wall"
                {"wall_dist_bps": wall_dist, "max_wall_dist_bps": max_wall_dist_bps, "min_wall_notional": min_wall_notional}
            )

        # Defensive near liquidity: bull needs bids near; bear needs asks near
        def_levels = _levels_near_price(bids if side > 0 else asks, price, near_bps)
        def_notional = _sum_notional(def_levels)
        if def_notional < min_def_notional:
            return ConfirmationResult(
                False
                "low_def_near"
                {"def_notional": def_notional, "min_def_notional": min_def_notional, "near_bps": near_bps}
            )

        return ConfirmationResult(
            True
            "ok"
            {"wall_dist_bps": wall_dist, "def_notional": def_notional, "near_bps": near_bps}
        )
