from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Any
import math

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


def clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def isfinite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def sanitize_book(levels: Iterable[L2Level], *, max_levels: int, min_notional: float) -> list[L2Level]:
    out: list[L2Level] = []
    for lv in levels:
        try:
            p = float(lv.price)
            n = float(getattr(lv, "notional", 0.0))
            if not math.isfinite(p) or p <= 0:
                continue
            if not math.isfinite(n) or n <= 0:
                continue
            if n < min_notional:
                continue
            out.append(lv)
        except Exception:
            continue
        if len(out) >= max_levels:
            break
    return out


def best_bid_ask(bids: list[L2Level], asks: list[L2Level]) -> tuple[Optional[float], Optional[float]]:
    bb = None
    ba = None
    for lv in bids:
        p = _f(lv.price, 0.0)
        if p > 0 and (bb is None or p > bb):
            bb = p
    for lv in asks:
        p = _f(lv.price, 0.0)
        if p > 0 and (ba is None or p < ba):
            ba = p
    return bb, ba


def spread_bps(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    if best_bid is None or best_ask is None:
        return None
    if best_bid <= 0 or best_ask <= 0:
        return None
    mid = 0.5 * (best_bid + best_ask)
    if mid <= 0:
        return None
    return (best_ask - best_bid) / mid * 10_000.0


def is_crossed(best_bid: Optional[float], best_ask: Optional[float]) -> bool:
    if best_bid is None or best_ask is None:
        return True
    return bool(best_bid >= best_ask)


def wall_distance_bps(
    *,
    ref_price: float,
    levels: list[L2Level],
    min_wall_notional: float,
    max_scan: int,
) -> Optional[float]:
    """
    Ищем ближайшую "стену" (level.notional >= min_wall_notional) и возвращаем min |p-ref| в bps.
    """
    if not math.isfinite(ref_price) or ref_price <= 0:
        return None
    best: Optional[float] = None
    m = min_wall_notional
    scanned = 0
    for lv in levels:
        if scanned >= max_scan:
            break
        scanned += 1
        n = _f(getattr(lv, "notional", 0.0), 0.0)
        if n < m:
            continue
        p = _f(lv.price, 0.0)
        if p <= 0:
            continue
        d = abs(p - ref_price) / ref_price * 10_000.0
        if best is None or d < best:
            best = d
    return best


def top_wall_notional(levels: list[L2Level], *, max_scan: int) -> float:
    best = 0.0
    scanned = 0
    for lv in levels:
        if scanned >= max_scan:
            break
        scanned += 1
        n = _f(getattr(lv, "notional", 0.0), 0.0)
        if n > best:
            best = n
    return float(best)


@dataclass(frozen=True)
class WallInfo:
    found: bool
    wall_price: Optional[float]
    wall_dist_bps: Optional[float]
    wall_notional: float
    wall_ratio: float


def find_near_wall(
    levels: list[L2Level],
    target_price: float,
    within_bps: float,
    top_n_for_baseline: int = 10,
) -> WallInfo:
    """
    Ищем уровень (wall) в пределах within_bps от target_price.
    Возвращаем ratio относительно median(notional) top-N.
    """
    if not levels or target_price <= 0.0 or within_bps <= 0.0:
        return WallInfo(False, None, None, 0.0, 0.0)

    target_price = float(target_price)
    within_bps = float(within_bps)

    # baseline: median notional top-N
    base_src = [float(lv.notional) for lv in levels[: max(1, int(top_n_for_baseline))] if math.isfinite(float(lv.notional)) and float(lv.notional) > 0.0]
    if not base_src:
        baseline = 1.0
    else:
        base_src.sort()
        baseline = base_src[len(base_src) // 2]
        baseline = baseline if baseline > 0.0 else 1.0

    best: Optional["L2Level"] = None
    best_dist = None
    for lv in levels:
        try:
            p = float(lv.price)
            if not (math.isfinite(p) and p > 0.0):
                continue
            d = abs(p - target_price) / target_price * 10_000.0
            if d <= within_bps:
                if best_dist is None or d < best_dist:
                    best = lv
                    best_dist = d
        except Exception:
            continue

    if best is None or best_dist is None:
        return WallInfo(False, None, None, 0.0, 0.0)

    wn = float(best.notional)
    ratio = (wn / baseline) if baseline > 0 else 0.0
    if not math.isfinite(ratio) or ratio < 0.0:
        ratio = 0.0
    return WallInfo(True, float(best.price), float(best_dist), float(wn), float(ratio))
