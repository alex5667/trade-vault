from __future__ import annotations

"""Book sanity & tick-to-book consistency (P5).

This module provides deterministic checks that *describe* market-data sanity.
It is intentionally split from time-based DataQualityGate to keep policies clean:
- time integrity: epoch ms, lag, quarantine
- book integrity: crossed book, NaNs, negative depth, trade outside BBO

The outputs are:
- flags (finite set of strings)
- numeric counters for monitoring

Important: By default this is MONITOR-ONLY.
A separate BookSanityGate decides whether to veto.
"""

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(d)
    if not math.isfinite(v):
        return float(d)
    return float(v)


def _finite(x: Any) -> bool:
    try:
        v = float(x)
    except Exception:
        return False
    return bool(math.isfinite(v))


@dataclass
class BookSanityResult:
    ok: bool
    flags: List[str]
    best_bid: float = 0.0
    best_ask: float = 0.0
    mid: float = 0.0


_ALLOWED_FLAGS = {
    "missing_bbo",
    "crossed_bbo",
    "bad_mid",
    "bad_depth",
    "nan_depth",
    "nan_px",
    "neg_qty",
}


def check_book_sanity(*, book: Any) -> BookSanityResult:
    """Check BBO and top-level depth sanity.

    Accepts either a typed BookSnapshot (preferred) or a dict-like raw book.
    """
    flags: List[str] = []
    bb = 0.0
    ba = 0.0

    try:
        if isinstance(book, dict):
            bb = _f(book.get("best_bid_px") or book.get("best_bid") or 0.0)
            ba = _f(book.get("best_ask_px") or book.get("best_ask") or 0.0)
        else:
            bb = _f(getattr(book, "best_bid_px", 0.0) or getattr(book, "best_bid", 0.0) or 0.0)
            ba = _f(getattr(book, "best_ask_px", 0.0) or getattr(book, "best_ask", 0.0) or 0.0)
    except Exception:
        bb, ba = 0.0, 0.0

    mid = 0.0
    if bb <= 0 or ba <= 0:
        flags.append("missing_bbo")
        mid = 0.0
    else:
        if bb >= ba:
            flags.append("crossed_bbo")
        mid = (bb + ba) / 2.0
        if mid <= 0 or (not math.isfinite(mid)):
            flags.append("bad_mid")

    # Depth checks (top5) — only when book provides them.
    try:
        if isinstance(book, dict):
            bids = book.get("top5_bids") or book.get("bids") or []
            asks = book.get("top5_asks") or book.get("asks") or []
        else:
            bids = getattr(book, "top5_bids", None) or []
            asks = getattr(book, "top5_asks", None) or []

        # Each level is (px, qty) list/tuple.
        for side in (bids, asks):
            for lv in list(side)[:10]:
                try:
                    px = lv[0]
                    qty = lv[1]
                except Exception:
                    continue
                if not _finite(px):
                    flags.append("nan_px")
                    break
                if not _finite(qty):
                    flags.append("nan_depth")
                    break
                if float(qty) < 0:
                    flags.append("neg_qty")
                    break
    except Exception:
        pass

    # Sanitize flags to finite set.
    flags = [f for f in flags if f in _ALLOWED_FLAGS]
    ok = len(flags) == 0

    return BookSanityResult(ok=ok, flags=flags, best_bid=float(bb), best_ask=float(ba), mid=float(mid))


def trade_outside_bbo(*, trade_px: float, best_bid: float, best_ask: float, eps_bps: float = 1.0) -> Tuple[bool, float]:
    """Detect stale-book symptom: trade price outside current BBO.

    eps_bps provides tolerance for small timing jitter.

    Returns: (is_outside, distance_bps)
    """
    px = _f(trade_px, 0.0)
    bb = _f(best_bid, 0.0)
    ba = _f(best_ask, 0.0)

    if px <= 0 or bb <= 0 or ba <= 0:
        return False, 0.0

    mid = (bb + ba) / 2.0
    if mid <= 0:
        return False, 0.0

    tol = (float(eps_bps) / 10_000.0) * mid

    if px > (ba + tol):
        dist = (px - ba) / mid * 10_000.0
        return True, float(dist)

    if px < (bb - tol):
        dist = (bb - px) / mid * 10_000.0
        return True, float(dist)

    return False, 0.0
