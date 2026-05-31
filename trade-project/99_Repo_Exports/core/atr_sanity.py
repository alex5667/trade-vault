from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _median(xs) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    ys = sorted(xs)
    mid = n // 2
    if n % 2 == 1:
        return float(ys[mid])
    return 0.5 * (float(ys[mid - 1]) + float(ys[mid]))


@dataclass
class ATRSanityResult:
    atr_used: float
    atr_bps: float
    bad: int
    reason: str
    used_last_good: int
    jump_event: int
    jump_count_window: int


class ATRSanity:
    def __init__(self, window: int = 120) -> None:
        self.window = int(os.getenv("ATR_SANITY_WINDOW", str(window)))
        self.jump_max_rel = float(os.getenv("ATR_JUMP_MAX_REL", "0.8"))
        self.max_age_ms = int(os.getenv("ATR_MAX_AGE_MS", "900000"))
        self.bps_min = float(os.getenv("ATR_BPS_MIN_SANITY", "2"))
        self.bps_max = float(os.getenv("ATR_BPS_MAX_SANITY", "800"))
        self.last_good_ttl_ms = int(os.getenv("ATR_LAST_GOOD_TTL_MS", "1800000"))

        # per-symbol state
        self._hist: Dict[str, Deque[float]] = {}
        self._last_good: Dict[str, float] = {}
        self._last_good_ts: Dict[str, int] = {}
        # per-symbol jump timestamps (for window count)
        self._jump_ts: Dict[str, Deque[int]] = {}
        self._jump_window_ms = int(os.getenv("ATR_JUMP_WINDOW_SEC", "3600")) * 1000

    def update(self, *, atr: float, px: float, age_ms: int, now_ms: int, symbol: str = "na") -> ATRSanityResult:
        atr = _f(atr, 0.0)
        px = _f(px, 0.0)
        age_ms = int(age_ms or 0)
        atr_bps = (atr / px * 10000.0) if (px > 0 and atr > 0) else 0.0
        sym = str(symbol or "na")

        bad = 0
        reason = ""
        jump_event = 0
        if atr <= 0:
            bad, reason = 1, "atr<=0"
        elif age_ms > self.max_age_ms:
            bad, reason = 1, f"stale>{self.max_age_ms}"
        elif atr_bps <= 0 or atr_bps < self.bps_min or atr_bps > self.bps_max:
            bad, reason = 1, f"atr_bps_oob:{atr_bps:.2f}"
        else:
            # jump check vs robust center (median)
            buf = self._hist.setdefault(sym, deque(maxlen=self.window))
            if len(buf) >= max(10, self.window // 4):
                med = _median(list(buf))
                if med > 0:
                    rel = abs(atr_bps - med) / max(1e-9, med)
                    if rel > self.jump_max_rel:
                        bad, reason = 1, f"jump_rel>{self.jump_max_rel:.3f}:{rel:.3f}"
                        jump_event = 1

        used_last_good = 0
        atr_used = atr
        if bad:
            # last-good fallback
            lg = self._last_good.get(sym)
            lg_ts = int(self._last_good_ts.get(sym, 0) or 0)
            if (lg is not None) and (now_ms - lg_ts) <= int(self.last_good_ttl_ms):
                atr_used = float(lg)
                used_last_good = 1
            else:
                atr_used = float(atr if atr > 0 else 0.0)
        else:
            atr_used = float(atr)
            # store last-good
            self._last_good[sym] = float(atr_used)
            self._last_good_ts[sym] = int(now_ms)
            # update history buffer
            buf = self._hist.setdefault(sym, deque(maxlen=self.window))
            buf.append(float(atr_bps))

        # maintain jump window count
        jq = self._jump_ts.setdefault(sym, deque())
        if jump_event:
            jq.append(int(now_ms))
        # drop old
        while jq and (int(now_ms) - int(jq[0])) > int(self._jump_window_ms):
            jq.popleft()
        jump_count_window = int(len(jq))

        atr_used_bps = (atr_used / px * 10000.0) if (px > 0 and atr_used > 0) else 0.0
        return ATRSanityResult(
            atr_used=float(atr_used),
            atr_bps=float(atr_used_bps),
            bad=int(bad),
            reason=str(reason),
            used_last_good=int(used_last_good),
            jump_event=int(jump_event),
            jump_count_window=int(jump_count_window),
        )

