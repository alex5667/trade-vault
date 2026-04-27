from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Optional


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class CVDDecision:
    quarantine_active: bool
    quarantine_until_ms: int
    reason: str


class CVDConsistencyGuard:
    """
    Detects 'two baselines' / jumps in CVD series and activates quarantine.
    During quarantine: downstream should avoid CVD-derived signals; use volume-delta instead.
    """

    def __init__(self) -> None:
        self.enable = os.getenv("CVD_QUARANTINE_ENABLE", "0") == "1"
        self.abs_usd = float(os.getenv("CVD_JUMP_ABS_USD", "2000000"))  # 2M default
        self.rel_k = float(os.getenv("CVD_JUMP_REL_K", "8.0"))
        self.window_ms = int(os.getenv("CVD_JUMP_WINDOW_MS", "180000"))  # 3 min
        self.k_events = int(os.getenv("CVD_JUMP_K_EVENTS", "2"))
        self.quarantine_ttl_ms = int(os.getenv("CVD_QUARANTINE_TTL_MS", "900000"))  # 15 min
        self.med_window = int(os.getenv("CVD_MEDIAN_WINDOW", "120"))  # last N deltas

        self.prev_cvd: Dict[str, float] = {}
        self.delta_abs_hist: Dict[str, Deque[float]] = {}
        self.jump_ts: Dict[str, Deque[int]] = {}
        self.until: Dict[str, int] = {}
        self.reason: Dict[str, str] = {}

    def _median_abs_delta(self, sym: str) -> float:
        h = self.delta_abs_hist.get(sym)
        if not h:
            return 0.0
        xs = sorted(h)
        n = len(xs)
        if n == 0:
            return 0.0
        return xs[n // 2]

    def update(self, *, sym: str, ts_ms: int, cvd_now: float, delta_usd: float) -> CVDDecision:
        if not self.enable:
            return CVDDecision(False, 0, "")

        now = int(ts_ms or _now_ms())
        # maintain abs(delta) median
        h = self.delta_abs_hist.setdefault(sym, deque(maxlen=self.med_window))
        h.append(abs(float(delta_usd or 0.0)))
        med = self._median_abs_delta(sym)

        prev = self.prev_cvd.get(sym)
        self.prev_cvd[sym] = float(cvd_now or 0.0)
        if prev is None:
            return CVDDecision(self.is_active(sym, now), self.until.get(sym, 0), self.reason.get(sym, ""))

        jump = abs(float(cvd_now or 0.0) - float(prev or 0.0))
        thr = max(self.abs_usd, self.rel_k * max(1.0, med))
        if jump > thr:
            q = self.jump_ts.setdefault(sym, deque())
            q.append(now)
            # drop old
            while q and (now - q[0]) > self.window_ms:
                q.popleft()
            if len(q) >= self.k_events:
                self.until[sym] = now + self.quarantine_ttl_ms
                self.reason[sym] = f"cvd_jump>{thr:.0f} (jump={jump:.0f}, med_abs_delta={med:.0f})"
                q.clear()

        return CVDDecision(self.is_active(sym, now), self.until.get(sym, 0), self.reason.get(sym, ""))

    def is_active(self, sym: str, now_ms: Optional[int] = None) -> bool:
        if not self.enable:
            return False
        now = int(now_ms or _now_ms())
        return int(self.until.get(sym, 0) or 0) > now

