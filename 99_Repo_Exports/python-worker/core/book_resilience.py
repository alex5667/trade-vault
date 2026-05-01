# tick_flow_full/core/book_resilience.py
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Liquidity resilience / replenishment tracker.

Goal:
- Track how quickly top-of-book depth replenishes after a detected sweep.
- Provide deterministic, low-cost features for anti-fake-impulse / adverse-selection filters.

Usage:
- Call on_sweep(ts_ms, bid_depth_usd, ask_depth_usd) when sweep is detected (microbar close).
- Call on_book(ts_ms, bid_depth_usd, ask_depth_usd) on each L2 book update.
"""


from dataclasses import dataclass
from typing import Dict


@dataclass
class ResilienceSnapshot:
    res_active: int = 0
    res_recovered: int = 0
    res_recovery_ms: int = 0
    res_min_ratio: float = 1.0
    res_curr_ratio: float = 1.0
    res_speed_per_s: float = 0.0
    baseline_min_usd: float = 0.0
    min_min_usd: float = 0.0
    last_min_usd: float = 0.0
    sweep_ts_ms: int = 0


class BookResilienceTracker:
    def __init__(
        self,
        *,
        target_recovery_ratio: float = 0.85,
        max_window_ms: int = 15000,
        eps: float = 1e-9,
    ) -> None:
        self.target_recovery_ratio = float(target_recovery_ratio)
        self.max_window_ms = int(max_window_ms)
        self.eps = float(eps)
        self._snap = ResilienceSnapshot()

    def on_sweep(self, ts_ms: int, *, bid_depth_usd: float, ask_depth_usd: float) -> None:
        ts_ms = int(ts_ms or 0)
        b = float(bid_depth_usd or 0.0)
        a = float(ask_depth_usd or 0.0)
        baseline = float(min(b, a))
        if baseline <= self.eps or ts_ms <= 0:
            return
        self._snap = ResilienceSnapshot(
            res_active=1,
            res_recovered=0,
            res_recovery_ms=0,
            res_min_ratio=1.0,
            res_curr_ratio=1.0,
            res_speed_per_s=0.0,
            baseline_min_usd=baseline,
            min_min_usd=baseline,
            last_min_usd=baseline,
            sweep_ts_ms=ts_ms,
        )

    def on_book(self, ts_ms: int, *, bid_depth_usd: float, ask_depth_usd: float) -> None:
        ts_ms = int(ts_ms or 0)
        if self._snap.sweep_ts_ms <= 0:
            return
        if ts_ms <= 0 or ts_ms < self._snap.sweep_ts_ms:
            return

        baseline = float(self._snap.baseline_min_usd or 0.0)
        if baseline <= self.eps:
            return

        cur_min = float(min(float(bid_depth_usd or 0.0), float(ask_depth_usd or 0.0)))
        if cur_min <= self.eps:
            return

        elapsed_ms = int(ts_ms - self._snap.sweep_ts_ms)

        # update min observed depth after sweep
        if cur_min < self._snap.min_min_usd:
            self._snap.min_min_usd = cur_min

        # ratios vs baseline
        min_ratio = float(self._snap.min_min_usd / max(baseline, self.eps))
        curr_ratio = float(cur_min / max(baseline, self.eps))
        self._snap.res_min_ratio = float(max(0.0, min(1.5, min_ratio)))
        self._snap.res_curr_ratio = float(max(0.0, min(1.5, curr_ratio)))
        self._snap.last_min_usd = cur_min

        # recovery time (first cross of target ratio)
        if self._snap.res_recovered == 0 and curr_ratio >= self.target_recovery_ratio:
            self._snap.res_recovered = 1
            self._snap.res_recovery_ms = int(elapsed_ms)

        # speed proxy: (curr_ratio - min_ratio) / elapsed_s
        if elapsed_ms > 0:
            self._snap.res_speed_per_s = float((curr_ratio - min_ratio) / max(elapsed_ms / 1000.0, 1e-6))

        # deactivate after window OR after recovered + small grace
        if elapsed_ms >= self.max_window_ms:
            self._snap.res_active = 0
        elif self._snap.res_recovered == 1 and elapsed_ms >= max(250, min(self.max_window_ms, self._snap.res_recovery_ms + 250)):
            self._snap.res_active = 0

    def snapshot(self) -> Dict[str, float]:
        s = self._snap
        return {
            "res_active": int(s.res_active),
            "res_recovered": int(s.res_recovered),
            "res_recovery_ms": int(s.res_recovery_ms),
            "res_min_ratio": float(s.res_min_ratio),
            "res_curr_ratio": float(s.res_curr_ratio),
            "res_speed_per_s": float(s.res_speed_per_s),
            "res_baseline_min_usd": float(s.baseline_min_usd),
            "res_min_min_usd": float(s.min_min_usd),
            "res_last_min_usd": float(s.last_min_usd),
            "res_sweep_ts_ms": int(s.sweep_ts_ms),
        }
