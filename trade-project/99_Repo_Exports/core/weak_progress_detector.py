from __future__ import annotations

"""Weak progress history detector.

Purpose
-------
You already compute a *per-bar* WeakProgressSnapshot (core/weak_progress.py).
That snapshot is useful, but a single bar can be noise.

This detector maintains a small rolling history and exposes "trend" queries
used in Phase E (StrongConfirm):
  - recent_weak_count(): how many of last N bars were weak
  - recent_weak_frac(): fraction of weak bars in last N
  - weak_streak(): consecutive weak bars ending at the most recent bar

Integration pattern
-------------------
On each microbar close:
    wp = compute_weak_progress(bar, atr=..., delta_abs=...)
    runtime.last_wp = wp
    runtime.weak_progress_det.push(wp, ts_ms=bar_ts_ms)

Then:
    indicators["weak_recent_cnt"] = runtime.weak_progress_det.recent_weak_count()

Determinism / time
------------------
- We store timestamps but do not attempt to "fix" non-monotonic time.
- If ts_ms goes backward, we accept the sample but treat it as "now" in history
  ordering (deque append). In practice microbar close times should be monotonic.

This module is intentionally dependency-free.
"""

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Protocol


class WeakProgressSnapshotLike(Protocol):
    """Protocol for snapshots produced by core.weak_progress.compute_weak_progress."""

    weak_any: bool
    range_atr: float
    body_atr: float
    eff: float


@dataclass(frozen=True)
class WeakTrendSample:
    """Stored sample in the ring buffer."""

    ts_ms: int
    weak: bool
    range_atr: float
    body_atr: float
    eff: float


class WeakProgressDetector:
    """Rolling detector of weak progress bars.

    Parameters
    ----------
    maxlen:
        Maximum stored history length.
    recent_window:
        Window size (bars) for *recent_* queries.
    range_max_atr / body_max_atr:
        Optional thresholds (used only if caller passes raw bars and wants detector
        to decide weak itself). If you already pass WeakProgressSnapshot with
        weak_any set, these thresholds are not used.
    eff_max:
        Efficiency threshold for alt weak definition.
    """

    def __init__(
        self,
        *,
        maxlen: int = 50,
        recent_window: int = 5,
        range_max_atr: float = 0.30,
        body_max_atr: float = 0.35,
        eff_max: float = 0.02,
    ) -> None:
        self.maxlen = int(maxlen)
        self.recent_window = max(1, int(recent_window))
        self.range_max_atr = float(range_max_atr)
        self.body_max_atr = float(body_max_atr)
        self.eff_max = float(eff_max)

        self._buf: Deque[WeakTrendSample] = deque(maxlen=self.maxlen)

    def reset(self) -> None:
        self._buf.clear()

    def push(self, snap: WeakProgressSnapshotLike, *, ts_ms: int) -> WeakTrendSample:
        """Push an externally computed snapshot (recommended path)."""
        s = WeakTrendSample(
            ts_ms=int(ts_ms),
            weak=bool(getattr(snap, "weak_any", False)),
            range_atr=float(getattr(snap, "range_atr", 0.0) or 0.0),
            body_atr=float(getattr(snap, "body_atr", 0.0) or 0.0),
            eff=float(getattr(snap, "eff", 0.0) or 0.0),
        )
        self._buf.append(s)
        return s

    def push_raw(
        self,
        *,
        ts_ms: int,
        range_atr: float,
        body_atr: float,
        eff: float,
    ) -> WeakTrendSample:
        """Optional path: detector computes weak from raw ratios."""
        weak = (float(range_atr) <= self.range_max_atr) or (float(body_atr) <= self.body_max_atr) or (float(eff) <= self.eff_max)
        s = WeakTrendSample(ts_ms=int(ts_ms), weak=bool(weak), range_atr=float(range_atr), body_atr=float(body_atr), eff=float(eff))
        self._buf.append(s)
        return s

    def recent_weak_count(self) -> int:
        if not self._buf:
            return 0
        w = list(self._buf)[-self.recent_window :]
        return int(sum(1 for s in w if s.weak))

    def recent_weak_frac(self) -> float:
        if not self._buf:
            return 0.0
        w = list(self._buf)[-self.recent_window :]
        if not w:
            return 0.0
        return float(sum(1 for s in w if s.weak)) / float(len(w))

    def weak_streak(self) -> int:
        """Consecutive weak bars ending at the most recent bar."""
        if not self._buf:
            return 0
        streak = 0
        for s in reversed(self._buf):
            if s.weak:
                streak += 1
            else:
                break
        return streak

    def last(self) -> Optional[WeakTrendSample]:
        return self._buf[-1] if self._buf else None
