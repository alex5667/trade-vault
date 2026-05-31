from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class RollingRateMeter:
    """
    Tracks event rate over a sliding time window.
    Deterministically based on provided timestamps (ts_ms).
    """
    window_ms: int
    _ts: deque[int] = field(default_factory=deque)

    def add(self, ts_ms: int) -> None:
        """Add an event timestamp."""
        if ts_ms <= 0:
            return
        self._ts.append(int(ts_ms))

    def rate_per_min(self, now_ms: int) -> float:
        """Calculate rate (events/min) at the given time."""
        self._prune(now_ms)
        w = max(1, self.window_ms)
        # Scale count to per-minute rate
        # Example: 10 events in 30s window -> 20 events/min
        return 60000.0 * (len(self._ts) / float(w))

    def count(self, now_ms: int) -> int:
        """Return raw count of events in the window."""
        self._prune(now_ms)
        return len(self._ts)

    def _prune(self, now_ms: int) -> None:
        """Remove events outside the window [now_ms - window_ms, now_ms]."""
        cut = int(now_ms) - int(self.window_ms)
        # Ideally, _ts is sorted. Clean from left.
        while self._ts and self._ts[0] < cut:
            self._ts.popleft()
