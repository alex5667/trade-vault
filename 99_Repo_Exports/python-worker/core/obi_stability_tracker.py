from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class _Sample:
    ts_ms: int
    obi: float
    dir: int  # 1=LONG, -1=SHORT, 0=NONE


class OBIStabilityTracker:
    """Track OBI persistence in a sliding window.

    update(...) returns:
      - stability_score in [0,1]: time-weighted fraction of window where
        direction matches and |obi| >= threshold.
      - stable_secs: continuous seconds in the current direction (with grace
        to ignore brief NONE intervals).

    The consumer (CryptoOrderflowService) can combine this with min_secs and a
    score threshold.
    """

    def __init__(
        self,
        window_ms: int = 3000,
        threshold: float = 0.4,
        deadband: float = 0.05,
        grace_ms: int = 250,
    ) -> None:
        self.window_ms = int(window_ms)
        self.threshold = float(threshold)
        self.deadband = float(deadband)
        self.grace_ms = int(grace_ms)

        self._samples: deque[_Sample] = deque()
        self._dir: int = 0
        self._dir_start_ts_ms: int | None = None
        self._last_ts_ms: int | None = None

    def reset(self) -> None:
        self._samples.clear()
        self._dir = 0
        self._dir_start_ts_ms = None
        self._last_ts_ms = None

    def update(self, *, ts_ms: int, obi: float) -> tuple[float, float]:
        """Add a new sample and return (stability_score, stable_secs)."""
        ts_ms = int(ts_ms)
        obi = float(obi)

        if self._last_ts_ms is not None and ts_ms < self._last_ts_ms:
            # Non-monotonic time: ignore sample (determinism over "fixing" time).
            return self._compute()
        self._last_ts_ms = ts_ms

        d = self._dir_from_obi(obi)
        self._samples.append(_Sample(ts_ms=ts_ms, obi=obi, dir=d))
        self._prune(now_ms=ts_ms)

        # Direction state machine (with grace for short NONE runs)
        if d in (1, -1):
            if self._dir != d:
                self._dir = d
                self._dir_start_ts_ms = ts_ms
            elif self._dir_start_ts_ms is None:
                self._dir_start_ts_ms = ts_ms
        else:
            # d == 0 (deadband). Keep direction unless deadband lasts > grace_ms.
            if self._dir != 0:
                last_non_none = self._last_non_none_ts()
                if last_non_none is None or (ts_ms - last_non_none) > self.grace_ms:
                    self._dir = 0
                    self._dir_start_ts_ms = None

        return self._compute()

    def _dir_from_obi(self, obi: float) -> int:
        if obi >= self.deadband:
            return 1
        if obi <= -self.deadband:
            return -1
        return 0

    def _last_non_none_ts(self) -> int | None:
        for s in reversed(self._samples):
            if s.dir != 0:
                return s.ts_ms
        return None

    def _prune(self, *, now_ms: int) -> None:
        if self.window_ms <= 0:
            self._samples.clear()
            return
        cut = int(now_ms) - int(self.window_ms)
        while self._samples and self._samples[0].ts_ms < cut:
            self._samples.popleft()

    def _compute(self) -> tuple[float, float]:
        # stable_secs
        stable_secs = 0.0
        if self._dir != 0 and self._dir_start_ts_ms is not None and self._last_ts_ms is not None:
            stable_secs = max(0.0, (self._last_ts_ms - self._dir_start_ts_ms) / 1000.0)

        # stability_score (time-weighted)
        if len(self._samples) < 2 or self._dir == 0:
            return 0.0, float(stable_secs)

        total_dt = 0
        good_dt = 0
        prev = self._samples[0]
        for cur in list(self._samples)[1:]:
            dt = int(cur.ts_ms - prev.ts_ms)
            if dt > 0:
                total_dt += dt
                if prev.dir == self._dir and abs(prev.obi) >= self.threshold:
                    good_dt += dt
            prev = cur

        score = (good_dt / total_dt) if total_dt > 0 else 0.0
        if score < 0.0:
            score = 0.0
        elif score > 1.0:
            score = 1.0
        return float(score), float(stable_secs)
