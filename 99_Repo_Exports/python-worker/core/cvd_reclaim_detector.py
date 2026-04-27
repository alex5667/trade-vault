from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import Deque, List, Literal, Tuple
import math


Bias = Literal["LONG", "SHORT"]


def _median(xs: List[float]) -> float:
    """
    Robust median calculation.
    
    Args:
        xs: List of floats
        
    Returns:
        Median value, or 0.0 if empty
    """
    if not xs:
        return 0.0
    xs = sorted(xs)
    m = len(xs) // 2
    if len(xs) % 2 == 1:
        return float(xs[m])
    return 0.5 * (float(xs[m - 1]) + float(xs[m]))


@dataclass
class CVDReclaimResult:
    """
    Result of CVD reclaim evaluation.
    
    Attributes:
        ok: Whether CVD supports the reclaim (ratio >= threshold)
        ratio: Normalized CVD strength (sign-adjusted delta / baseline)
        cvd_delta: Raw cumulative volume delta in window
        n: Number of bars in evaluation window
        baseline: Median absolute delta (robust normalization)
        window_ms: Window duration in milliseconds
    """
    ok: bool
    ratio: float
    cvd_delta: float
    n: int
    baseline: float
    window_ms: int


class CVDReclaimTracker:
    """
    Stores microbar volume-delta points for CVD-reclaim evaluation.

    Each point:
      - ts_ms: microbar close timestamp (epoch ms)
      - delta: volume delta for that bar (taker_buy - taker_sell)

    Implementation goals:
      - deterministic
      - low memory
      - fast enough for rare evaluate() calls (only on reclaim)
    """

    def __init__(self, maxlen: int = 7200):
        """
        Initialize CVD tracker.
        
        Args:
            maxlen: Maximum number of bars to retain (default: 7200 = ~2h @ 1s)
        """
        self.buf: Deque[Tuple[int, float]] = deque(maxlen=max(128, int(maxlen)))
        self.last_ts: int = 0

    def push(self, *, ts_ms: int, delta: float) -> None:
        """
        Add a new microbar delta point.
        
        Args:
            ts_ms: Bar close timestamp (epoch ms)
            delta: Volume delta (taker_buy - taker_sell)
            
        Note:
            Ignores non-monotonic timestamps (bad time handling)
        """
        ts_ms = int(ts_ms)
        if ts_ms <= 0:
            return
        # bad time: ignore (upstream should quarantine/metrics)
        if self.last_ts and ts_ms < self.last_ts:
            return
        self.last_ts = ts_ms
        self.buf.append((ts_ms, float(delta)))

    def median_abs_delta(self, lookback_n: int) -> float:
        """
        Calculate robust baseline: median(|delta|) over last N bars.
        
        Args:
            lookback_n: Number of recent bars to consider
            
        Returns:
            Median absolute delta (robust to outliers)
        """
        if not self.buf:
            return 0.0
        n = max(8, int(lookback_n))
        tail = list(self.buf)[-n:]
        xs = [abs(d) for _, d in tail]
        return _median(xs)

    def sum_range(self, *, ts_from: int, ts_to: int, exclude_first_bar: bool) -> Tuple[float, int]:
        """
        Sum deltas in [ts_from, ts_to] inclusive.
        
        Args:
            ts_from: Window start timestamp (sweep_ts_ms)
            ts_to: Window end timestamp (reclaim_ts_ms)
            exclude_first_bar: If True, use (ts_from, ts_to] to avoid polluting 
                             the window with the sweep bar itself
                             
        Returns:
            Tuple of (sum_delta, count)
        """
        if not self.buf:
            return 0.0, 0
        ts_from = int(ts_from)
        ts_to = int(ts_to)
        if ts_to <= 0 or ts_from <= 0 or ts_to <= ts_from:
            return 0.0, 0

        s = 0.0
        n = 0
        for ts, d in self.buf:
            if ts < ts_from:
                continue
            if exclude_first_bar and ts <= ts_from:
                continue
            if ts > ts_to:
                break
            s += float(d)
            n += 1
        return s, n


class CVDReclaimDetector:
    """
    Computes whether CVD supports a structural reclaim.

    Inputs:
      - tracker: CVDReclaimTracker
      - bias: LONG/SHORT (direction_bias from reclaim event)
      - ts_from: sweep_ts_ms
      - ts_to: reclaim_ts_ms (hold_end bar ts)

    Normalization:
      ratio = (sign(bias) * sum_delta) / (baseline * sqrt(n))

    baseline = median(|delta|) over last lookback_n points (robust).
    """

    def __init__(
        self,
        *,
        ratio_min: float = 1.2,
        lookback_n: int = 120,
        exclude_first_bar: bool = True,
        baseline_floor: float = 1e-9,
    ):
        """
        Initialize CVD reclaim detector.
        
        Args:
            ratio_min: Minimum ratio threshold for confirmation
            lookback_n: Number of bars for baseline calculation
            exclude_first_bar: Whether to exclude sweep bar from window
            baseline_floor: Minimum baseline to prevent division by zero
        """
        self.ratio_min = float(ratio_min)
        self.lookback_n = int(lookback_n)
        self.exclude_first_bar = bool(exclude_first_bar)
        self.baseline_floor = float(baseline_floor)

    def evaluate(self, *, tracker: CVDReclaimTracker, bias: Bias, ts_from: int, ts_to: int) -> CVDReclaimResult:
        """
        Evaluate CVD support for reclaim event.
        
        Args:
            tracker: CVDReclaimTracker with historical delta data
            bias: Direction bias ("LONG" or "SHORT")
            ts_from: Sweep timestamp (window start)
            ts_to: Reclaim timestamp (window end)
            
        Returns:
            CVDReclaimResult with evaluation details
            
        Logic:
            - Computes CVD delta in [ts_from, ts_to] window
            - Normalizes by median absolute delta (robust baseline)
            - Applies directional sign (LONG expects positive, SHORT negative)
            - Requires ratio >= ratio_min and n >= 2 bars
        """
        bias_u = str(bias or "").upper()
        if bias_u not in ("LONG", "SHORT"):
            return CVDReclaimResult(False, 0.0, 0.0, 0, 0.0, 0)

        cvd_delta, n = tracker.sum_range(ts_from=int(ts_from), ts_to=int(ts_to), exclude_first_bar=self.exclude_first_bar)
        baseline = tracker.median_abs_delta(self.lookback_n)
        baseline = max(self.baseline_floor, float(baseline))

        # require at least 2 bars in window for stability
        if n < 2:
            return CVDReclaimResult(False, 0.0, float(cvd_delta), int(n), float(baseline), int(ts_to - ts_from))

        exp = 1.0 if bias_u == "LONG" else -1.0
        denom = baseline * math.sqrt(float(n))
        ratio = (float(cvd_delta) * exp) / max(self.baseline_floor, denom)

        ok = bool(ratio >= self.ratio_min)
        return CVDReclaimResult(ok, float(ratio), float(cvd_delta), int(n), float(baseline), int(ts_to - ts_from))
