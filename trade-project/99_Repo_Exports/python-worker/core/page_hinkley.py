"""
core/page_hinkley.py — Plan 3 / Step 3 streaming drift detector.

Page-Hinkley test for detecting a downward shift in the mean of an online
stream of metrics (e.g. rolling util_r, Brier score residual, slippage residual).

Reference: Page (1954) "Continuous Inspection Schemes"; commonly used by
drift-detection libraries such as river / scikit-multiflow.

Symmetric on demand: we generally care about *deterioration* — a lower mean of
edge / a higher mean of error. Construct one detector with the natural sign
(higher = worse) so a single threshold breach is unambiguously bad.

Usage:
    ph = PageHinkley(delta=0.005, threshold=2.5, min_n=100)
    for x in stream:
        warn = ph.update(x)
        if warn:
            handle_drift()
            ph.reset()  # caller decides reset cadence
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PageHinkleyState:
    n: int = 0
    mean: float = 0.0
    cumulative: float = 0.0     # m_t in classical notation
    min_cumulative: float = 0.0  # M_t = min over time of cumulative
    last_signal_n: int = 0       # n at the most recent signal (for cooldown)


class PageHinkley:
    """One-sided Page-Hinkley change detector.

    Args:
        delta:     allowable change per sample (slack). Smaller delta = more sensitive.
        threshold: triggers signal when (cumulative - min_cumulative) > threshold.
        min_n:     ignore signals before this many samples (warm-up).
        cooldown:  suppress repeat signals within `cooldown` samples of the last one.

    Convention: input x is "higher = worse" (e.g. error rate, |slippage_residual|).
    For edge-style metrics where lower = worse, pass `-x` (caller's job).
    """

    def __init__(
        self,
        *,
        delta: float = 0.005,
        threshold: float = 2.5,
        min_n: int = 100,
        cooldown: int = 50,
    ) -> None:
        if delta < 0:
            raise ValueError("delta must be >= 0")
        if threshold <= 0:
            raise ValueError("threshold must be > 0")
        if min_n < 1:
            raise ValueError("min_n must be >= 1")
        self.delta = float(delta)
        self.threshold = float(threshold)
        self.min_n = int(min_n)
        self.cooldown = int(cooldown)
        self.state = PageHinkleyState()

    def reset(self) -> None:
        """Wipe state — caller typically resets after handling a drift signal."""
        self.state = PageHinkleyState()

    def update(self, x: float) -> bool:
        """Feed one observation; return True iff a drift signal fires this step.

        Side effect: increments n, updates running mean and cumulative sum.
        """
        s = self.state
        s.n += 1
        # Incremental mean
        s.mean += (x - s.mean) / float(s.n)
        # Track upward drift: positive (x - mean - delta) accumulates when x trends up
        s.cumulative += x - s.mean - self.delta
        if s.cumulative < s.min_cumulative:
            s.min_cumulative = s.cumulative

        if s.n < self.min_n:
            return False
        if (s.n - s.last_signal_n) < self.cooldown and s.last_signal_n > 0:
            return False

        score = s.cumulative - s.min_cumulative
        if score > self.threshold:
            s.last_signal_n = s.n
            return True
        return False

    def score(self) -> float:
        """Current (cumulative - min_cumulative); diagnostic for dashboards."""
        s = self.state
        return s.cumulative - s.min_cumulative

    def n(self) -> int:
        return self.state.n


# ─── Convenience builders for trade-quality metrics ──────────────────────────


def detector_for_edge_drop(min_n: int = 100) -> PageHinkley:
    """Detect downward drift in util_r / edge_R.

    Caller feeds NEGATED values: ph.update(-util_r). delta=0.02 ≈ 0.02R per
    sample slack; threshold=2.5 ≈ ~125 samples of consistent -0.02R drift
    before signal (typical for a 1h cadence × 24h window).
    """
    return PageHinkley(delta=0.02, threshold=2.5, min_n=min_n, cooldown=50)


def detector_for_brier_increase(min_n: int = 100) -> PageHinkley:
    """Detect upward drift in Brier score (calibration deterioration).

    Brier in [0, 1]; delta=0.005 slack, threshold=2.5.
    """
    return PageHinkley(delta=0.005, threshold=2.5, min_n=min_n, cooldown=50)


def detector_for_slippage_residual(min_n: int = 100) -> PageHinkley:
    """Detect upward drift in |slippage_residual| in bps."""
    return PageHinkley(delta=0.5, threshold=5.0, min_n=min_n, cooldown=50)
