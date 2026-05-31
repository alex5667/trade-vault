from __future__ import annotations

# core/pressure_tier_calibrator.py
"""
Pressure Tier Calibrator - Adaptive DN threshold calibration

Automatically adjusts delta_notional_usd tier thresholds based on real market activity.
Uses quantile-based approach to ensure tiers adapt to current liquidity/volatility.

Key Features:
- Regime-aware: separate calibration per market regime (trend/range/thin)
- Hysteresis: prevents rapid threshold oscillation
- Hold-down: enforces minimum time between updates
- Jump limiting: prevents extreme threshold changes
"""
import math
from collections import deque
from dataclasses import dataclass, field


def _quantile(xs, q: float) -> float:
    """Calculate quantile without numpy dependency."""
    if not xs:
        return 0.0
    a = sorted(xs)
    if len(a) == 1:
        return float(a[0])
    q = min(0.999, max(0.0, float(q)))
    i = q * (len(a) - 1)
    lo = int(math.floor(i))
    hi = int(math.ceil(i))
    if lo == hi:
        return float(a[lo])
    w = i - lo
    return float(a[lo] * (1.0 - w) + a[hi] * w)


@dataclass
class PressureTierCalibrator:
    """
    Adaptive calibrator for pressure tier thresholds.
    
    Collects raw delta_notional_usd samples (before filtering) and computes
    quantile-based thresholds per regime to ensure tiers adapt to market conditions.
    """
    min_samples: int = 300              # Minimum samples before calibration
    window: int = 2000                  # Rolling window size for samples
    recompute_gap_ms: int = 10_000      # Min interval between recompute (10s)
    hold_ms: int = 60_000               # Min interval between applying new tiers (60s)
    max_jump_mult: float = 2.0          # Max multiplier for threshold change

    # Internal state
    buf: dict[str, deque[float]] = field(default_factory=dict)  # regime -> dn_usd samples
    last_recompute_ms: int = 0
    last_apply_ms: int = 0
    last_tiers: dict[str, tuple[float, float, float]] = field(default_factory=dict)  # regime -> (t0,t1,t2)

    def observe(self, *, regime: str, dn_usd: float) -> None:
        """
        Record a raw delta_notional_usd sample.
        
        CRITICAL: Call this BEFORE tier filtering to avoid selection bias.
        """
        rg = (regime or "na").lower()
        if dn_usd <= 0:
            return
        d = self.buf.setdefault(rg, deque(maxlen=self.window))
        d.append(float(dn_usd))

    def ready(self, regime: str) -> bool:
        """Check if enough samples collected for calibration."""
        rg = (regime or "na").lower()
        return len(self.buf.get(rg, ())) >= self.min_samples

    def maybe_recompute(self, *, now_ms: int, regime: str) -> dict[str, float]:
        """
        Recompute tier thresholds if conditions met.
        
        Returns dict with tier0/tier1/tier2 keys if updated, empty dict otherwise.
        
        Quantile policy:
        - tier0 (Trend): p80 - lenient for strong trends
        - tier1 (Range): p90 - standard for ranging markets
        - tier2 (Thin):  p97 - strict for low liquidity
        """
        rg = (regime or "na").lower()

        # Throttle recompute
        if (now_ms - self.last_recompute_ms) < self.recompute_gap_ms:
            return {}

        self.last_recompute_ms = int(now_ms)

        xs = list(self.buf.get(rg, ()))
        if len(xs) < self.min_samples:
            return {}

        # Compute quantile-based tiers
        t0 = _quantile(xs, 0.80)  # Tier 0 (Trend) - lenient
        t1 = _quantile(xs, 0.90)  # Tier 1 (Range) - standard
        t2 = _quantile(xs, 0.97)  # Tier 2 (Thin) - strict

        # Hysteresis: hold-down period
        prev = self.last_tiers.get(rg)
        if prev and (now_ms - self.last_apply_ms) < self.hold_ms:
            return {}

        # Jump limiting: prevent extreme changes
        if prev:
            def clamp(prev_v, new_v):
                if prev_v <= 0:
                    return new_v
                hi = prev_v * self.max_jump_mult
                lo = prev_v / self.max_jump_mult
                return max(lo, min(hi, new_v))

            t0 = clamp(prev[0], t0)
            t1 = clamp(prev[1], t1)
            t2 = clamp(prev[2], t2)

        # Enforce minimum spacing (tier1 >= tier0 * 1.1, tier2 >= tier1 * 1.1)
        if t1 < t0 * 1.1:
            t1 = t0 * 1.1
        if t2 < t1 * 1.1:
            t2 = t1 * 1.1

        self.last_tiers[rg] = (float(t0), float(t1), float(t2))
        self.last_apply_ms = int(now_ms)

        return {"tier0": float(t0), "tier1": float(t1), "tier2": float(t2)}
