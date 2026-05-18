from __future__ import annotations

"""
Z-score mapping calibrator — adaptive (lo, hi) bounds for linear
z-score → confidence mapping used in confidence_scorer.py.

Replaces hardcoded ranges:
  - main_z ∈ [1.0, 4.0]      (z_core mapping)
  - obi_z  ∈ [0.5, 2.5]      (obi_persist mapping)

Method (per roadmap project_autocalibrators_roadmap_2026_05_17.md):
  - per-(symbol × regime × session × metric) rolling buffer of |z| samples
  - quantile q60 → lo, q95 → hi (configurable)
  - MAD-based sanity guard against degenerate tails
  - hysteresis (rel_thresh, default 10%) — skip near-noop updates
  - jump-limit (max_jump_mult, default 2×) — cap large swings
  - hierarchical fallback when finer key has not yet warmed up
  - shadow_mode: record samples + expose shadow bounds for telemetry,
    but bounds() returns defaults until promoted to enforce

Stateless wrt time except for last_apply_ms/last_recompute_ms throttling.
Deterministic over ts_ms — no wall clock reads inside.
"""

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

# Default mapping bounds — match historic hardcoded values in
# handlers/crypto_orderflow/scoring/confidence_scorer.py
DEFAULT_BOUNDS: dict[str, tuple[float, float]] = {
    "main_z": (1.0, 4.0),
    "obi_z": (0.5, 2.5),
}


def _quantile(xs: list[float], q: float) -> float:
    """Linear-interpolated quantile on a sorted copy (no numpy)."""
    if not xs:
        return 0.0
    a = sorted(xs)
    if len(a) == 1:
        return a[0]
    q = min(0.999, max(0.0, q))
    i = q * (len(a) - 1)
    lo = math.floor(i)
    hi = math.ceil(i)
    if lo == hi:
        return a[lo]
    w = i - lo
    return a[lo] * (1.0 - w) + a[hi] * w


def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    a = sorted(xs)
    n = len(a)
    m = n // 2
    if n % 2 == 1:
        return a[m]
    return 0.5 * (a[m - 1] + a[m])


def _mad(xs: list[float], med: float | None = None) -> float:
    """Median Absolute Deviation."""
    if not xs:
        return 0.0
    if med is None:
        med = _median(xs)
    devs = [abs(x - med) for x in xs]
    return _median(devs)


Key = tuple[str, str, str, str]  # (metric, symbol, regime, session)


@dataclass
class _Bin:
    """Single calibration bin for a (metric, symbol, regime, session) key."""
    buf: deque[float] = field(default_factory=lambda: deque(maxlen=2000))
    lo: float = 0.0
    hi: float = 0.0
    shadow_lo: float = 0.0
    shadow_hi: float = 0.0
    last_recompute_ms: int = 0
    last_apply_ms: int = 0
    n_observed: int = 0


@dataclass
class ZMappingCalibrator:
    """
    Adaptive z→confidence mapping bounds.

    Usage:
        c = ZMappingCalibrator()
        c.observe("main_z", symbol="BTCUSDT", regime="trend", session="us",
                  z_abs=2.7, now_ms=ts)
        lo, hi = c.bounds("main_z", symbol="BTCUSDT", regime="trend",
                          session="us", now_ms=ts)

    Bounds with fallback hierarchy when finer key is cold:
        1. (metric, symbol, regime, session)
        2. (metric, symbol, regime, "*")
        3. (metric, "*",    regime, "*")
        4. (metric, "*",    "*",    "*")
        5. DEFAULT_BOUNDS[metric]
    """

    # quantile policy
    q_lo: float = 0.60
    q_hi: float = 0.95

    # buffering
    window: int = 2000
    min_samples: int = 300

    # throttling
    recompute_gap_ms: int = 10_000   # min interval between quantile recompute
    hold_ms: int = 60_000            # min interval between applied bounds updates

    # safety
    rel_thresh: float = 0.10         # hysteresis — skip if |Δ|/prev < rel_thresh
    max_jump_mult: float = 2.0       # |new/prev| ≤ max_jump_mult
    min_spacing_mult: float = 1.5    # hi ≥ lo * min_spacing_mult

    # MAD sanity — if MAD ≈ 0 (degenerate), refuse to update bins
    mad_floor: float = 1e-6

    # global enforcement flag (False = shadow only)
    enforce: bool = False

    # internal state
    bins: dict[Key, _Bin] = field(default_factory=dict)

    # ----- public API -------------------------------------------------

    def observe(
        self,
        metric: str,
        *,
        symbol: str,
        regime: str,
        session: str,
        z_abs: float,
        now_ms: int = 0,
    ) -> None:
        """Append |z| sample to all relevant bins (full key + aggregated parents).

        Aggregated parents are filled too so cold finer keys can fall back to
        warmer parents without losing the broader distribution.
        """
        # Boundary cast — z_abs may arrive as int / Decimal / numpy scalar
        try:
            z = float(z_abs)
        except (TypeError, ValueError):
            return
        if not math.isfinite(z) or z < 0.0:
            return
        if metric not in DEFAULT_BOUNDS:
            return

        sym = (symbol or "*").upper()
        reg = (regime or "*").lower()
        ses = (session or "*").lower()

        keys: tuple[Key, ...] = (
            (metric, sym, reg, ses),
            (metric, sym, reg, "*"),
            (metric, "*", reg, "*"),
            (metric, "*", "*", "*"),
        )
        for k in keys:
            b = self.bins.get(k)
            if b is None:
                b = _Bin(buf=deque(maxlen=self.window))
                self.bins[k] = b
            b.buf.append(z)
            b.n_observed += 1

        # Try to recompute the full-resolution bin on every observation; the
        # internal throttle decides whether to actually run quantiles.
        self._maybe_recompute((metric, sym, reg, ses), now_ms=now_ms or 0)

    def bounds(
        self,
        metric: str,
        *,
        symbol: str,
        regime: str,
        session: str,
        default_lo: float | None = None,
        default_hi: float | None = None,
    ) -> tuple[float, float]:
        """Return (lo, hi) mapping bounds with hierarchical fallback.

        If `enforce` is False, shadow bins are still updated by observe(),
        but this method returns defaults — preserves prod behavior during
        warm-up / shadow phase.
        """
        if metric not in DEFAULT_BOUNDS:
            d_lo, d_hi = (default_lo or 0.0), (default_hi or 1.0)
            return d_lo, d_hi

        d_lo, d_hi = DEFAULT_BOUNDS[metric]
        if default_lo is not None:
            d_lo = default_lo
        if default_hi is not None:
            d_hi = default_hi

        if not self.enforce:
            return d_lo, d_hi

        sym = (symbol or "*").upper()
        reg = (regime or "*").lower()
        ses = (session or "*").lower()

        for k in (
            (metric, sym, reg, ses),
            (metric, sym, reg, "*"),
            (metric, "*", reg, "*"),
            (metric, "*", "*", "*"),
        ):
            b = self.bins.get(k)
            if b is None or b.lo <= 0.0 or b.hi <= 0.0:
                continue
            return b.lo, b.hi
        return d_lo, d_hi

    def shadow_bounds(
        self,
        metric: str,
        *,
        symbol: str,
        regime: str,
        session: str,
    ) -> tuple[float, float]:
        """Latest shadow (proposed) bounds, regardless of enforce flag.

        Returns (0.0, 0.0) if no calibrated value is available yet.
        """
        if metric not in DEFAULT_BOUNDS:
            return 0.0, 0.0
        sym = (symbol or "*").upper()
        reg = (regime or "*").lower()
        ses = (session or "*").lower()
        for k in (
            (metric, sym, reg, ses),
            (metric, sym, reg, "*"),
            (metric, "*", reg, "*"),
            (metric, "*", "*", "*"),
        ):
            b = self.bins.get(k)
            if b is None:
                continue
            if b.shadow_lo > 0.0 and b.shadow_hi > 0.0:
                return b.shadow_lo, b.shadow_hi
        return 0.0, 0.0

    def snapshot(self) -> dict[str, Any]:
        """Compact dict for Redis publish / telemetry.

        Schema:
            {
              "enforce": bool,
              "bins": [
                {"metric","symbol","regime","session","n",
                 "lo","hi","shadow_lo","shadow_hi",
                 "last_apply_ms","last_recompute_ms"},
                ...
              ]
            }
        """
        out_bins: list[dict[str, Any]] = []
        for (metric, sym, reg, ses), b in self.bins.items():
            out_bins.append({
                "metric": metric,
                "symbol": sym,
                "regime": reg,
                "session": ses,
                "n": len(b.buf),
                "lo": b.lo,
                "hi": b.hi,
                "shadow_lo": b.shadow_lo,
                "shadow_hi": b.shadow_hi,
                "last_apply_ms": b.last_apply_ms,
                "last_recompute_ms": b.last_recompute_ms,
            })
        return {"enforce": self.enforce, "bins": out_bins}

    def load_state(self, state: dict[str, Any]) -> None:
        """Restore bins from snapshot (used to warm-start after restart).

        Buffers are NOT restored — only persisted bounds — to avoid
        round-tripping large sample arrays. Calibration resumes once
        new samples accumulate; bounds() keeps serving previously
        calibrated values via fallback hierarchy.

        Boundary method — tolerant to malformed rows.
        """
        self.enforce = bool(state.get("enforce", self.enforce))
        for row in state.get("bins", []) or []:
            try:
                metric = str(row["metric"])
                if metric not in DEFAULT_BOUNDS:
                    continue
                k: Key = (
                    metric,
                    str(row.get("symbol", "*")).upper(),
                    str(row.get("regime", "*")).lower(),
                    str(row.get("session", "*")).lower(),
                )
                b = self.bins.get(k) or _Bin(buf=deque(maxlen=self.window))
                b.lo = float(row.get("lo", 0.0) or 0.0)
                b.hi = float(row.get("hi", 0.0) or 0.0)
                b.shadow_lo = float(row.get("shadow_lo", 0.0) or 0.0)
                b.shadow_hi = float(row.get("shadow_hi", 0.0) or 0.0)
                b.last_apply_ms = int(row.get("last_apply_ms", 0) or 0)
                b.last_recompute_ms = int(row.get("last_recompute_ms", 0) or 0)
                self.bins[k] = b
            except (KeyError, TypeError, ValueError):
                continue

    # ----- internals --------------------------------------------------

    def _maybe_recompute(self, key: Key, *, now_ms: int) -> None:
        b = self.bins.get(key)
        if b is None:
            return
        if len(b.buf) < self.min_samples:
            return
        # recompute throttle — skipped on first recompute (last_recompute_ms==0)
        if b.last_recompute_ms > 0 and (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms

        xs = list(b.buf)
        new_lo = _quantile(xs, self.q_lo)
        new_hi = _quantile(xs, self.q_hi)

        # MAD sanity: if MAD≈0 the distribution is degenerate (constant)
        med = _median(xs)
        mad = _mad(xs, med)
        if mad < self.mad_floor:
            return

        # enforce min-spacing so linear map width never collapses
        if new_hi < new_lo * self.min_spacing_mult:
            new_hi = new_lo * self.min_spacing_mult

        # Update shadow always — it's the latest proposal
        b.shadow_lo = new_lo
        b.shadow_hi = new_hi

        # apply-side throttle
        if (now_ms - b.last_apply_ms) < self.hold_ms and b.lo > 0.0:
            return

        committed_lo, committed_hi = b.lo, b.hi

        if committed_lo > 0.0 and committed_hi > 0.0:
            # hysteresis — skip near-noop updates
            d_lo = abs(new_lo - committed_lo) / max(committed_lo, 1e-9)
            d_hi = abs(new_hi - committed_hi) / max(committed_hi, 1e-9)
            if d_lo < self.rel_thresh and d_hi < self.rel_thresh:
                return

            # jump-limit — cap |new / prev|. Both prevs are > 0 here.
            def _clamp(prev: float, nxt: float) -> float:
                hi_cap = prev * self.max_jump_mult
                lo_cap = prev / self.max_jump_mult
                return max(lo_cap, min(hi_cap, nxt))

            new_lo = _clamp(committed_lo, new_lo)
            new_hi = _clamp(committed_hi, new_hi)
            if new_hi < new_lo * self.min_spacing_mult:
                new_hi = new_lo * self.min_spacing_mult

        b.lo = new_lo
        b.hi = new_hi
        b.last_apply_ms = now_ms
