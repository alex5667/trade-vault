from __future__ import annotations

"""cusum_drift_detector.py

CUSUM/Page-Hinkley drift detector for calibration quality (ECE / Brier).

Why
  After model updates or regime shifts the confidence→outcome calibration
  can drift silently. Manual monitoring (currently via ml_drift_monitor_v14_of)
  catches it late. This module provides a lightweight in-process detector that
  fires an alarm after observing a sustained upward drift in Brier score or ECE.

Method (Page-Hinkley, one-sided upper)
  PH_t = max(PH_{t-1} + (x_t - μ₀ - δ), 0)
  Alarm when PH_t > λ

  x_t = per-trade Brier score  (p_hat - outcome)²
  μ₀  = baseline Brier  — EWMA of first `warmup` observations
  δ   = sensitivity slack  (default 0.005, ~5 Brier units × 10⁻³)
  λ   = alarm threshold  (default 0.30)

ECE tracker
  Maintains per-decile (10 buckets) running sums for ECE computation.
  ECE = Σ_b  (n_b / N) × |mean_p_b − mean_y_b|

Stateless wrt time except for last_alarm_ms; no wall-clock reads inside
observe(). All state is in-memory per (schema × regime) bin.

Thread-safety: NOT thread-safe — call from a single event loop / thread.
"""

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _ece_from_bins(bins: list[tuple[float, float, int]]) -> float:
    """Compute ECE from [(sum_p, sum_y, n), ...] decile bins."""
    total = sum(b[2] for b in bins)
    if total == 0:
        return 0.0
    ece = 0.0
    for s_p, s_y, n in bins:
        if n == 0:
            continue
        mean_p = s_p / n
        mean_y = s_y / n
        ece += (n / total) * abs(mean_p - mean_y)
    return ece


# ---------------------------------------------------------------------------
# Per-key state
# ---------------------------------------------------------------------------

@dataclass
class _Bin:
    # Page-Hinkley state
    ph_score: float = 0.0
    n_observed: int = 0
    n_alarms: int = 0
    last_alarm_idx: int = -1   # observation index of last alarm

    # baseline EWMA (updated during warmup, then frozen)
    ewma_brier: float = -1.0   # -1.0 = not yet set
    _warmup_buf: deque = field(default_factory=lambda: deque(maxlen=200))

    # ECE state: 10 decile bins, each = (sum_p, sum_y, n)
    ece_bins: list = field(
        default_factory=lambda: [(0.0, 0.0, 0) for _ in range(10)]
    )
    ece_window: deque = field(default_factory=lambda: deque(maxlen=500))


Key = tuple[str, str]  # (schema, regime)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass
class CuSumDriftDetector:
    """
    Page-Hinkley CUSUM detector for per-(schema × regime) Brier / ECE drift.

    Usage::

        det = CuSumDriftDetector()
        alarmed = det.observe(
            schema="v14_of", regime="trend",
            p_hat=0.63, outcome=1,
        )
        if alarmed:
            # emit alert / metric / log
            ...
        ece = det.current_ece(schema="v14_of", regime="trend")
        ph  = det.current_ph(schema="v14_of", regime="trend")

    Parameters
    ----------
    warmup : int
        Number of samples used to estimate baseline Brier before PH starts.
        During warmup the baseline μ₀ is updated via EWMA; after warmup it
        is frozen (μ₀ = ewma_brier).
    delta : float
        Sensitivity slack δ.  Smaller → more sensitive (more false alarms).
    threshold : float
        PH alarm threshold λ.  Larger → less sensitive (slower detection).
    ece_window_size : int
        Rolling window size for ECE computation.
    cooldown_observations : int
        After an alarm the detector resets PH and ignores the next N samples
        (prevents alarm flooding).
    """

    warmup: int = 100
    delta: float = 0.005
    threshold: float = 0.30
    ece_window_size: int = 500
    cooldown_observations: int = 50

    _bins: dict[Key, _Bin] = field(default_factory=dict)

    # ----- public API --------------------------------------------------------

    def observe(
        self,
        schema: str,
        regime: str,
        p_hat: float,
        outcome: int,      # 1 = win, 0 = loss
    ) -> bool:
        """Record one (p_hat, outcome) observation.

        Returns True if a drift alarm fired on this observation.
        """
        try:
            p = _clamp01(p_hat if isinstance(p_hat, float) else float(p_hat))
            y = 1 if outcome else 0
        except (TypeError, ValueError):
            return False
        if not math.isfinite(p):
            return False

        k: Key = (_canon(schema), _canon(regime))
        b = self._bins.get(k)
        if b is None:
            b = _Bin(
                _warmup_buf=deque(maxlen=self.warmup),
                ece_window=deque(maxlen=self.ece_window_size),
                ece_bins=[(0.0, 0.0, 0) for _ in range(10)],
            )
            self._bins[k] = b

        b.n_observed += 1
        brier = (p - y) ** 2

        # ECE update — rolling decile bins rebuilt every call from window
        b.ece_window.append((p, y))
        b.ece_bins = _rebuild_ece_bins(b.ece_window)

        # Baseline warmup
        if b.ewma_brier < 0.0:
            b._warmup_buf.append(brier)
            if len(b._warmup_buf) >= self.warmup:
                # freeze baseline
                b.ewma_brier = sum(b._warmup_buf) / len(b._warmup_buf)
            return False  # no alarm during warmup

        # Cooldown guard — skip PH update for N obs after alarm
        if b.n_alarms > 0:
            obs_since_alarm = b.n_observed - b.last_alarm_idx
            if obs_since_alarm <= self.cooldown_observations:
                return False

        # Page-Hinkley update
        b.ph_score = max(0.0, b.ph_score + (brier - b.ewma_brier - self.delta))

        if b.ph_score >= self.threshold:
            b.n_alarms += 1
            b.last_alarm_idx = b.n_observed
            b.ph_score = 0.0   # reset after alarm
            return True

        return False

    def current_ph(self, schema: str, regime: str) -> float:
        """Current PH score (0.0 if no bin or in warmup)."""
        b = self._bins.get((_canon(schema), _canon(regime)))
        if b is None or b.ewma_brier < 0.0:
            return 0.0
        return b.ph_score

    def current_ece(self, schema: str, regime: str) -> float:
        """Current ECE estimate (0.0 if no data)."""
        b = self._bins.get((_canon(schema), _canon(regime)))
        if b is None:
            return 0.0
        return _ece_from_bins(b.ece_bins)

    def baseline_brier(self, schema: str, regime: str) -> float:
        """Frozen baseline Brier (-1.0 if still in warmup)."""
        b = self._bins.get((_canon(schema), _canon(regime)))
        if b is None:
            return -1.0
        return b.ewma_brier

    def n_observed(self, schema: str, regime: str) -> int:
        b = self._bins.get((_canon(schema), _canon(regime)))
        return b.n_observed if b else 0

    def n_alarms(self, schema: str, regime: str) -> int:
        b = self._bins.get((_canon(schema), _canon(regime)))
        return b.n_alarms if b else 0

    def snapshot(self) -> list[dict[str, Any]]:
        """Telemetry snapshot for Redis persist / Prometheus scrape."""
        out = []
        for (schema, regime), b in self._bins.items():
            out.append({
                "schema": schema,
                "regime": regime,
                "n_observed": b.n_observed,
                "n_alarms": b.n_alarms,
                "ph_score": round(b.ph_score, 6),
                "baseline_brier": round(b.ewma_brier, 6),
                "ece": round(_ece_from_bins(b.ece_bins), 6),
                "warmup_done": b.ewma_brier >= 0.0,
            })
        return out

    def load_state(self, rows: list[Any]) -> None:
        """Restore frozen baselines from a previous snapshot (warm-start).

        Buffers and ECE windows are NOT restored — only baseline_brier and
        n_alarms — so the detector resumes from the correct μ₀ without
        needing to replay the full history.
        """
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            try:
                schema = _canon(str(row.get("schema", "")))
                regime = _canon(str(row.get("regime", "")))
                baseline = float(row.get("baseline_brier", -1.0) or -1.0)
                if not schema or not regime or baseline < 0.0:
                    continue
                k: Key = (schema, regime)
                b = self._bins.get(k) or _Bin(
                    _warmup_buf=deque(maxlen=self.warmup),
                    ece_window=deque(maxlen=self.ece_window_size),
                    ece_bins=[(0.0, 0.0, 0) for _ in range(10)],
                )
                b.ewma_brier = baseline
                b.n_alarms = int(row.get("n_alarms", 0) or 0)
                b.n_observed = int(row.get("n_observed", 0) or 0)
                self._bins[k] = b
            except (KeyError, TypeError, ValueError):
                continue


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _canon(s: str) -> str:
    return (s or "").strip().lower()


def _rebuild_ece_bins(window: Any) -> list[tuple[float, float, int]]:
    """Recompute 10-decile ECE bins from rolling window of (p, y) pairs."""
    bins: list[list] = [[0.0, 0.0, 0] for _ in range(10)]
    for p, y in window:
        idx = min(int(p * 10), 9)
        bins[idx][0] += p
        bins[idx][1] += float(y)
        bins[idx][2] += 1
    return [(s_p, s_y, n) for s_p, s_y, n in bins]
