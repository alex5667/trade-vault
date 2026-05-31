# -*- coding: utf-8 -*-
"""
ATR(bps) Sanity Calibrator (per-symbol, per-regime)
---------------------------------------------------
Goal:
  - Learn realistic ATR% / ATR(bps) floors from live data (bar-close),
    per regime ("trend/range/thin/news/...").
  - Provide deterministic thresholds used by tick-level gates.

Design constraints (your project):
  - Deterministic timestamps: update with bar.end_ts_ms (not wall clock).
  - Fail-open: if missing data => return bootstrap defaults.
  - Persist/load state per symbol+regime to Redis (same pattern as EffQuoteCalibrator).
  - Low memory footprint: P² quantile estimators.

Semantics:
  - We maintain quantiles for atr_bps distribution: q10/q20/q30.
  - Floors:
      tier0 (trend)  -> q10
      tier1 (range)  -> q20
      tier2 (thin)   -> q30
    (these are "sanity floors" that scale with symbol's typical noise)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from core.quantile_p2 import P2Quantile


@dataclass
class ATRBpsThresholds:
    floor_t0: float
    floor_t1: float
    floor_t2: float
    floor_t3: float = 0.0 # kept for compatibility if needed, but mega-diff uses 0/1/2
    n: int = 0
    src: str = "static" # "static" | "calib_q10q20q30"


class ATRBpsCalibrator:
    """
    Per-regime ATR(bps) floor calibrator using P² quantiles.
    """

    def __init__(self, *, min_samples: int = 500) -> None:
        self.min_samples = int(min_samples)
        self._q10: Dict[str, P2Quantile] = {}
        self._q20: Dict[str, P2Quantile] = {}
        self._q30: Dict[str, P2Quantile] = {}
        self._n: Dict[str, int] = {}

    def _get(self, m: Dict[str, P2Quantile], rg: str, p: float) -> P2Quantile:
        q = m.get(rg)
        if q is None:
            q = P2Quantile(p=float(p))
            m[rg] = q
        return q

    def update(self, *, regime: str, atr_bps: float) -> None:
        rg = str(regime or "na").lower()
        x = float(atr_bps or 0.0)
        if not math.isfinite(x) or x <= 0:
            return
        # robust guard against absurd spikes (bad source / decode)
        # you can widen later, but keep it deterministic and fail-open.
        if x > 5_000:
            return
        self._get(self._q10, rg, 0.10).update(x)
        self._get(self._q20, rg, 0.20).update(x)
        self._get(self._q30, rg, 0.30).update(x)
        self._n[rg] = int(self._n.get(rg, 0) + 1)

    def thresholds(
        self,
        *,
        regime: str,
        default_floor_t0: float,
        default_floor_t1: float,
        default_floor_t2: float,
        clamp: Tuple[float, float] = (0.1, 5_000.0),
    ) -> ATRBpsThresholds:
        rg = str(regime or "na").lower()
        n = int(self._n.get(rg, 0))
        lo, hi = float(clamp[0]), float(clamp[1])

        def _cl(x: float) -> float:
            if not math.isfinite(x):
                return 0.0
            return max(lo, min(hi, float(x)))

        # bootstrap until ready
        if n < int(self.min_samples):
            return ATRBpsThresholds(
                floor_t0=_cl(float(default_floor_t0 or 0.0)),
                floor_t1=_cl(float(default_floor_t1 or 0.0)),
                floor_t2=_cl(float(default_floor_t2 or 0.0)),
                n=n,
                src="static",
            )

        q10 = self._q10.get(rg).value() if self._q10.get(rg) else None
        q20 = self._q20.get(rg).value() if self._q20.get(rg) else None
        q30 = self._q30.get(rg).value() if self._q30.get(rg) else None

        # fail-open fallback to defaults if estimator not ready
        t0 = _cl(float(q10 if (q10 and q10 > 0) else default_floor_t0))
        t1 = _cl(float(q20 if (q20 and q20 > 0) else default_floor_t1))
        t2 = _cl(float(q30 if (q30 and q30 > 0) else default_floor_t2))

        # monotone safety (must not invert)
        if t1 < t0:
            t1 = t0
        if t2 < t1:
            t2 = t1

        return ATRBpsThresholds(floor_t0=t0, floor_t1=t1, floor_t2=t2, n=n, src="calib_q10q20q30")

    # ---------------- Persistence (same pattern as EffQuoteCalibrator) ----------------

    def dump_regime_state(self, *, symbol: str, regime: str, updated_ts_ms: int) -> Dict[str, Any]:
        rg = str(regime or "na").lower()
        return {
            "v": 1,
            "symbol": str(symbol or "").upper(),
            "regime": rg,
            "updated_ts_ms": int(updated_ts_ms or 0),
            "min_samples": int(self.min_samples),
            "n": int(self._n.get(rg, 0)),
            "q10": (self._q10.get(rg).to_state() if self._q10.get(rg) else None),
            "q20": (self._q20.get(rg).to_state() if self._q20.get(rg) else None),
            "q30": (self._q30.get(rg).to_state() if self._q30.get(rg) else None),
        }

    def load_regime_state(self, state: Dict[str, Any]) -> None:
        try:
            rg = str(state.get("regime") or "na").lower()
            self._n[rg] = int(state.get("n", 0) or 0)
            q10 = state.get("q10")
            q20 = state.get("q20")
            q30 = state.get("q30")
            if isinstance(q10, dict):
                self._q10[rg] = P2Quantile.from_state(q10)
            if isinstance(q20, dict):
                self._q20[rg] = P2Quantile.from_state(q20)
            if isinstance(q30, dict):
                self._q30[rg] = P2Quantile.from_state(q30)
        except Exception:
            return
