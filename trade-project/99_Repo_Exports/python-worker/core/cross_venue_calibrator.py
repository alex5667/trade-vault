from __future__ import annotations

"""CrossVenueCalibratorCore — adaptive threshold computation for the cross-venue gate.

Replaces hardcoded constants in signal_pipeline.py:
  min_agree          = 0.67  (≈ 2/3 venue agreement)
  max_dislocation_z  = 3.0

with per-symbol rolling 24-hour statistics using robust estimators:

  adaptive_disloc_z  = max(DISLOC_FLOOR,
                           median(disloc) + MAD_DISLOC_MULT × MAD(disloc))

  adaptive_min_agree = clamp(median(agree) − MAD_AGREE_MULT × MAD(agree),
                             AGREE_FLOOR, AGREE_CAP)

Rationale:
  - At disloc=2.5σ below the current adaptive threshold the gate fires, but if
    the symbol's *typical* disloc is 2.0σ then 2.5σ is only 0.5σ above typical —
    not a real signal.  Adaptive threshold anchors to each symbol's distribution.
  - If three venues usually agree 90%, then 65% is a real disagreement.
    If they usually agree 70%, 65% is noise.  Adaptive min_agree reacts to the
    historical correlation structure, not a fixed 2/3 heuristic.

No I/O here — pure math.  The feed service lives in:
  orderflow_services/cross_venue_calibrator_v1.py
The runtime reader lives in:
  core/cross_venue_calib_reader.py
"""

import json
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

# ────────────────────────────── public constants ───────────────────────────── #

DISLOC_FLOOR: float = 1.5   # never set threshold below 1.5σ
DISLOC_CAP:   float = 6.0   # never set threshold above 6.0σ

AGREE_FLOOR:  float = 0.50  # absolute minimum — never accept <50% agreement
AGREE_CAP:    float = 0.85  # don't over-restrict — upper bound on adaptive floor

DEFAULT_DISLOC_Z:  float = 3.0   # ENV fallback (matches CROSSVENUE_CTX_MAX_DISLOCATION_Z)
DEFAULT_MIN_AGREE: float = 0.67  # ENV fallback (matches CROSSVENUE_CTX_MIN_AGREE)

WINDOW_MS:       int   = 86_400_000   # 24-hour rolling window
MIN_SAMPLES:     int   = 30           # minimum observations before calibration kicks in
MAD_DISLOC_MULT: float = 2.5          # spec: median + 2.5 × MAD for dislocation
MAD_AGREE_MULT:  float = 2.0          # median − 2.0 × MAD for agreement floor

_MAXLEN: int = 1_440  # ring-buffer capacity: 24 h at one sample per minute


# ────────────────────────────────── helpers ────────────────────────────────── #

def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    m = n >> 1
    return s[m] if n & 1 else (s[m - 1] + s[m]) * 0.5


def _mad(xs: list[float], med: float) -> float:
    """Median absolute deviation (not scaled by 1.4826)."""
    if not xs:
        return 0.0
    return _median([abs(x - med) for x in xs])


# ────────────────────────────────── core ──────────────────────────────────── #

@dataclass
class _Sample:
    disloc_z: float
    agree:    float
    ts_ms:    int


class CrossVenueCalibBin:
    """Rolling buffer + committed thresholds for one symbol."""

    __slots__ = (
        "buf",
        "adaptive_disloc_z",
        "adaptive_min_agree",
        "last_compute_ms",
        "last_ts_ms",
    )

    def __init__(self) -> None:
        self.buf: deque[_Sample]  = deque(maxlen=_MAXLEN)
        self.adaptive_disloc_z:  float = DEFAULT_DISLOC_Z
        self.adaptive_min_agree: float = DEFAULT_MIN_AGREE
        self.last_compute_ms:    int   = 0
        self.last_ts_ms:         int   = 0

    def observe(self, disloc_z: float, agree: float, ts_ms: int) -> None:
        self.buf.append(_Sample(disloc_z=disloc_z, agree=agree, ts_ms=ts_ms))
        if ts_ms > self.last_ts_ms:
            self.last_ts_ms = ts_ms

    def evict(self, cutoff_ms: int) -> None:
        while self.buf and self.buf[0].ts_ms < cutoff_ms:
            self.buf.popleft()

    def recompute(
        self,
        now_ms: int,
        *,
        min_samples: int,
        mad_disloc_mult: float,
        mad_agree_mult: float,
    ) -> bool:
        """Return True iff there were enough samples to update thresholds."""
        n = len(self.buf)
        if n < min_samples:
            return False

        disloc_vals = [s.disloc_z for s in self.buf]
        agree_vals  = [s.agree    for s in self.buf]

        med_d = _median(disloc_vals)
        raw_d = med_d + mad_disloc_mult * _mad(disloc_vals, med_d)
        self.adaptive_disloc_z = max(DISLOC_FLOOR, min(DISLOC_CAP, raw_d))

        med_a = _median(agree_vals)
        raw_a = med_a - mad_agree_mult * _mad(agree_vals, med_a)
        self.adaptive_min_agree = max(AGREE_FLOOR, min(AGREE_CAP, raw_a))

        self.last_compute_ms = now_ms
        return True

    def to_dict(self) -> dict[str, Any]:
        n = len(self.buf)
        disloc_vals = [s.disloc_z for s in self.buf]
        agree_vals  = [s.agree    for s in self.buf]
        med_d = _median(disloc_vals) if disloc_vals else 0.0
        med_a = _median(agree_vals)  if agree_vals  else 0.0
        return {
            "n":                  n,
            "median_disloc":      round(med_d, 4),
            "mad_disloc":         round(_mad(disloc_vals, med_d), 4),
            "adaptive_disloc_z":  round(self.adaptive_disloc_z,  4),
            "median_agree":       round(med_a, 4),
            "mad_agree":          round(_mad(agree_vals,  med_a), 4),
            "adaptive_min_agree": round(self.adaptive_min_agree, 4),
            "last_ts_ms":         self.last_ts_ms,
            "last_compute_ms":    self.last_compute_ms,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "CrossVenueCalibBin":
        b = cls()
        b.adaptive_disloc_z  = float(d.get("adaptive_disloc_z",  DEFAULT_DISLOC_Z))
        b.adaptive_min_agree = float(d.get("adaptive_min_agree", DEFAULT_MIN_AGREE))
        b.last_compute_ms    = int(d.get("last_compute_ms", 0))
        b.last_ts_ms         = int(d.get("last_ts_ms", 0))
        # Buffers are NOT persisted — rebuilt from live data after restart.
        return b


class CrossVenueCalibratorCore:
    """Rolling 24-hour per-symbol adaptive threshold calibrator.

    Thread-safe for reads; single-writer assumption for observe/recompute.
    """

    def __init__(
        self,
        *,
        window_ms:       int   = WINDOW_MS,
        min_samples:     int   = MIN_SAMPLES,
        mad_disloc_mult: float = MAD_DISLOC_MULT,
        mad_agree_mult:  float = MAD_AGREE_MULT,
        enforce:         bool  = False,
    ) -> None:
        self.window_ms       = window_ms
        self.min_samples     = min_samples
        self.mad_disloc_mult = mad_disloc_mult
        self.mad_agree_mult  = mad_agree_mult
        self.enforce         = enforce
        self._bins: dict[str, CrossVenueCalibBin] = {}

    # ── write path ────────────────────────────────────────────────────────── #

    def observe(self, symbol: str, disloc_z: float, agree: float, ts_ms: int) -> None:
        """Record one snapshot observation.  Silently drops NaN/Inf inputs."""
        if not math.isfinite(disloc_z) or not math.isfinite(agree):
            return
        sym = (symbol or "").upper().strip()
        if not sym:
            return
        if sym not in self._bins:
            self._bins[sym] = CrossVenueCalibBin()
        b = self._bins[sym]
        b.evict(ts_ms - self.window_ms)
        b.observe(disloc_z, agree, ts_ms)

    def recompute_all(self, now_ms: int) -> int:
        """Evict stale samples and recompute all bins.  Returns count updated."""
        cutoff = now_ms - self.window_ms
        updated = 0
        for b in self._bins.values():
            b.evict(cutoff)
            if b.recompute(
                now_ms,
                min_samples=self.min_samples,
                mad_disloc_mult=self.mad_disloc_mult,
                mad_agree_mult=self.mad_agree_mult,
            ):
                updated += 1
        return updated

    # ── read path ─────────────────────────────────────────────────────────── #

    def thresholds_for(
        self,
        symbol: str,
        *,
        default_disloc_z:  float = DEFAULT_DISLOC_Z,
        default_min_agree: float = DEFAULT_MIN_AGREE,
    ) -> tuple[float, float]:
        """Return ``(adaptive_disloc_z, adaptive_min_agree)`` with fail-open semantics.

        Returns supplied defaults when:
        - ``enforce=False`` (shadow mode — observe but don't act)
        - symbol has fewer than ``min_samples`` observations
        - symbol is unknown
        """
        if not self.enforce:
            return default_disloc_z, default_min_agree
        sym = (symbol or "").upper().strip()
        b = self._bins.get(sym)
        if b is None or len(b.buf) < self.min_samples:
            return default_disloc_z, default_min_agree
        return b.adaptive_disloc_z, b.adaptive_min_agree

    # ── persistence ───────────────────────────────────────────────────────── #

    def snapshot(self, now_ms: int) -> dict[str, Any]:
        """Serialise committed thresholds (not raw buffers) for Redis SET."""
        return {
            "schema_version":  1,
            "generated_ms":    now_ms,
            "enforce":         self.enforce,
            "window_ms":       self.window_ms,
            "min_samples":     self.min_samples,
            "mad_disloc_mult": self.mad_disloc_mult,
            "mad_agree_mult":  self.mad_agree_mult,
            "n_symbols":       len(self._bins),
            "symbols": {sym: b.to_dict() for sym, b in self._bins.items()},
        }

    @classmethod
    def load_state(cls, state: dict[str, Any]) -> "CrossVenueCalibratorCore":
        """Restore committed thresholds from a snapshot.  Buffers start empty."""
        cal = cls(
            window_ms=int(state.get("window_ms", WINDOW_MS)),
            min_samples=int(state.get("min_samples", MIN_SAMPLES)),
            mad_disloc_mult=float(state.get("mad_disloc_mult", MAD_DISLOC_MULT)),
            mad_agree_mult=float(state.get("mad_agree_mult",  MAD_AGREE_MULT)),
            enforce=bool(state.get("enforce", False)),
        )
        for sym, d in (state.get("symbols") or {}).items():
            cal._bins[sym.upper().strip()] = CrossVenueCalibBin.from_dict(d)
        return cal
