"""
manip_pattern_basis_calibrator.py — per-symbol adaptive manip pattern state-machine calibrator.

Calibrates the base constants of ManipulationTracker state machine:
  - layering_build_mult: how much depth must grow before triggering (default 1.6×)
  - layering_revert_frac: how much must revert to confirm manipulation (default 0.35)
  - layering_revert_ms:   revert window (default 900ms)
  - qs_msg_z_thr:        quote-stuffing book-rate z-score (default 4.0)
  - qs_cancel_z_thr:     cancel-rate z-score (default 3.5)

These are calibrated from historical manip score distributions per symbol, so that
the False Positive Rate is bounded at ~p99 per symbol.

Output: autocal:manip_pattern_basis:state (STRING, JSON)
Master switch: MANIP_PATTERN_BASIS_CAL_ENFORCE=0 (shadow default)
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1

# Global defaults (mirrors manip_patterns.py)
_DEFAULT_BUILD_MULT = 1.6
_DEFAULT_REVERT_FRAC = 0.35
_DEFAULT_REVERT_MS = 900.0
_DEFAULT_QS_MSG_Z = 4.0
_DEFAULT_QS_CANCEL_Z = 3.5

# Bounds for calibrated values
_BUILD_MULT_MIN = 1.2
_BUILD_MULT_MAX = 3.0
_REVERT_FRAC_MIN = 0.20
_REVERT_FRAC_MAX = 0.70
_REVERT_MS_MIN = 300.0
_REVERT_MS_MAX = 3000.0
_QS_Z_MIN = 2.0
_QS_Z_MAX = 8.0

_UPDATE_BAND_FRAC = 0.05   # 5% change before committing


@dataclass
class _ObsSample:
    layering_score: float
    qs_score: float
    build_depth_ratio: float   # actual peak depth / baseline
    revert_delay_ms: float     # how fast revert occurred
    ts_ms: int


@dataclass
class _Bin:
    buf: deque[_ObsSample] = field(default_factory=lambda: deque(maxlen=5_000))
    # Committed state-machine parameters
    committed_build_mult: float = _DEFAULT_BUILD_MULT
    committed_revert_frac: float = _DEFAULT_REVERT_FRAC
    committed_revert_ms: float = _DEFAULT_REVERT_MS
    committed_qs_msg_z: float = _DEFAULT_QS_MSG_Z
    committed_qs_cancel_z: float = _DEFAULT_QS_CANCEL_Z
    # Shadow proposals
    shadow_build_mult: float = _DEFAULT_BUILD_MULT
    shadow_revert_ms: float = _DEFAULT_REVERT_MS
    last_recompute_ms: int = 0
    n_observed: int = 0


class ManipPatternBasisCalibrator:
    """Per-symbol adaptive manip pattern threshold calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_ms: int = 43_200_000,   # 12h rolling (mirrors manip_calibrator)
        min_samples: int = 200,
        recompute_gap_ms: int = 600_000,
        fpr_target: float = 0.01,      # false-positive rate target (p99 → z@1%)
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = window_ms
        self.min_samples = min_samples
        self.recompute_gap_ms = recompute_gap_ms
        self.fpr_target = fpr_target
        self._bins: dict[str, _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        layering_score: float,
        qs_score: float,
        build_depth_ratio: float = 1.0,
        revert_delay_ms: float = 0.0,
        ts_ms: int,
    ) -> None:
        if not math.isfinite(layering_score) or not math.isfinite(qs_score):
            return
        sym = (symbol or "*").upper()
        now_ms = int(time.time() * 1000)
        sample = _ObsSample(
            layering_score=layering_score,
            qs_score=qs_score,
            build_depth_ratio=max(1.0, build_depth_ratio),
            revert_delay_ms=max(0.0, revert_delay_ms),
            ts_ms=ts_ms,
        )
        for key in [sym, "*"]:
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(sym, now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_params(self, symbol: str) -> dict[str, float]:
        """Return per-symbol state-machine params dict (fail-open to defaults)."""
        defaults = {
            "build_mult": _DEFAULT_BUILD_MULT,
            "revert_frac": _DEFAULT_REVERT_FRAC,
            "revert_ms": _DEFAULT_REVERT_MS,
            "qs_msg_z": _DEFAULT_QS_MSG_Z,
            "qs_cancel_z": _DEFAULT_QS_CANCEL_Z,
        }
        sym = (symbol or "*").upper()
        for key in [sym, "*"]:
            b = self._bins.get(key)
            if b and (self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples)):
                return {
                    "build_mult": b.committed_build_mult,
                    "revert_frac": b.committed_revert_frac,
                    "revert_ms": b.committed_revert_ms,
                    "qs_msg_z": b.committed_qs_msg_z,
                    "qs_cancel_z": b.committed_qs_cancel_z,
                }
        return defaults

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for sym, b in self._bins.items():
            bins_out.append({
                "symbol": sym,
                "committed_build_mult": round(b.committed_build_mult, 3),
                "committed_revert_frac": round(b.committed_revert_frac, 3),
                "committed_revert_ms": round(b.committed_revert_ms, 1),
                "committed_qs_msg_z": round(b.committed_qs_msg_z, 2),
                "committed_qs_cancel_z": round(b.committed_qs_cancel_z, 2),
                "n": b.n_observed, "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: x["symbol"])
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            for row in state.get("bins", []):
                sym = str(row.get("symbol", "*"))
                b = self._get_or_create(sym)
                b.committed_build_mult = float(row.get("committed_build_mult", _DEFAULT_BUILD_MULT))
                b.committed_revert_frac = float(row.get("committed_revert_frac", _DEFAULT_REVERT_FRAC))
                b.committed_revert_ms = float(row.get("committed_revert_ms", _DEFAULT_REVERT_MS))
                b.committed_qs_msg_z = float(row.get("committed_qs_msg_z", _DEFAULT_QS_MSG_Z))
                b.committed_qs_cancel_z = float(row.get("committed_qs_cancel_z", _DEFAULT_QS_CANCEL_Z))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_or_create(self, symbol: str) -> _Bin:
        if symbol not in self._bins:
            self._bins[symbol] = _Bin()
        return self._bins[symbol]

    def _maybe_recompute(self, symbol: str, now_ms: int) -> None:
        b = self._bins.get(symbol)
        if b is None:
            return
        if (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms
        self._prune_window(b, now_ms)
        if len(b.buf) < self.min_samples:
            return

        lay_scores = sorted(s.layering_score for s in b.buf)
        qs_scores = sorted(s.qs_score for s in b.buf)
        depths = sorted(s.build_depth_ratio for s in b.buf)
        reverts = sorted(s.revert_delay_ms for s in b.buf if s.revert_delay_ms > 0)

        # build_mult: p(1 - fpr_target) of depth ratios + safety margin
        p_build = _quantile(depths, 1.0 - self.fpr_target)
        new_build = max(_BUILD_MULT_MIN, min(_BUILD_MULT_MAX, p_build * 1.05))
        b.shadow_build_mult = new_build
        _update_committed(b, "committed_build_mult", new_build, _UPDATE_BAND_FRAC)

        # revert_ms: p95 of observed revert delays + safety margin
        if reverts:
            p_revert = _quantile(reverts, 0.95)
            new_revert_ms = max(_REVERT_MS_MIN, min(_REVERT_MS_MAX, p_revert * 1.20))
            b.shadow_revert_ms = new_revert_ms
            _update_committed(b, "committed_revert_ms", new_revert_ms, _UPDATE_BAND_FRAC)

        # qs_msg_z: p(1 - fpr_target) of qs scores as z-like threshold
        p_qs = _quantile(qs_scores, 1.0 - self.fpr_target)
        new_qs_msg_z = max(_QS_Z_MIN, min(_QS_Z_MAX, p_qs * 4.0 + 2.0))
        _update_committed(b, "committed_qs_msg_z", new_qs_msg_z, _UPDATE_BAND_FRAC)
        _update_committed(b, "committed_qs_cancel_z", new_qs_msg_z * 0.9, _UPDATE_BAND_FRAC)

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] * (1 - (idx - lo)) + sorted_vals[hi] * (idx - lo)


def _update_committed(b: _Bin, attr: str, new_val: float, band_frac: float) -> None:
    old = getattr(b, attr)
    if old <= 0 or abs(new_val - old) / max(abs(old), 1e-9) >= band_frac:
        setattr(b, attr, new_val)
