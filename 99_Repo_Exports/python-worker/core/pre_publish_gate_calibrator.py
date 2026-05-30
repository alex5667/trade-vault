"""
pre_publish_gate_calibrator.py — per-(symbol × regime) adaptive calibration
of delta_z and OBI thresholds used in ConsistencyGate / pre-publish filters.

Algorithm:
  For delta_z:
    per-symbol EWMA MAD of observed delta_z values from signals:of:inputs
    threshold = median(delta_z) + GATE_MAD_Z_MULT × MAD(delta_z)
    (robust z-score based adaptive gate)

  For OBI:
    per-symbol rolling p75(|obi|)
    threshold = p75(|obi|) × SAFETY_MULT

Fallback hierarchy (reader): (symbol, regime) → (symbol, *) → (*, *) → global
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_DEFAULT_DELTA_Z_THR = 2.0
_DEFAULT_OBI_THR = 0.35
_MIN_DELTA_Z_THR = 0.5
_MAX_DELTA_Z_THR = 20.0
_MIN_OBI_THR = 0.05
_MAX_OBI_THR = 1.0
_UPDATE_BAND_Z = 0.1     # min delta to commit new delta_z threshold
_UPDATE_BAND_OBI = 0.02  # min delta to commit new obi threshold


@dataclass
class _GateSample:
    delta_z: float
    obi_abs: float
    ts_ms: int
    w: float = 1.0


@dataclass
class _GateBin:
    buf: deque[_GateSample] = field(default_factory=lambda: deque(maxlen=10_000))
    committed_delta_z_thr: float = _DEFAULT_DELTA_Z_THR
    shadow_delta_z_thr: float = _DEFAULT_DELTA_Z_THR
    committed_obi_thr: float = _DEFAULT_OBI_THR
    shadow_obi_thr: float = _DEFAULT_OBI_THR
    last_recompute_ms: int = 0
    n_observed: int = 0


def _weighted_quantile(values: list[float], weights: list[float], q: float) -> float:
    """Weighted quantile via sorted linear interpolation."""
    if not values:
        return 0.0
    pairs = sorted(zip(values, weights), key=lambda x: x[0])
    total_w = sum(w for _, w in pairs)
    if total_w <= 0:
        return 0.0
    target = q * total_w
    cumulative = 0.0
    for v, w in pairs:
        cumulative += w
        if cumulative >= target:
            return v
    return pairs[-1][0]


def _weighted_median(values: list[float], weights: list[float]) -> float:
    return _weighted_quantile(values, weights, 0.5)


def _weighted_mad(values: list[float], weights: list[float], med: float) -> float:
    """Weighted MAD = weighted median of |x - median|."""
    abs_devs = [abs(v - med) for v in values]
    return _weighted_median(abs_devs, weights)


class PrePublishGateCalibrator:
    """Per-(symbol × regime) adaptive threshold calibrator for delta_z and OBI gates."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_hours: float = 24.0,
        min_samples: int = 50,
        recompute_gap_ms: int = 60_000,
        gate_mad_z_mult: float = 1.5,
        obi_safety_mult: float = 1.2,
        default_delta_z_thr: float = _DEFAULT_DELTA_Z_THR,
        default_obi_thr: float = _DEFAULT_OBI_THR,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_hours * 3_600_000)
        self.min_samples = min_samples
        self.recompute_gap_ms = recompute_gap_ms
        self.gate_mad_z_mult = gate_mad_z_mult
        self.obi_safety_mult = obi_safety_mult
        self.default_delta_z_thr = default_delta_z_thr
        self.default_obi_thr = default_obi_thr
        self._bins: dict[tuple[str, str], _GateBin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        regime: str,
        delta_z: float,
        obi: float,
        ts_ms: int,
        w: float = 1.0,
    ) -> None:
        if not math.isfinite(delta_z) or not math.isfinite(obi):
            return
        now_ms = int(time.time() * 1000)
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        sample = _GateSample(
            delta_z=abs(delta_z),    # use magnitude for threshold calibration
            obi_abs=abs(obi),
            ts_ms=ts_ms,
            w=max(0.01, w),
        )
        # feed into (sym, reg), (sym, *), (*, *)
        for key in self._bucket_keys(sym, reg):
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(self._primary_key(sym, reg), now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_delta_z_thr(self, symbol: str, regime: str) -> float:
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        for key in self._fallback_keys(sym, reg):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_delta_z_thr >= _MIN_DELTA_Z_THR:
                    return b.committed_delta_z_thr
        return self.default_delta_z_thr

    def get_obi_thr(self, symbol: str, regime: str) -> float:
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        for key in self._fallback_keys(sym, reg):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_obi_thr >= _MIN_OBI_THR:
                    return b.committed_obi_thr
        return self.default_obi_thr

    def get_shadow_delta_z(self, symbol: str, regime: str) -> float:
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        for key in self._fallback_keys(sym, reg):
            b = self._bins.get(key)
            if b:
                return b.shadow_delta_z_thr
        return self.default_delta_z_thr

    def get_shadow_obi(self, symbol: str, regime: str) -> float:
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        for key in self._fallback_keys(sym, reg):
            b = self._bins.get(key)
            if b:
                return b.shadow_obi_thr
        return self.default_obi_thr

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (sym, reg), b in self._bins.items():
            bins_out.append({
                "symbol": sym,
                "regime": reg,
                "committed_delta_z_thr": round(b.committed_delta_z_thr, 4),
                "shadow_delta_z_thr": round(b.shadow_delta_z_thr, 4),
                "committed_obi_thr": round(b.committed_obi_thr, 4),
                "shadow_obi_thr": round(b.shadow_obi_thr, 4),
                "n": b.n_observed,
                "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: (x["symbol"], x["regime"]))
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "default_delta_z_thr": self.default_delta_z_thr,
            "default_obi_thr": self.default_obi_thr,
            "gate_mad_z_mult": self.gate_mad_z_mult,
            "obi_safety_mult": self.obi_safety_mult,
            "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            if "default_delta_z_thr" in state:
                self.default_delta_z_thr = float(state["default_delta_z_thr"])
            if "default_obi_thr" in state:
                self.default_obi_thr = float(state["default_obi_thr"])
            if "gate_mad_z_mult" in state:
                self.gate_mad_z_mult = float(state["gate_mad_z_mult"])
            if "obi_safety_mult" in state:
                self.obi_safety_mult = float(state["obi_safety_mult"])
            for row in state.get("bins", []):
                sym = str(row.get("symbol", "*"))
                reg = str(row.get("regime", "*"))
                b = self._get_or_create((sym, reg))
                b.committed_delta_z_thr = float(row.get("committed_delta_z_thr", self.default_delta_z_thr))
                b.shadow_delta_z_thr = float(row.get("shadow_delta_z_thr", self.default_delta_z_thr))
                b.committed_obi_thr = float(row.get("committed_obi_thr", self.default_obi_thr))
                b.shadow_obi_thr = float(row.get("shadow_obi_thr", self.default_obi_thr))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _primary_key(self, sym: str, reg: str) -> tuple[str, str]:
        return (sym, reg)

    def _bucket_keys(self, sym: str, reg: str) -> list[tuple[str, str]]:
        return [(sym, reg), (sym, "*"), ("*", "*")]

    def _fallback_keys(self, sym: str, reg: str) -> list[tuple[str, str]]:
        return [(sym, reg), (sym, "*"), ("*", "*")]

    def _get_or_create(self, key: tuple[str, str]) -> _GateBin:
        if key not in self._bins:
            self._bins[key] = _GateBin(
                committed_delta_z_thr=self.default_delta_z_thr,
                shadow_delta_z_thr=self.default_delta_z_thr,
                committed_obi_thr=self.default_obi_thr,
                shadow_obi_thr=self.default_obi_thr,
            )
        return self._bins[key]

    def _maybe_recompute(self, primary_key: tuple[str, str], now_ms: int) -> None:
        b = self._bins.get(primary_key)
        if b is None:
            return
        if (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms
        self._prune_window(b, now_ms)
        if len(b.buf) < self.min_samples:
            return

        new_z_thr, new_obi_thr = self._recompute(b)

        if new_z_thr is not None:
            b.shadow_delta_z_thr = new_z_thr
            if abs(new_z_thr - b.committed_delta_z_thr) >= _UPDATE_BAND_Z:
                b.committed_delta_z_thr = max(_MIN_DELTA_Z_THR, min(_MAX_DELTA_Z_THR, new_z_thr))

        if new_obi_thr is not None:
            b.shadow_obi_thr = new_obi_thr
            if abs(new_obi_thr - b.committed_obi_thr) >= _UPDATE_BAND_OBI:
                b.committed_obi_thr = max(_MIN_OBI_THR, min(_MAX_OBI_THR, new_obi_thr))

    def _prune_window(self, b: _GateBin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()

    def _recompute(self, b: _GateBin) -> tuple[float | None, float | None]:
        samples = list(b.buf)
        if not samples:
            return None, None
        zs = [s.delta_z for s in samples]
        obis = [s.obi_abs for s in samples]
        ws = [s.w for s in samples]

        # delta_z: median + mult × MAD
        z_med = _weighted_median(zs, ws)
        z_mad = _weighted_mad(zs, ws, z_med)
        new_z_thr = z_med + self.gate_mad_z_mult * z_mad
        if new_z_thr < _MIN_DELTA_Z_THR:
            new_z_thr = _MIN_DELTA_Z_THR

        # OBI: p75 × safety mult
        obi_p75 = _weighted_quantile(obis, ws, 0.75)
        new_obi_thr = obi_p75 * self.obi_safety_mult
        if new_obi_thr < _MIN_OBI_THR:
            new_obi_thr = _MIN_OBI_THR

        return (
            max(_MIN_DELTA_Z_THR, min(_MAX_DELTA_Z_THR, new_z_thr)),
            max(_MIN_OBI_THR, min(_MAX_OBI_THR, new_obi_thr)),
        )
