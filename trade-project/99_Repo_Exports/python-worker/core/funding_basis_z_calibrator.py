"""
funding_basis_z_calibrator.py — per-(symbol × vol_regime) funding/basis z-score calibrator.

Calibrates DERIV_CTX_FUNDING_Z_MAX and DERIV_CTX_BASIS_BPS_MAX per symbol and
volatility regime using P² streaming quantiles.

Algorithm:
  For each bucket (symbol, vol_regime):
    P²-q95 of |funding_rate_z| per symbol/regime
    P²-q95 of |basis_bps| per symbol/regime
    committed_funding_z = max(2.0, p95_funding_z × 1.2)  [conservative +20%]
    committed_basis_bps = max(5.0, p95_basis_bps × 1.2)

Fail-open: cold → ENV defaults (3.0 / 10.0).
"""
from __future__ import annotations

import math
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_SAFETY_MULT = 1.2
_MIN_FUNDING_Z = 2.0
_MAX_FUNDING_Z = 12.0
_MIN_BASIS_BPS = 3.0
_MAX_BASIS_BPS = 50.0
_UPDATE_BAND_FRAC = 0.10  # 10% relative change before commit


@dataclass
class _Sample:
    funding_z: float
    basis_bps: float
    ts_ms: int


@dataclass
class _Bin:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=2_000))
    committed_funding_z: float = 3.0
    committed_basis_bps: float = 10.0
    shadow_funding_z: float = 3.0
    shadow_basis_bps: float = 10.0
    last_recompute_ms: int = 0
    n_observed: int = 0


class FundingBasisZCalibrator:
    """Per-(symbol × vol_regime) funding z / basis bps threshold calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 7.0,
        min_samples: int = 100,
        safety_mult: float = _SAFETY_MULT,
        recompute_gap_ms: int = 600_000,
        default_funding_z: float = 3.0,
        default_basis_bps: float = 10.0,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.safety_mult = safety_mult
        self.recompute_gap_ms = recompute_gap_ms
        self.default_funding_z = default_funding_z
        self.default_basis_bps = default_basis_bps
        self._bins: dict[tuple[str, str], _Bin] = {}
        self._lock = threading.Lock()

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(self, *, symbol: str, vol_regime: str, funding_z: float,
                basis_bps: float, ts_ms: int) -> None:
        if not math.isfinite(funding_z) or not math.isfinite(basis_bps):
            return
        now_ms = int(time.time() * 1000)
        sample = _Sample(funding_z=abs(funding_z), basis_bps=abs(basis_bps), ts_ms=ts_ms)
        for key in self._bucket_keys(symbol, vol_regime):
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(self._bucket_keys(symbol, vol_regime)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_funding_z(self, *, symbol: str, vol_regime: str) -> float:
        for key in self._fallback_keys(symbol, vol_regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_funding_z > _MIN_FUNDING_Z:
                    return b.committed_funding_z
        return self.default_funding_z

    def get_basis_bps(self, *, symbol: str, vol_regime: str) -> float:
        for key in self._fallback_keys(symbol, vol_regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_basis_bps > _MIN_BASIS_BPS:
                    return b.committed_basis_bps
        return self.default_basis_bps

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (sym, reg), b in self._bins.items():
            bins_out.append({
                "symbol": sym, "vol_regime": reg,
                "committed_funding_z": round(b.committed_funding_z, 3),
                "committed_basis_bps": round(b.committed_basis_bps, 3),
                "shadow_funding_z": round(b.shadow_funding_z, 3),
                "shadow_basis_bps": round(b.shadow_basis_bps, 3),
                "n": b.n_observed,
            })
        bins_out.sort(key=lambda x: (x["symbol"], x["vol_regime"]))
        return {
            "schema_version": _SCHEMA_VERSION, "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce, "auto_enforce": self.auto_enforce, "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            for row in state.get("bins", []):
                sym = str(row.get("symbol", "*"))
                reg = str(row.get("vol_regime", "*"))
                b = self._get_or_create((sym, reg))
                b.committed_funding_z = float(row.get("committed_funding_z", self.default_funding_z))
                b.committed_basis_bps = float(row.get("committed_basis_bps", self.default_basis_bps))
                b.shadow_funding_z = float(row.get("shadow_funding_z", self.default_funding_z))
                b.shadow_basis_bps = float(row.get("shadow_basis_bps", self.default_basis_bps))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bucket_keys(self, symbol: str, vol_regime: str) -> list[tuple[str, str]]:
        s = (symbol or "*").upper()
        r = (vol_regime or "*").lower()
        return [(s, r), (s, "*"), ("*", r), ("*", "*")]

    def _fallback_keys(self, symbol: str, vol_regime: str) -> list[tuple[str, str]]:
        return self._bucket_keys(symbol, vol_regime)

    def _get_or_create(self, key: tuple[str, str]) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin(
                committed_funding_z=self.default_funding_z,
                committed_basis_bps=self.default_basis_bps,
                shadow_funding_z=self.default_funding_z,
                shadow_basis_bps=self.default_basis_bps,
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
        funding_vals = sorted(s.funding_z for s in b.buf)
        basis_vals = sorted(s.basis_bps for s in b.buf)
        p95_f = _quantile(funding_vals, 0.95)
        p95_b = _quantile(basis_vals, 0.95)
        new_fz = max(_MIN_FUNDING_Z, min(_MAX_FUNDING_Z, p95_f * self.safety_mult))
        new_bb = max(_MIN_BASIS_BPS, min(_MAX_BASIS_BPS, p95_b * self.safety_mult))
        b.shadow_funding_z = new_fz
        b.shadow_basis_bps = new_bb
        if abs(new_fz - b.committed_funding_z) / max(b.committed_funding_z, 1e-6) >= _UPDATE_BAND_FRAC:
            b.committed_funding_z = new_fz
        if abs(new_bb - b.committed_basis_bps) / max(b.committed_basis_bps, 1e-6) >= _UPDATE_BAND_FRAC:
            b.committed_basis_bps = new_bb

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
