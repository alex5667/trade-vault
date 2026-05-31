"""
signal_min_conf_calibrator.py — EV-grid per (kind × regime) confidence threshold calibrator.

Reads trades:closed, computes EV-weighted optimal confidence cutoff τ per bucket.
Algorithm:
  For each bucket B = (kind × regime):
    grid τ ∈ [MIN_THR..MAX_THR step 1]:
      ev(τ) = mean(R | conf_pct >= τ) × coverage(τ)
    committed_thr = argmax ev(τ) clamped [MIN_THR, MAX_THR]
    hysteresis: only flip if |Δ| >= UPDATE_BAND

Fallback hierarchy (reader): (kind, regime) → (*, regime) → (kind, *) → global
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_MIN_THR = 50.0
_MAX_THR = 95.0
_STEP = 1.0
_COVERAGE_FLOOR = 0.02   # τ must keep at least 2% of samples
_UPDATE_BAND = 1.5       # minimum Δ to commit new threshold
_MAX_JUMP = 10.0         # max single-step change


@dataclass
class _Sample:
    conf_pct: float   # 0-100 normalised
    r_multiple: float
    ts_ms: int
    w: float = 1.0    # IPS weight


@dataclass
class _Bin:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=20_000))
    committed_thr: float = _MIN_THR
    shadow_thr: float = _MIN_THR
    last_recompute_ms: int = 0
    n_observed: int = 0


class SignalMinConfCalibrator:
    """Per-(kind × regime) EV-grid confidence threshold calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 7.0,
        min_samples: int = 100,
        target_coverage: float = 0.25,
        recompute_gap_ms: int = 300_000,
        default_thr: float = 70.0,
        min_thr: float = _MIN_THR,
        max_thr: float = _MAX_THR,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.target_coverage = target_coverage
        self.recompute_gap_ms = recompute_gap_ms
        self.default_thr = default_thr
        self.min_thr = min_thr
        self.max_thr = max_thr
        self._bins: dict[tuple[str, str], _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        kind: str,
        regime: str,
        conf_pct: float,
        r_multiple: float,
        ts_ms: int,
        w: float = 1.0,
    ) -> None:
        if not math.isfinite(conf_pct) or not math.isfinite(r_multiple):
            return
        if conf_pct < 0 or conf_pct > 100:
            return
        now_ms = int(time.time() * 1000)
        sample = _Sample(conf_pct=conf_pct, r_multiple=r_multiple, ts_ms=ts_ms, w=max(0.01, w))
        for key in self._bucket_keys(kind, regime):
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(self._bucket_keys(kind, regime)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_threshold(self, *, kind: str, regime: str) -> float:
        for key in self._fallback_keys(kind, regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_thr > self.min_thr:
                    return b.committed_thr
        return self.default_thr

    def get_shadow(self, *, kind: str, regime: str) -> float:
        for key in self._fallback_keys(kind, regime):
            b = self._bins.get(key)
            if b:
                return b.shadow_thr
        return self.default_thr

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (knd, reg), b in self._bins.items():
            bins_out.append({
                "kind": knd, "regime": reg,
                "committed_thr": round(b.committed_thr, 2),
                "shadow_thr": round(b.shadow_thr, 2),
                "n": b.n_observed,
                "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: (x["kind"], x["regime"]))
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "default_thr": self.default_thr,
            "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            for row in state.get("bins", []):
                knd = str(row.get("kind", "*"))
                reg = str(row.get("regime", "*"))
                b = self._get_or_create((knd, reg))
                b.committed_thr = float(row.get("committed_thr", self.default_thr))
                b.shadow_thr = float(row.get("shadow_thr", self.default_thr))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bucket_keys(self, kind: str, regime: str) -> list[tuple[str, str]]:
        k = (kind or "*").strip().lower()
        r = (regime or "*").strip().lower()
        return [(k, r), ("*", r), (k, "*"), ("*", "*")]

    def _fallback_keys(self, kind: str, regime: str) -> list[tuple[str, str]]:
        return self._bucket_keys(kind, regime)

    def _get_or_create(self, key: tuple[str, str]) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin(committed_thr=self.default_thr, shadow_thr=self.default_thr)
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
        shadow = self._ev_grid(b)
        if shadow is None:
            return
        b.shadow_thr = shadow
        if abs(shadow - b.committed_thr) >= _UPDATE_BAND:
            delta = shadow - b.committed_thr
            if abs(delta) > _MAX_JUMP:
                delta = math.copysign(_MAX_JUMP, delta)
            b.committed_thr = max(self.min_thr, min(self.max_thr, b.committed_thr + delta))

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()

    def _ev_grid(self, b: _Bin) -> float | None:
        samples = list(b.buf)
        if not samples:
            return None
        total_w = sum(s.w for s in samples)
        if total_w <= 0:
            return None
        best_tau = None
        best_ev = -1e9
        tau = self.min_thr
        while tau <= self.max_thr:
            subset = [s for s in samples if s.conf_pct >= tau]
            if not subset:
                tau += _STEP
                continue
            coverage = sum(s.w for s in subset) / total_w
            if coverage < _COVERAGE_FLOOR:
                tau += _STEP
                continue
            mean_r = sum(s.r_multiple * s.w for s in subset) / sum(s.w for s in subset)
            ev = mean_r * coverage
            if ev > best_ev:
                best_ev = ev
                best_tau = tau
            tau += _STEP
        return best_tau
