"""
entry_rr_min_calibrator.py — per-(side × regime) RR floor calibrator.

Input: trades:closed.  For winning trades computes p25(R_realized) per (side × regime)
and uses that as the adaptive RR minimum floor.

Algorithm:
  For each bucket (side, regime):
    winners = [r | r > 0, result in TP_RESULTS]
    rr_min = max(GLOBAL_FLOOR, p25(winners)) if len(winners) >= min_samples

  Fallback: (side, regime) → (side, *) → (*, regime) → global
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_GLOBAL_FLOOR = 1.0      # never go below 1:1
_MAX_FLOOR = 3.0         # cap to avoid over-restriction
_WINNER_RESULTS = frozenset({"TP", "TP1", "TP2", "TP3", "PARTIAL_TP", "tp", "tp1"})
_UPDATE_BAND = 0.05      # min Δ to commit
_MAX_JUMP = 0.3


@dataclass
class _Sample:
    r_multiple: float
    ts_ms: int
    w: float = 1.0


@dataclass
class _Bin:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=10_000))
    committed_rr_min: float = _GLOBAL_FLOOR
    shadow_rr_min: float = _GLOBAL_FLOOR
    last_recompute_ms: int = 0
    n_observed: int = 0


class EntryRRMinCalibrator:
    """Per-(side × regime) p25-winner RR floor calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 14.0,
        min_samples: int = 50,
        quantile: float = 0.25,
        recompute_gap_ms: int = 600_000,
        default_rr: float = 1.3,
        global_floor: float = _GLOBAL_FLOOR,
        max_floor: float = _MAX_FLOOR,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.quantile = quantile
        self.recompute_gap_ms = recompute_gap_ms
        self.default_rr = default_rr
        self.global_floor = global_floor
        self.max_floor = max_floor
        self._bins: dict[tuple[str, str], _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        side: str,
        regime: str,
        r_multiple: float,
        result: str,
        ts_ms: int,
        w: float = 1.0,
    ) -> None:
        if not math.isfinite(r_multiple):
            return
        result_norm = (result or "").strip().upper()
        if result_norm not in {r.upper() for r in _WINNER_RESULTS}:
            return  # only winners matter for RR floor
        now_ms = int(time.time() * 1000)
        sample = _Sample(r_multiple=r_multiple, ts_ms=ts_ms, w=max(0.01, w))
        for key in self._bucket_keys(side, regime):
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(self._bucket_keys(side, regime)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_rr_min(self, *, side: str, regime: str) -> float:
        for key in self._fallback_keys(side, regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_rr_min > self.global_floor:
                    return b.committed_rr_min
        return self.default_rr

    def get_shadow(self, *, side: str, regime: str) -> float:
        for key in self._fallback_keys(side, regime):
            b = self._bins.get(key)
            if b:
                return b.shadow_rr_min
        return self.default_rr

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (side, reg), b in self._bins.items():
            bins_out.append({
                "side": side, "regime": reg,
                "committed_rr_min": round(b.committed_rr_min, 3),
                "shadow_rr_min": round(b.shadow_rr_min, 3),
                "n": b.n_observed,
                "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: (x["side"], x["regime"]))
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "default_rr": self.default_rr,
            "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            for row in state.get("bins", []):
                side = str(row.get("side", "*"))
                reg = str(row.get("regime", "*"))
                b = self._get_or_create((side, reg))
                b.committed_rr_min = float(row.get("committed_rr_min", self.default_rr))
                b.shadow_rr_min = float(row.get("shadow_rr_min", self.default_rr))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bucket_keys(self, side: str, regime: str) -> list[tuple[str, str]]:
        s = (side or "*").strip().upper()
        if s == "BUY":
            s = "LONG"
        elif s == "SELL":
            s = "SHORT"
        r = (regime or "*").strip().lower()
        return [(s, r), (s, "*"), ("*", r), ("*", "*")]

    def _fallback_keys(self, side: str, regime: str) -> list[tuple[str, str]]:
        return self._bucket_keys(side, regime)

    def _get_or_create(self, key: tuple[str, str]) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin(committed_rr_min=self.default_rr, shadow_rr_min=self.default_rr)
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
        shadow = self._compute_quantile(b)
        if shadow is None:
            return
        b.shadow_rr_min = shadow
        if abs(shadow - b.committed_rr_min) >= _UPDATE_BAND:
            delta = shadow - b.committed_rr_min
            if abs(delta) > _MAX_JUMP:
                delta = math.copysign(_MAX_JUMP, delta)
            new_val = b.committed_rr_min + delta
            b.committed_rr_min = max(self.global_floor, min(self.max_floor, new_val))

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()

    def _compute_quantile(self, b: _Bin) -> float | None:
        samples = sorted(b.buf, key=lambda s: s.r_multiple)
        if not samples:
            return None
        total_w = sum(s.w for s in samples)
        if total_w <= 0:
            return None
        target = self.quantile * total_w
        cumulative = 0.0
        for s in samples:
            cumulative += s.w
            if cumulative >= target:
                return max(self.global_floor, min(self.max_floor, s.r_multiple))
        return None
