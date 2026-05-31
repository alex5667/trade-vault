"""
sl_atr_floor_calibrator.py — per-(symbol × venue) SL ATR floor multiplier calibrator.

Calibrates SL_ATR_MULT_FLOOR per symbol using p25(SL_dist_to_ATR | realized winners).
Goal: floor adapts to actual execution environment (spot=narrow, perp=wider).

Algorithm:
  For each bucket (symbol, venue):
    Collect (sl_dist_bps / atr_bps) ratios from closed trades
    committed_floor = max(GLOBAL_FLOOR, p25(sl_to_atr_ratio | ALL_TRADES))
    — conservative: uses all trades (not winners only), capped [0.50, 1.50]

Output: autocal:sl_atr_floor:state
Master switch: SL_ATR_FLOOR_CAL_ENFORCE=0
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_GLOBAL_FLOOR = 0.50    # minimum SL/ATR ratio
_MAX_FLOOR = 1.50       # cap to avoid over-widening SL
_Q_TARGET = 0.25        # p25 — conservative (protects from too-tight SL)
_UPDATE_BAND = 0.03     # min absolute Δ to commit


@dataclass
class _Sample:
    sl_to_atr: float
    ts_ms: int


@dataclass
class _Bin:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=5_000))
    committed_floor: float = 0.78
    shadow_floor: float = 0.78
    last_recompute_ms: int = 0
    n_observed: int = 0


class SLATRFloorCalibrator:
    """Per-(symbol × venue) SL ATR floor multiplier calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 14.0,
        min_samples: int = 30,
        recompute_gap_ms: int = 600_000,
        default_floor: float = 0.78,
        global_floor: float = _GLOBAL_FLOOR,
        max_floor: float = _MAX_FLOOR,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.recompute_gap_ms = recompute_gap_ms
        self.default_floor = default_floor
        self.global_floor = global_floor
        self.max_floor = max_floor
        self._bins: dict[tuple[str, str], _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        venue: str,
        sl_bps: float,
        atr_bps: float,
        ts_ms: int,
    ) -> None:
        if not math.isfinite(sl_bps) or not math.isfinite(atr_bps) or atr_bps <= 0:
            return
        ratio = sl_bps / atr_bps
        if ratio <= 0 or not math.isfinite(ratio):
            return
        now_ms = int(time.time() * 1000)
        sample = _Sample(sl_to_atr=ratio, ts_ms=ts_ms)
        for key in self._bucket_keys(symbol, venue):
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(self._bucket_keys(symbol, venue)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_floor(self, *, symbol: str, venue: str) -> float:
        for key in self._fallback_keys(symbol, venue):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_floor >= self.global_floor:
                    return b.committed_floor
        return self.default_floor

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (sym, venue), b in self._bins.items():
            bins_out.append({
                "symbol": sym, "venue": venue,
                "committed_floor": round(b.committed_floor, 4),
                "shadow_floor": round(b.shadow_floor, 4),
                "n": b.n_observed, "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: (x["symbol"], x["venue"]))
        return {
            "schema_version": _SCHEMA_VERSION, "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce, "auto_enforce": self.auto_enforce,
            "default_floor": self.default_floor, "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            for row in state.get("bins", []):
                sym = str(row.get("symbol", "*"))
                venue = str(row.get("venue", "*"))
                b = self._get_or_create((sym, venue))
                b.committed_floor = float(row.get("committed_floor", self.default_floor))
                b.shadow_floor = float(row.get("shadow_floor", self.default_floor))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bucket_keys(self, symbol: str, venue: str) -> list[tuple[str, str]]:
        s = (symbol or "*").upper()
        v = (venue or "*").lower()
        return [(s, v), (s, "*"), ("*", v), ("*", "*")]

    def _fallback_keys(self, symbol: str, venue: str) -> list[tuple[str, str]]:
        return self._bucket_keys(symbol, venue)

    def _get_or_create(self, key: tuple[str, str]) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin(committed_floor=self.default_floor, shadow_floor=self.default_floor)
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
        vals = sorted(s.sl_to_atr for s in b.buf)
        p25 = _quantile(vals, _Q_TARGET)
        new_floor = max(self.global_floor, min(self.max_floor, p25))
        b.shadow_floor = new_floor
        if abs(new_floor - b.committed_floor) >= _UPDATE_BAND:
            b.committed_floor = new_floor

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
