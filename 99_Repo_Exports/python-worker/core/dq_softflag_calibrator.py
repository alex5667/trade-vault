"""
dq_softflag_calibrator.py — per-symbol adaptive DQ soft-flag thresholds.

Calibrates DQ_BOOK_STALE_FLAG_MS and DQ_SPREAD_WIDE_FLAG_BPS per symbol from
observed book update intervals and bid-ask spreads.

Algorithm:
  For each symbol:
    stale_flag_ms  = max(GLOBAL_MIN_MS, p95(book_update_dt_ms) × 2.0)
    spread_flag_bps = max(GLOBAL_MIN_BPS, p75(spread_bps) × 2.0)

Fail-open: cold → ENV defaults (1500ms / 12.0 bps).
Output: autocal:dq_soft_flag:state (HASH, one field per symbol)
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_STALE_MULT = 2.0
_SPREAD_MULT = 2.0
_MIN_STALE_MS = 200
_MAX_STALE_MS = 10_000
_MIN_SPREAD_BPS = 2.0
_MAX_SPREAD_BPS = 100.0
_UPDATE_BAND_FRAC = 0.15  # 15% change before commit


@dataclass
class _Sample:
    value: float
    ts_ms: int


@dataclass
class _Bin:
    stale_buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=2_000))
    spread_buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=2_000))
    committed_stale_ms: int = 1500
    committed_spread_bps: float = 12.0
    shadow_stale_ms: int = 1500
    shadow_spread_bps: float = 12.0
    last_recompute_ms: int = 0
    n_stale: int = 0
    n_spread: int = 0


class DQSoftFlagCalibrator:
    """Per-symbol DQ soft-flag threshold calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 3.0,
        min_samples: int = 100,
        recompute_gap_ms: int = 300_000,
        default_stale_ms: int = 1500,
        default_spread_bps: float = 12.0,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.recompute_gap_ms = recompute_gap_ms
        self.default_stale_ms = default_stale_ms
        self.default_spread_bps = default_spread_bps
        self._bins: dict[str, _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe_book_dt(self, *, symbol: str, dt_ms: float, ts_ms: int) -> None:
        if not math.isfinite(dt_ms) or dt_ms <= 0:
            return
        now_ms = int(time.time() * 1000)
        for key in [symbol.upper(), "*"]:
            b = self._get_or_create(key)
            b.stale_buf.append(_Sample(value=dt_ms, ts_ms=ts_ms))
            b.n_stale += 1
        self._maybe_recompute(symbol.upper(), now_ms)

    def observe_spread(self, *, symbol: str, spread_bps: float, ts_ms: int) -> None:
        if not math.isfinite(spread_bps) or spread_bps <= 0:
            return
        now_ms = int(time.time() * 1000)
        for key in [symbol.upper(), "*"]:
            b = self._get_or_create(key)
            b.spread_buf.append(_Sample(value=spread_bps, ts_ms=ts_ms))
            b.n_spread += 1
        self._maybe_recompute(symbol.upper(), now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_stale_flag_ms(self, symbol: str) -> int:
        for key in [symbol.upper(), "*"]:
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_stale >= self.min_samples):
                if b.committed_stale_ms > 0:
                    return b.committed_stale_ms
        return self.default_stale_ms

    def get_spread_flag_bps(self, symbol: str) -> float:
        for key in [symbol.upper(), "*"]:
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_spread >= self.min_samples):
                if b.committed_spread_bps > 0:
                    return b.committed_spread_bps
        return self.default_spread_bps

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for sym, b in self._bins.items():
            bins_out.append({
                "symbol": sym,
                "committed_stale_ms": b.committed_stale_ms,
                "committed_spread_bps": round(b.committed_spread_bps, 2),
                "shadow_stale_ms": b.shadow_stale_ms,
                "shadow_spread_bps": round(b.shadow_spread_bps, 2),
                "n_stale": b.n_stale, "n_spread": b.n_spread,
            })
        bins_out.sort(key=lambda x: x["symbol"])
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
                b = self._get_or_create(sym)
                b.committed_stale_ms = int(row.get("committed_stale_ms", self.default_stale_ms))
                b.committed_spread_bps = float(row.get("committed_spread_bps", self.default_spread_bps))
                b.shadow_stale_ms = int(row.get("shadow_stale_ms", self.default_stale_ms))
                b.shadow_spread_bps = float(row.get("shadow_spread_bps", self.default_spread_bps))
                b.n_stale = int(row.get("n_stale", 0))
                b.n_spread = int(row.get("n_spread", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_or_create(self, symbol: str) -> _Bin:
        if symbol not in self._bins:
            self._bins[symbol] = _Bin(
                committed_stale_ms=self.default_stale_ms,
                committed_spread_bps=self.default_spread_bps,
                shadow_stale_ms=self.default_stale_ms,
                shadow_spread_bps=self.default_spread_bps,
            )
        return self._bins[symbol]

    def _maybe_recompute(self, symbol: str, now_ms: int) -> None:
        b = self._bins.get(symbol)
        if b is None:
            return
        if (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms
        self._prune_window(b, now_ms)

        if len(b.stale_buf) >= self.min_samples:
            vals = sorted(s.value for s in b.stale_buf)
            p95 = _quantile(vals, 0.95)
            new_stale = int(max(_MIN_STALE_MS, min(_MAX_STALE_MS, p95 * _STALE_MULT)))
            b.shadow_stale_ms = new_stale
            if abs(new_stale - b.committed_stale_ms) / max(b.committed_stale_ms, 1) >= _UPDATE_BAND_FRAC:
                b.committed_stale_ms = new_stale

        if len(b.spread_buf) >= self.min_samples:
            vals = sorted(s.value for s in b.spread_buf)
            p75 = _quantile(vals, 0.75)
            new_spread = max(_MIN_SPREAD_BPS, min(_MAX_SPREAD_BPS, p75 * _SPREAD_MULT))
            b.shadow_spread_bps = new_spread
            if abs(new_spread - b.committed_spread_bps) / max(b.committed_spread_bps, 1e-6) >= _UPDATE_BAND_FRAC:
                b.committed_spread_bps = new_spread

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        for buf in (b.stale_buf, b.spread_buf):
            while buf and buf[0].ts_ms < cutoff:
                buf.popleft()


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] * (1 - (idx - lo)) + sorted_vals[hi] * (idx - lo)
