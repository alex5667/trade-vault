"""
tb_cost_bps_calibrator.py — per-symbol weekly refit of TB_COST_BPS.

Algorithm:
  cost_bps = 2 × spread_p50 + 2 × fee_bps + slip_p50

  where:
    spread_p50  = weekly median of entry_spread_bps from trades:closed
    slip_p50    = weekly median of entry_slip_bps (fallback: adverse_bps_t)
    fee_bps     = FEE_BPS (default 3.0, maker Binance = CRYPTO_COMMISSION_RATE × 10000)

Fallback hierarchy (reader): (symbol) → global (7.0)
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_DEFAULT_COST_BPS = 7.0
_DEFAULT_FEE_BPS = 3.0           # 0.03% maker, i.e. 3 bps
_MIN_COST_BPS = 1.0
_MAX_COST_BPS = 50.0
_UPDATE_BAND = 0.5               # minimum Δ to commit new value


@dataclass
class _SpreadSample:
    spread_bps: float
    slip_bps: float
    ts_ms: int
    w: float = 1.0


@dataclass
class _Bin:
    buf: deque[_SpreadSample] = field(default_factory=lambda: deque(maxlen=10_000))
    committed_cost_bps: float = _DEFAULT_COST_BPS
    shadow_cost_bps: float = _DEFAULT_COST_BPS
    last_recompute_ms: int = 0
    n_observed: int = 0


def _weighted_median(values: list[float], weights: list[float]) -> float:
    """Weighted median via sorted interpolation."""
    if not values:
        return 0.0
    pairs = sorted(zip(values, weights), key=lambda x: x[0])
    total_w = sum(w for _, w in pairs)
    if total_w <= 0:
        return 0.0
    cumulative = 0.0
    for v, w in pairs:
        cumulative += w
        if cumulative >= total_w / 2.0:
            return v
    return pairs[-1][0]


class TbCostBpsCalibrator:
    """Per-symbol weekly refit calibrator for triple-barrier cost estimate."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 7.0,
        min_samples: int = 50,
        recompute_gap_ms: int = 300_000,
        fee_bps: float = _DEFAULT_FEE_BPS,
        default_cost_bps: float = _DEFAULT_COST_BPS,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.recompute_gap_ms = recompute_gap_ms
        self.fee_bps = fee_bps
        self.default_cost_bps = default_cost_bps
        self._bins: dict[str, _Bin] = {}   # symbol → _Bin

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        spread_bps: float,
        slip_bps: float,
        ts_ms: int,
        w: float = 1.0,
    ) -> None:
        if not math.isfinite(spread_bps) or not math.isfinite(slip_bps):
            return
        if spread_bps < 0 or slip_bps < 0:
            return
        now_ms = int(time.time() * 1000)
        sym = (symbol or "*").strip().upper()
        sample = _SpreadSample(
            spread_bps=spread_bps,
            slip_bps=slip_bps,
            ts_ms=ts_ms,
            w=max(0.01, w),
        )
        # feed into per-symbol bucket + global wildcard
        for key in [sym, "*"]:
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(sym, now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_cost_bps(self, symbol: str) -> float:
        sym = (symbol or "*").strip().upper()
        for key in [sym, "*"]:
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_cost_bps > _MIN_COST_BPS:
                    return b.committed_cost_bps
        return self.default_cost_bps

    def get_shadow(self, symbol: str) -> float:
        sym = (symbol or "*").strip().upper()
        for key in [sym, "*"]:
            b = self._bins.get(key)
            if b:
                return b.shadow_cost_bps
        return self.default_cost_bps

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for sym, b in self._bins.items():
            bins_out.append({
                "symbol": sym,
                "committed_cost_bps": round(b.committed_cost_bps, 3),
                "shadow_cost_bps": round(b.shadow_cost_bps, 3),
                "n": b.n_observed,
                "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: x["symbol"])
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "default_cost_bps": self.default_cost_bps,
            "fee_bps": self.fee_bps,
            "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            if "default_cost_bps" in state:
                self.default_cost_bps = float(state["default_cost_bps"])
            if "fee_bps" in state:
                self.fee_bps = float(state["fee_bps"])
            for row in state.get("bins", []):
                sym = str(row.get("symbol", "*"))
                b = self._get_or_create(sym)
                b.committed_cost_bps = float(row.get("committed_cost_bps", self.default_cost_bps))
                b.shadow_cost_bps = float(row.get("shadow_cost_bps", self.default_cost_bps))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_or_create(self, key: str) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin(
                committed_cost_bps=self.default_cost_bps,
                shadow_cost_bps=self.default_cost_bps,
            )
        return self._bins[key]

    def _maybe_recompute(self, sym: str, now_ms: int) -> None:
        b = self._bins.get(sym)
        if b is None:
            return
        if (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms
        self._prune_window(b, now_ms)
        if len(b.buf) < self.min_samples:
            return
        shadow = self._recompute(b)
        if shadow is None:
            return
        b.shadow_cost_bps = shadow
        if abs(shadow - b.committed_cost_bps) >= _UPDATE_BAND:
            b.committed_cost_bps = max(_MIN_COST_BPS, min(_MAX_COST_BPS, shadow))

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()

    def _recompute(self, b: _Bin) -> float | None:
        samples = list(b.buf)
        if not samples:
            return None
        spreads = [s.spread_bps for s in samples]
        slips = [s.slip_bps for s in samples]
        weights = [s.w for s in samples]

        spread_p50 = _weighted_median(spreads, weights)
        slip_p50 = _weighted_median(slips, weights)

        cost = 2.0 * spread_p50 + 2.0 * self.fee_bps + slip_p50
        return max(_MIN_COST_BPS, min(_MAX_COST_BPS, cost))
