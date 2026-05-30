"""
entry_slippage_cap_calibrator.py — entry-side slippage cap calibrator.

Calibrates max expected entry slippage per (symbol × session × hour-of-day) from
actual filled entry slippage in trades:closed.

Algorithm:
  For each bucket (symbol, session):
    q75(entry_slippage_bps, decayed) × 1.5  →  cap_bps
    EWMA blend with previous committed value (alpha=0.10)

Fail-open: cold → ENV RISK_TIER_*_SLIPPAGE_BPS_CAP used.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_MIN_CAP = 2.0     # minimum cap (bps)
_MAX_CAP = 50.0    # maximum cap (bps)
_Q_TARGET = 0.75   # quantile
_OVERSHOOT_MULT = 1.5
_ALPHA = 0.10      # EWMA blend
_UPDATE_BAND = 0.5


@dataclass
class _Sample:
    slip_bps: float
    ts_ms: int


@dataclass
class _Bin:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=5_000))
    committed_cap_bps: float = 12.0
    shadow_cap_bps: float = 12.0
    last_recompute_ms: int = 0
    n_observed: int = 0


class EntrySlippageCapCalibrator:
    """Per-(symbol × session) entry slippage cap calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 14.0,
        half_life_days: float = 7.0,
        alpha: float = _ALPHA,
        min_samples: int = 20,
        min_cap: float = _MIN_CAP,
        max_cap: float = _MAX_CAP,
        overshoot_mult: float = _OVERSHOOT_MULT,
        recompute_gap_ms: int = 600_000,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.half_life_ms = half_life_days * 86_400_000
        self.alpha = alpha
        self.min_samples = min_samples
        self.min_cap = min_cap
        self.max_cap = max_cap
        self.overshoot_mult = overshoot_mult
        self.recompute_gap_ms = recompute_gap_ms
        self._bins: dict[tuple[str, str], _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        session: str,
        entry_slip_bps: float,
        ts_ms: int,
    ) -> None:
        if not math.isfinite(entry_slip_bps) or entry_slip_bps < 0:
            return
        now_ms = int(time.time() * 1000)
        sample = _Sample(slip_bps=entry_slip_bps, ts_ms=ts_ms)
        for key in self._bucket_keys(symbol, session):
            b = self._get_or_create(key)
            b.buf.append(sample)
            b.n_observed += 1
        self._maybe_recompute(self._bucket_keys(symbol, session)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_cap(self, *, symbol: str, session: str) -> float | None:
        """Returns calibrated cap or None (caller uses ENV default)."""
        for key in self._fallback_keys(symbol, session):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples):
                if b.committed_cap_bps > 0:
                    return b.committed_cap_bps
        return None

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (sym, sess), b in self._bins.items():
            bins_out.append({
                "symbol": sym, "session": sess,
                "committed_cap_bps": round(b.committed_cap_bps, 2),
                "shadow_cap_bps": round(b.shadow_cap_bps, 2),
                "n": b.n_observed, "n_buf": len(b.buf),
            })
        bins_out.sort(key=lambda x: (x["symbol"], x["session"]))
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
                sess = str(row.get("session", "*"))
                b = self._get_or_create((sym, sess))
                b.committed_cap_bps = float(row.get("committed_cap_bps", 12.0))
                b.shadow_cap_bps = float(row.get("shadow_cap_bps", 12.0))
                b.n_observed = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bucket_keys(self, symbol: str, session: str) -> list[tuple[str, str]]:
        s = (symbol or "*").strip().upper()
        sess = (session or "*").strip().lower()
        return [(s, sess), (s, "*"), ("*", sess), ("*", "*")]

    def _fallback_keys(self, symbol: str, session: str) -> list[tuple[str, str]]:
        return self._bucket_keys(symbol, session)

    def _get_or_create(self, key: tuple[str, str]) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin()
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
        q75 = self._weighted_quantile(list(b.buf), _Q_TARGET, now_ms)
        shadow = max(self.min_cap, min(self.max_cap, q75 * self.overshoot_mult))
        b.shadow_cap_bps = shadow
        if abs(shadow - b.committed_cap_bps) >= _UPDATE_BAND:
            b.committed_cap_bps = b.committed_cap_bps * (1 - self.alpha) + shadow * self.alpha
            b.committed_cap_bps = max(self.min_cap, min(self.max_cap, b.committed_cap_bps))

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()

    def _weighted_quantile(self, samples: list[_Sample], q: float, now_ms: int) -> float:
        if not samples:
            return 0.0
        ln2 = math.log(2)
        weighted = [(s.slip_bps, math.exp(-ln2 * (now_ms - s.ts_ms) / self.half_life_ms))
                    for s in samples]
        weighted.sort(key=lambda x: x[0])
        total_w = sum(w for _, w in weighted)
        if total_w <= 0:
            return 0.0
        target = q * total_w
        cumulative = 0.0
        for val, w in weighted:
            cumulative += w
            if cumulative >= target:
                return val
        return weighted[-1][0]
