"""
daily_dd_per_tier_calibrator.py — per-tier × regime adaptive daily drawdown limit calibrator.

Reads closed-trade P&L streams, computes per-tier daily DD distribution,
auto-adjusts RISK_MAX_DAILY_LOSS_PCT per tier.

Algorithm:
  For each bucket (tier, regime):
    Accumulate daily_pnl_pct windows
    soft_limit = max(MIN_LIMIT, p75(daily_pnl_pct distrib)) × SAFETY_MULT
    hard_limit = max(MIN_LIMIT, p95(daily_pnl_pct distrib)) × SAFETY_MULT

  Conservative: limits never go above HARD_CAP (2.5% per tier).
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_MIN_LIMIT = 0.5          # floor: 0.5% daily DD
_HARD_CAP = 2.5           # ceiling: 2.5% daily DD per tier
_SAFETY_MULT = 0.85       # conservative multiplier
_UPDATE_BAND = 0.1        # min Δ to commit new limit
_MAX_JUMP = 0.5           # max single-step change


@dataclass
class _DailyWindow:
    """One UTC calendar-day DD aggregate."""
    date_str: str
    pnl_pct_abs: float   # absolute daily PnL pct (positive = loss)
    ts_ms: int


@dataclass
class _Bin:
    windows: deque[_DailyWindow] = field(default_factory=lambda: deque(maxlen=200))
    committed_soft_pct: float = 2.0
    committed_hard_pct: float = 3.0
    shadow_soft_pct: float = 2.0
    shadow_hard_pct: float = 3.0
    last_recompute_ms: int = 0
    n_days: int = 0


class DailyDDPerTierCalibrator:
    """Per-(tier × regime) adaptive daily drawdown limit calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: int = 30,
        min_days: int = 10,
        safety_mult: float = _SAFETY_MULT,
        recompute_gap_ms: int = 3_600_000,
        default_soft_pct: float = 2.0,
        default_hard_pct: float = 3.0,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_days = window_days
        self.min_days = min_days
        self.safety_mult = safety_mult
        self.recompute_gap_ms = recompute_gap_ms
        self.default_soft_pct = default_soft_pct
        self.default_hard_pct = default_hard_pct
        self._bins: dict[tuple[str, str], _Bin] = {}

    # ── Ingestion (call once per UTC day close per tier/regime) ───────────────

    def observe_day(
        self,
        *,
        tier: str,
        regime: str,
        date_str: str,
        pnl_pct: float,
        ts_ms: int,
    ) -> None:
        """pnl_pct: signed daily P&L as % of equity (negative = loss)."""
        if not math.isfinite(pnl_pct):
            return
        loss_pct = max(0.0, -pnl_pct)  # positive = drawdown
        now_ms = int(time.time() * 1000)
        window = _DailyWindow(date_str=date_str, pnl_pct_abs=loss_pct, ts_ms=ts_ms)
        for key in self._bucket_keys(tier, regime):
            b = self._get_or_create(key)
            # Deduplicate by date
            if b.windows and b.windows[-1].date_str == date_str:
                b.windows[-1] = window
            else:
                b.windows.append(window)
                b.n_days += 1
        self._maybe_recompute(self._bucket_keys(tier, regime)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_soft_limit(self, *, tier: str, regime: str) -> float:
        for key in self._fallback_keys(tier, regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and len(b.windows) >= self.min_days):
                if b.committed_soft_pct > _MIN_LIMIT:
                    return b.committed_soft_pct
        return self.default_soft_pct

    def get_hard_limit(self, *, tier: str, regime: str) -> float:
        for key in self._fallback_keys(tier, regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and len(b.windows) >= self.min_days):
                if b.committed_hard_pct > _MIN_LIMIT:
                    return b.committed_hard_pct
        return self.default_hard_pct

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for (tier, reg), b in self._bins.items():
            bins_out.append({
                "tier": tier, "regime": reg,
                "committed_soft_pct": round(b.committed_soft_pct, 3),
                "committed_hard_pct": round(b.committed_hard_pct, 3),
                "shadow_soft_pct": round(b.shadow_soft_pct, 3),
                "shadow_hard_pct": round(b.shadow_hard_pct, 3),
                "n_days": b.n_days,
            })
        bins_out.sort(key=lambda x: (x["tier"], x["regime"]))
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "default_soft_pct": self.default_soft_pct,
            "default_hard_pct": self.default_hard_pct,
            "bins": bins_out,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            for row in state.get("bins", []):
                tier = str(row.get("tier", "*"))
                reg = str(row.get("regime", "*"))
                b = self._get_or_create((tier, reg))
                b.committed_soft_pct = float(row.get("committed_soft_pct", self.default_soft_pct))
                b.committed_hard_pct = float(row.get("committed_hard_pct", self.default_hard_pct))
                b.shadow_soft_pct = float(row.get("shadow_soft_pct", self.default_soft_pct))
                b.shadow_hard_pct = float(row.get("shadow_hard_pct", self.default_hard_pct))
                b.n_days = int(row.get("n_days", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bucket_keys(self, tier: str, regime: str) -> list[tuple[str, str]]:
        t = (tier or "*").strip().upper()
        r = (regime or "*").strip().lower()
        return [(t, r), (t, "*"), ("*", r), ("*", "*")]

    def _fallback_keys(self, tier: str, regime: str) -> list[tuple[str, str]]:
        return self._bucket_keys(tier, regime)

    def _get_or_create(self, key: tuple[str, str]) -> _Bin:
        if key not in self._bins:
            self._bins[key] = _Bin(
                committed_soft_pct=self.default_soft_pct,
                committed_hard_pct=self.default_hard_pct,
                shadow_soft_pct=self.default_soft_pct,
                shadow_hard_pct=self.default_hard_pct,
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
        if len(b.windows) < self.min_days:
            return
        vals = sorted(w.pnl_pct_abs for w in b.windows)
        soft_raw = _quantile(vals, 0.75) * self.safety_mult
        hard_raw = _quantile(vals, 0.95) * self.safety_mult
        new_soft = max(_MIN_LIMIT, min(_HARD_CAP, soft_raw))
        new_hard = max(_MIN_LIMIT, min(_HARD_CAP, hard_raw))
        b.shadow_soft_pct = new_soft
        b.shadow_hard_pct = new_hard
        for attr, new_val in [("committed_soft_pct", new_soft), ("committed_hard_pct", new_hard)]:
            old_val = getattr(b, attr)
            if abs(new_val - old_val) >= _UPDATE_BAND:
                delta = new_val - old_val
                if abs(delta) > _MAX_JUMP:
                    delta = math.copysign(_MAX_JUMP, delta)
                setattr(b, attr, max(_MIN_LIMIT, min(_HARD_CAP, old_val + delta)))

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        cutoff = now_ms - self.window_days * 86_400_000
        while b.windows and b.windows[0].ts_ms < cutoff:
            b.windows.popleft()


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac
