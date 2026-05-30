"""
tp_size_fraction_calibrator.py — per-regime TP size fraction calibrator.

Calibrates TP1/TP2/TP3 position close fractions per regime.
Uses MFE path probabilities: fraction_i ∝ P(reach_TPi | regime)

Algorithm:
  From trade outcomes, compute:
    f1 = P(close_at_TP1 | all_closed_at_any_TP)
    f2 = P(close_at_TP2 | all_closed_at_any_TP)
    f3 = P(close_at_TP3 | all_closed_at_any_TP)
  Normalize to sum = 1.0 with floor = MIN_FRAC per leg.

Output: autocal:tp_size_fractions:state
Master switch: TP_SIZE_FRAC_CAL_ENFORCE=0
"""
from __future__ import annotations

import math
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1
_DEFAULT_FRACTIONS = (0.334, 0.333, 0.333)  # equal thirds by default
_MIN_FRAC = 0.10   # minimum per leg
_UPDATE_BAND = 0.03  # min absolute Δ to commit any leg


@dataclass
class _Bin:
    tp1_count: int = 0
    tp2_count: int = 0
    tp3_count: int = 0
    total: int = 0
    committed_f1: float = _DEFAULT_FRACTIONS[0]
    committed_f2: float = _DEFAULT_FRACTIONS[1]
    committed_f3: float = _DEFAULT_FRACTIONS[2]
    shadow_f1: float = _DEFAULT_FRACTIONS[0]
    shadow_f2: float = _DEFAULT_FRACTIONS[1]
    shadow_f3: float = _DEFAULT_FRACTIONS[2]
    last_recompute_ms: int = 0


class TPSizeFractionCalibrator:
    """Per-regime TP size fraction calibrator."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        min_samples: int = 50,
        recompute_gap_ms: int = 3_600_000,
        window_days: float = 14.0,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.min_samples = min_samples
        self.recompute_gap_ms = recompute_gap_ms
        self.window_ms = int(window_days * 86_400_000)
        self._bins: dict[str, _Bin] = {}

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe_tp(self, *, regime: str, tp_level: int, ts_ms: int) -> None:
        """Observe a TP hit (tp_level = 1, 2, or 3)."""
        if tp_level not in (1, 2, 3):
            return
        now_ms = int(time.time() * 1000)
        for key in self._regime_keys(regime):
            b = self._get_or_create(key)
            if tp_level == 1:
                b.tp1_count += 1
            elif tp_level == 2:
                b.tp2_count += 1
            elif tp_level == 3:
                b.tp3_count += 1
            b.total += 1
        self._maybe_recompute(self._regime_keys(regime)[0], now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_fractions(self, *, regime: str) -> tuple[float, float, float]:
        """Return (f1, f2, f3) summing to ~1.0."""
        for key in self._fallback_keys(regime):
            b = self._bins.get(key)
            if b is None:
                continue
            if self.enforce or (self.auto_enforce and b.total >= self.min_samples):
                return b.committed_f1, b.committed_f2, b.committed_f3
        return _DEFAULT_FRACTIONS

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        bins_out = []
        for reg, b in self._bins.items():
            bins_out.append({
                "regime": reg,
                "committed_f1": round(b.committed_f1, 4),
                "committed_f2": round(b.committed_f2, 4),
                "committed_f3": round(b.committed_f3, 4),
                "shadow_f1": round(b.shadow_f1, 4),
                "shadow_f2": round(b.shadow_f2, 4),
                "shadow_f3": round(b.shadow_f3, 4),
                "n": b.total,
            })
        bins_out.sort(key=lambda x: x["regime"])
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
                reg = str(row.get("regime", "*"))
                b = self._get_or_create(reg)
                b.committed_f1 = float(row.get("committed_f1", _DEFAULT_FRACTIONS[0]))
                b.committed_f2 = float(row.get("committed_f2", _DEFAULT_FRACTIONS[1]))
                b.committed_f3 = float(row.get("committed_f3", _DEFAULT_FRACTIONS[2]))
                b.shadow_f1 = float(row.get("shadow_f1", _DEFAULT_FRACTIONS[0]))
                b.total = int(row.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _regime_keys(self, regime: str) -> list[str]:
        r = (regime or "*").lower().strip()
        return [r, "*"]

    def _fallback_keys(self, regime: str) -> list[str]:
        return self._regime_keys(regime)

    def _get_or_create(self, regime: str) -> _Bin:
        if regime not in self._bins:
            self._bins[regime] = _Bin()
        return self._bins[regime]

    def _maybe_recompute(self, primary_key: str, now_ms: int) -> None:
        b = self._bins.get(primary_key)
        if b is None:
            return
        if (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms
        total_tp = b.tp1_count + b.tp2_count + b.tp3_count
        if total_tp < self.min_samples:
            return
        f1_raw = b.tp1_count / total_tp
        f2_raw = b.tp2_count / total_tp
        f3_raw = b.tp3_count / total_tp
        # Apply floor and renormalize
        f1 = max(_MIN_FRAC, f1_raw)
        f2 = max(_MIN_FRAC, f2_raw)
        f3 = max(_MIN_FRAC, f3_raw)
        s = f1 + f2 + f3
        f1, f2, f3 = f1 / s, f2 / s, f3 / s
        b.shadow_f1, b.shadow_f2, b.shadow_f3 = f1, f2, f3
        if (abs(f1 - b.committed_f1) >= _UPDATE_BAND or
                abs(f2 - b.committed_f2) >= _UPDATE_BAND):
            b.committed_f1 = f1
            b.committed_f2 = f2
            b.committed_f3 = f3
