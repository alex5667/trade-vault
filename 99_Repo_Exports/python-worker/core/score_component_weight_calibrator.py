from __future__ import annotations

"""
Walk-forward IR-based calibrator for scoring component weights.

Algorithm (Lopez de Prado, "Advances in Financial ML"):
  w_i(t) = IR_i(t-W..t) / Σ IR_j(t-W..t)
  IR = IC × sqrt(BR)
  IC = Pearson(component_score_i, sign(r_multiple)) over rolling window W
  BR = n_samples / window_days

Components (maps to ScoreModelCfg fields):
  regime        → s_mode / regime class quality
  geometry      → s_z (momentum z-score)
  liquidity     → s_obi20 / s_obi (order book imbalance)
  l3            → s_l3 / s_l3_pressure (L3 quality)
  micro_quality → s_microprice / s_micro_block (microprice / micro quality)

Per key (symbol, regime):
  - Rolling ring buffer of (component_scores, outcome_sign, ts_ms)
  - IC computed from samples; BR from sample rate
  - w_i ∈ [W_FLOOR, W_CAP], renormalized to sum=1
  - Fallback to DEFAULT_WEIGHTS when < min_samples

State persistence: snapshot() → JSON-serializable dict; load_state(data) to restore.
"""

import math
from collections import deque
from dataclasses import dataclass
from typing import Any

COMPONENTS: list[str] = ["regime", "geometry", "liquidity", "l3", "micro_quality"]

DEFAULT_WEIGHTS: dict[str, float] = {
    "regime": 0.25,
    "geometry": 0.25,
    "liquidity": 0.25,
    "l3": 0.15,
    "micro_quality": 0.10,
}

W_FLOOR: float = 0.05
W_CAP: float = 0.50
MIN_SAMPLES: int = 50
WINDOW_DAYS: int = 30
MAX_BUFFER: int = 2000
BE_BAND_R: float = 0.05  # |r| < threshold → exclude from IC

Key = tuple[str, str]  # (symbol, regime)

# Parts keys tried in order for each component
COMPONENT_PART_KEYS: dict[str, list[str]] = {
    "regime": ["s_mode"],
    "geometry": ["s_z", "s_z_breakout", "s_z_extreme"],
    "liquidity": ["s_obi20", "s_obi", "s_support"],
    "l3": ["s_l3", "s_l3_pressure"],
    "micro_quality": ["s_microprice", "s_micro_block", "s_mp"],
}


@dataclass
class _Sample:
    component_scores: dict[str, float]
    outcome_sign: int  # +1 win, -1 loss, 0 excluded (BE)
    ts_ms: int


def _pearson(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation, returns 0.0 on degenerate inputs."""
    n = len(xs)
    if n < 10:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
    sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
    if sx < 1e-9 or sy < 1e-9:
        return 0.0
    return max(-1.0, min(1.0, cov / (sx * sy)))


def _compute_ir(samples: list[_Sample], window_days: int) -> dict[str, float]:
    """IC × √BR per component."""
    active = [s for s in samples if s.outcome_sign != 0]
    if len(active) < 10:
        return {c: 0.0 for c in COMPONENTS}

    ys = [float(s.outcome_sign) for s in active]
    br = len(active) / max(1, window_days)

    result: dict[str, float] = {}
    for comp in COMPONENTS:
        xs = [s.component_scores.get(comp, 0.0) for s in active]
        non_zero = sum(1 for x in xs if abs(x) > 1e-6)
        if non_zero < 5:
            result[comp] = 0.0
            continue
        ic = _pearson(xs, ys)
        result[comp] = ic * math.sqrt(br)
    return result


def _normalize_weights(ir: dict[str, float]) -> dict[str, float] | None:
    """IR → normalized weights in [W_FLOOR, W_CAP] summing to 1.

    Two-phase constrained projection:
      Phase 1 — fix components capped at W_CAP or floored at W_FLOOR.
                Distribute remaining budget proportionally among "free" ones.
                Repeat until no free component violates a bound.
      Phase 2 — if budget remains after all components hit bounds, lift
                the floored group proportionally (they can accept more).

    Returns None when all IR ≤ 0.
    """
    pos = {c: max(0.0, ir.get(c, 0.0)) for c in COMPONENTS}
    total = sum(pos.values())
    if total < 1e-9:
        return None

    # Initial proportional share
    w = {c: v / total for c, v in pos.items()}

    # Phase 1: iteratively fix violators and redistribute to free components
    for _ in range(20):
        capped = {c for c in COMPONENTS if w[c] > W_CAP + 1e-12}
        floored = {c for c in COMPONENTS if w[c] < W_FLOOR - 1e-12}
        if not capped and not floored:
            break

        for c in capped:
            w[c] = W_CAP
        for c in floored:
            w[c] = W_FLOOR

        fixed = capped | floored
        free = [c for c in COMPONENTS if c not in fixed]
        fixed_sum = sum(w[c] for c in fixed)
        budget = 1.0 - fixed_sum

        if not free:
            # Phase 2: no free components — lift the floored group
            # (floored components have headroom up to W_CAP)
            liftable = [c for c in COMPONENTS if c in floored]
            if not liftable:
                break
            lift_each = budget / len(liftable)
            for c in liftable:
                w[c] = min(W_CAP, W_FLOOR + lift_each)
            break

        free_pos_total = sum(pos[c] for c in free)
        if free_pos_total < 1e-9:
            each = budget / len(free)
            for c in free:
                w[c] = each
        else:
            for c in free:
                w[c] = pos[c] / free_pos_total * budget

    # Guarantee sum=1 after floating-point drift
    t = sum(w.values())
    return {c: w[c] / t for c in COMPONENTS}


class ScoreComponentWeightCalibrator:
    """
    Walk-forward IR calibrator for (symbol × regime) scoring component weights.

    Thread-safety: NOT thread-safe — wrap in a lock if shared across threads.
    """

    def __init__(
        self,
        window_days: int = WINDOW_DAYS,
        min_samples: int = MIN_SAMPLES,
        max_buffer: int = MAX_BUFFER,
        be_band_r: float = BE_BAND_R,
    ) -> None:
        self.window_days = window_days
        self.min_samples = min_samples
        self.max_buffer = max_buffer
        self.be_band_r = be_band_r

        self._buffers: dict[Key, deque[_Sample]] = {}
        self._committed: dict[Key, dict[str, float]] = {}
        self._shadow: dict[Key, dict[str, float]] = {}
        self._ir_last: dict[Key, dict[str, float]] = {}

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def observe(
        self,
        symbol: str,
        regime: str,
        component_scores: dict[str, float],
        outcome_r: float,
        ts_ms: int,
    ) -> None:
        """Record one closed trade sample."""
        if abs(outcome_r) < self.be_band_r:
            outcome_sign = 0
        else:
            outcome_sign = 1 if outcome_r > 0 else -1

        key: Key = (symbol, regime)
        if key not in self._buffers:
            self._buffers[key] = deque(maxlen=self.max_buffer)

        self._buffers[key].append(
            _Sample(
                component_scores={c: component_scores.get(c, 0.0) for c in COMPONENTS},
                outcome_sign=outcome_sign,
                ts_ms=ts_ms,
            )
        )
        self._prune(key, ts_ms)

    def _prune(self, key: Key, now_ms: int) -> None:
        buf = self._buffers.get(key)
        if not buf:
            return
        cutoff = now_ms - self.window_days * 86_400_000
        while buf and buf[0].ts_ms < cutoff:
            buf.popleft()

    # ------------------------------------------------------------------
    # Compute
    # ------------------------------------------------------------------

    def compute_weights(self, symbol: str, regime: str) -> dict[str, float]:
        """
        Return calibrated weights for (symbol, regime).
        Falls back to committed → DEFAULT_WEIGHTS when insufficient data.
        """
        key: Key = (symbol, regime)
        samples = list(self._buffers.get(key, []))
        if len(samples) < self.min_samples:
            return dict(self._committed.get(key, DEFAULT_WEIGHTS))

        ir = _compute_ir(samples, self.window_days)
        self._ir_last[key] = ir
        w = _normalize_weights(ir)
        if w is None:
            return dict(self._committed.get(key, DEFAULT_WEIGHTS))

        self._shadow[key] = w
        return dict(self._committed.get(key, w))

    def promote_shadow(self, symbol: str, regime: str) -> bool:
        """Commit the pending shadow weights for this key. Returns True if promoted."""
        key: Key = (symbol, regime)
        if key in self._shadow:
            self._committed[key] = dict(self._shadow[key])
            return True
        return False

    def promote_all(self) -> list[str]:
        """Commit all pending shadow weights. Returns list of promoted keys."""
        promoted = []
        for key in list(self._shadow):
            self._committed[key] = dict(self._shadow[key])
            promoted.append(f"{key[0]}:{key[1]}")
        return promoted

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        committed_out: dict[str, Any] = {}
        for (sym, reg), w in self._committed.items():
            committed_out[f"{sym}:{reg}"] = w

        shadow_out: dict[str, Any] = {}
        for (sym, reg), w in self._shadow.items():
            shadow_out[f"{sym}:{reg}"] = w

        ir_out: dict[str, Any] = {}
        for (sym, reg), ir in self._ir_last.items():
            ir_out[f"{sym}:{reg}"] = ir

        buf_sizes: dict[str, int] = {
            f"{k[0]}:{k[1]}": len(v)
            for k, v in self._buffers.items()
        }

        return {
            "schema_version": 1,
            "committed": committed_out,
            "shadow": shadow_out,
            "ir_last": ir_out,
            "buf_sizes": buf_sizes,
        }

    def load_state(self, data: dict[str, Any]) -> None:
        """Restore committed weights from a previous snapshot (buffers not restored)."""
        if data.get("schema_version", 0) != 1:
            return
        for composite_key, w in data.get("committed", {}).items():
            parts = composite_key.split(":", 1)
            if len(parts) == 2:
                key: Key = (parts[0], parts[1])
                self._committed[key] = {c: float(w.get(c, DEFAULT_WEIGHTS[c])) for c in COMPONENTS}

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def sample_counts(self) -> dict[str, int]:
        return {f"{k[0]}:{k[1]}": len(v) for k, v in self._buffers.items()}

    def ir_last(self, symbol: str, regime: str) -> dict[str, float] | None:
        return self._ir_last.get((symbol, regime))


def extract_component_scores(parts: dict[str, Any]) -> dict[str, float]:
    """
    Map ConfidenceScorer parts → 5 calibrator components.

    For regime: if s_mode present use it; else infer from regime_class_raw.
    For others: first matching non-zero key wins; 0.0 fallback.
    """
    result: dict[str, float] = {}

    for comp, keys in COMPONENT_PART_KEYS.items():
        val = 0.0
        for k in keys:
            v = parts.get(k)
            if v is not None:
                try:
                    fv = float(v)
                    if math.isfinite(fv):
                        val = max(0.0, min(1.0, fv))
                        break
                except (TypeError, ValueError):
                    continue
        result[comp] = val

    # Regime fallback: infer from regime_class_raw if s_mode absent
    if result["regime"] == 0.0:
        rc = str(parts.get("regime_class_raw", parts.get("regime", ""))).lower()
        if "trend" in rc:
            result["regime"] = 1.0
        elif "mixed" in rc:
            result["regime"] = 0.75
        elif "range" in rc or "chop" in rc:
            result["regime"] = 0.55

    return result
