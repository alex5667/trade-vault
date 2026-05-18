from __future__ import annotations

"""
AdverseCrossCalibrator — adaptive per-(symbol × session) thresholds for
`adverse_cross_bps` in EntryPolicyGate.

Replaces hardcoded globals in entry_policy_gate.py:
  adverse_cross_soft_bps  0.5 bps → calibrated q90 of observed adverse cross
  adverse_cross_hard_bps  1.5 bps → calibrated q98 of observed adverse cross

Method:
  - Streaming P² quantile estimator (O(1) memory, no sample buffer required).
  - Regime key: "{symbol_lower}:{session}" — matches project convention.
  - Conditional observation: only feed when cross_bps > CROSS_BPS_FLOOR
    (i.e. actual adverse cross was detected). This gives the conditional
    distribution "how large is an observed adverse cross?" rather than mixing
    in the common case of 0 bps (no crossing).
  - Precision-on-loss floor: optional `observe_outcome()` feed that maintains
    a ring buffer of (cross_bps, is_loss) pairs. When enough losses are
    accumulated, the hard threshold is floored at q80 of LOSS-only cross_bps
    values — ensuring the hard veto fires at or below the level where trade
    losses concentrate. Requires `outcome_min_losses` samples to activate.
  - Warmup: min_samples per regime before calibrated values are used; until
    then (and when enforce=False), static defaults are returned (fail-open).
  - Shadow mode (enforce=False, default): observe + expose shadow_thresholds()
    for audit/telemetry, but thresholds() returns static defaults.
  - Enforce mode (enforce=True): thresholds() returns calibrated values once
    regime is warm.

Design invariants (aligned with SpreadStalenessCalibrator):
  - No IO: caller owns Redis; pure computation.
  - Stateless wrt wall clock: no time.time() calls.
  - Deterministic: same observe() sequence → same thresholds().
  - Hard rails: calibrated thresholds clamped to [CROSS_BPS_FLOOR, CROSS_BPS_CEIL].
  - Monotonicity: hard threshold always ≥ soft threshold.
"""

import json
import math
from collections import deque
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ──────────────────────────────────────────────────────────────
CROSS_BPS_FLOOR: float = 0.05   # below this: rounding noise / no real cross
CROSS_BPS_CEIL: float = 50.0    # above this: exchange anomaly / stale book

# Static defaults — kept in sync with entry_policy_gate.py __init__ defaults.
DEFAULT_ADVERSE_CROSS_SOFT_BPS: float = 0.5
DEFAULT_ADVERSE_CROSS_HARD_BPS: float = 1.5

# Quantiles for soft/hard boundaries.
_Q_SOFT: float = 0.90
_Q_HARD: float = 0.98
# Loss-conditioned floor quantile (precision-on-loss).
_Q_LOSS: float = 0.80


@dataclass
class AdverseCrossThresholds:
    """
    Calibrated (or static) gate thresholds for one (symbol × session) regime.

    adverse_cross_soft_bps — tighten boundary (q90 of observed cross_bps)
    adverse_cross_hard_bps — veto boundary   (q98 of observed cross_bps,
                                              floored at q80 of LOSS-only)
    n                      — total observations counted for this regime
    n_losses               — loss outcomes observed (for precision-on-loss)
    loss_floor_active      — True if loss floor is constraining hard threshold
    src                    — "static" if cold/shadow; "calib_q90q98" otherwise
    """

    adverse_cross_soft_bps: float
    adverse_cross_hard_bps: float
    n: int
    n_losses: int
    loss_floor_active: bool
    src: str


class AdverseCrossCalibrator:
    """
    Online calibrator for adverse-cross gate budgets.

    Usage:
        calib = AdverseCrossCalibrator(min_samples=500, enforce=False)

        # once per signal (inside EntryPolicyGate.evaluate):
        calib.observe(regime="btcusdt:ny", cross_bps=0.8)
        th = calib.thresholds(regime="btcusdt:ny")
        # use th.adverse_cross_soft_bps, th.adverse_cross_hard_bps

        # optionally, once per closed trade (feed from trades:closed):
        calib.observe_outcome(regime="btcusdt:ny", cross_bps=0.8, is_loss=True)

        # audit (always available, regardless of enforce):
        shadow = calib.shadow_thresholds(regime="btcusdt:ny")
    """

    def __init__(
        self,
        *,
        min_samples: int = 500,
        enforce: bool = False,
        outcome_min_losses: int = 30,
        outcome_max_buf: int = 2000,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.outcome_min_losses = outcome_min_losses
        self.outcome_max_buf = outcome_max_buf

        # per-regime P² estimators for cross_bps (conditional on cross > FLOOR)
        self._q90: dict[str, P2Quantile] = {}
        self._q98: dict[str, P2Quantile] = {}

        # total observation count (cross_bps > FLOOR only)
        self._n: dict[str, int] = {}

        # precision-on-loss ring buffer: deque of (cross_bps, is_loss) per regime
        self._outcomes: dict[str, deque[tuple[float, bool]]] = {}

        # shadow proposals (always computed, even in shadow mode)
        self._shadow: dict[str, AdverseCrossThresholds] = {}

    # ── public API ─────────────────────────────────────────────────────────

    def observe(self, *, regime: str, cross_bps: float) -> None:
        """
        Feed one microstructure observation.

        cross_bps: book_trade_consistency_adverse_cross_bps from ctx.
                   Only fed when > CROSS_BPS_FLOOR (actual adverse cross detected).
                   0 / NaN / ∞ / values above CROSS_BPS_CEIL are silently dropped.
        """
        r = (regime or "na").lower()
        if not math.isfinite(cross_bps) or not (CROSS_BPS_FLOOR < cross_bps <= CROSS_BPS_CEIL):
            return

        self._get(self._q90, r, _Q_SOFT).update(cross_bps)
        self._get(self._q98, r, _Q_HARD).update(cross_bps)
        self._n[r] = self._n.get(r, 0) + 1

    def observe_outcome(
        self, *, regime: str, cross_bps: float, is_loss: bool
    ) -> None:
        """
        Optional: feed a closed-trade outcome to compute precision-on-loss floor.

        regime:    same key as passed to observe().
        cross_bps: adverse cross bps at decision time (or 0 if unavailable).
        is_loss:   True if the trade was a loss (pnl < 0).

        Feed this from a trades:closed consumer whenever the adverse cross was
        recorded in the decision context. Requires outcome_min_losses samples
        in the LOSS subset to activate the loss floor.
        """
        r = (regime or "na").lower()
        try:
            cb = float(cross_bps)
        except (TypeError, ValueError):
            return
        if not math.isfinite(cb) or cb < 0.0:
            return

        buf = self._outcomes.get(r)
        if buf is None:
            buf = deque(maxlen=self.outcome_max_buf)
            self._outcomes[r] = buf
        buf.append((cb, is_loss))

    def thresholds(
        self,
        *,
        regime: str,
        default_soft: float = DEFAULT_ADVERSE_CROSS_SOFT_BPS,
        default_hard: float = DEFAULT_ADVERSE_CROSS_HARD_BPS,
    ) -> AdverseCrossThresholds:
        """
        Return thresholds for this regime.

        When enforce=False or regime not yet warm → returns static defaults (fail-open).
        Shadow proposal is always updated and accessible via shadow_thresholds().
        """
        r = (regime or "na").lower()
        n = self._n.get(r, 0)
        n_losses = self._count_losses(r)

        shadow = self._compute(r, n, n_losses, default_soft, default_hard)
        self._shadow[r] = shadow

        if not self.enforce or n < self.min_samples:
            return AdverseCrossThresholds(
                adverse_cross_soft_bps=default_soft,
                adverse_cross_hard_bps=default_hard,
                n=n,
                n_losses=n_losses,
                loss_floor_active=False,
                src="static",
            )

        return shadow

    def shadow_thresholds(self, *, regime: str) -> AdverseCrossThresholds | None:
        """Last computed shadow proposal, regardless of enforce mode."""
        return self._shadow.get((regime or "na").lower())

    # ── persistence ─────────────────────────────────────────────────────────

    def dump_regime_state(
        self, *, symbol: str, regime: str, updated_ts_ms: int
    ) -> dict[str, Any]:
        r = (regime or "na").lower()
        return {
            "v": 1,
            "kind": "adverse_cross",
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "enforce": self.enforce,
            "n": self._n.get(r, 0),
            "q90": (self._q90[r].to_state() if r in self._q90 else None),
            "q98": (self._q98[r].to_state() if r in self._q98 else None),
            "outcomes": (
                [(cb, il) for cb, il in self._outcomes[r]]
                if r in self._outcomes
                else []
            ),
        }

    def load_regime_state(self, state: Any) -> None:
        try:
            if not isinstance(state, dict):
                return
            r = str(state.get("regime") or "na").lower()
            self.min_samples = int(
                state.get("min_samples", self.min_samples) or self.min_samples
            )
            self._n[r] = state.get("n", 0) or 0
            for attr, key in [("_q90", "q90"), ("_q98", "q98")]:
                raw = state.get(key)
                if isinstance(raw, dict):
                    getattr(self, attr)[r] = P2Quantile.from_state(raw)
            raw_out = state.get("outcomes")
            if isinstance(raw_out, list):
                buf: deque[tuple[float, bool]] = deque(
                    maxlen=self.outcome_max_buf
                )
                for entry in raw_out:
                    try:
                        cb, il = float(entry[0]), bool(int(entry[1]))
                        buf.append((cb, il))
                    except Exception:
                        pass
                self._outcomes[r] = buf
        except Exception:
            return

    @staticmethod
    def loads(raw: str) -> dict[str, Any] | None:
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else None
        except Exception:
            return None

    # ── internals ───────────────────────────────────────────────────────────

    def _get(self, m: dict[str, P2Quantile], regime: str, p: float) -> P2Quantile:
        q = m.get(regime)
        if q is None:
            q = P2Quantile(p=p)
            m[regime] = q
        return q

    def _count_losses(self, regime: str) -> int:
        buf = self._outcomes.get(regime)
        if buf is None:
            return 0
        return sum(1 for _, il in buf if il)

    def _loss_floor(self, regime: str) -> float | None:
        """
        Compute q80 of LOSS-only cross_bps values for this regime.
        Returns None if fewer than outcome_min_losses samples available.
        """
        buf = self._outcomes.get(regime)
        if buf is None:
            return None
        losses = sorted(cb for cb, il in buf if il and cb > 0.0)
        if len(losses) < self.outcome_min_losses:
            return None
        # linear-interpolated q80
        p = _Q_LOSS
        a = losses
        i = p * (len(a) - 1)
        lo, hi = math.floor(i), math.ceil(i)
        if lo == hi:
            return a[lo]
        w = i - lo
        return a[lo] * (1.0 - w) + a[hi] * w

    def _compute(
        self,
        r: str,
        n: int,
        n_losses: int,
        default_soft: float,
        default_hard: float,
    ) -> AdverseCrossThresholds:
        """Compute proposed thresholds (no enforce/warmup check)."""
        if n < self.min_samples:
            return AdverseCrossThresholds(
                adverse_cross_soft_bps=default_soft,
                adverse_cross_hard_bps=default_hard,
                n=n,
                n_losses=n_losses,
                loss_floor_active=False,
                src="static",
            )

        def _val(m: dict[str, P2Quantile], default: float) -> float:
            q = m.get(r)
            v = q.value() if q is not None else None
            if v is None or not math.isfinite(v) or v <= 0:
                return default
            return v

        q90_val = _val(self._q90, default_soft)
        q98_val = _val(self._q98, default_hard)

        # clamp to rails; enforce hard ≥ soft
        soft = max(CROSS_BPS_FLOOR, min(CROSS_BPS_CEIL, q90_val))
        hard = max(soft, min(CROSS_BPS_CEIL, q98_val))

        # precision-on-loss floor: hard cannot exceed q80 of LOSS-only crosses
        loss_floor = self._loss_floor(r)
        loss_floor_active = False
        if loss_floor is not None and loss_floor > CROSS_BPS_FLOOR:
            loss_floor_clamped = max(CROSS_BPS_FLOOR, min(CROSS_BPS_CEIL, loss_floor))
            if loss_floor_clamped < hard:
                hard = max(soft, loss_floor_clamped)
                loss_floor_active = True

        return AdverseCrossThresholds(
            adverse_cross_soft_bps=soft,
            adverse_cross_hard_bps=hard,
            n=n,
            n_losses=n_losses,
            loss_floor_active=loss_floor_active,
            src="calib_q90q98",
        )
