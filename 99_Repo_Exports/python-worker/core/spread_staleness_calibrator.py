from __future__ import annotations

"""
SpreadStalenessCalibrator — adaptive per-(symbol × session) budgets for
spread_bps (market width) and book_age_ms (data freshness).

Replaces hardcoded globals in entry_policy_gate.py:
  spread_shock_bps       35 bps → calibrated q90 of observed spread_bps
  spread_shock_bps_hard  60 bps → calibrated q95 of observed spread_bps
  book_stale_soft_ms    600 ms  → calibrated q90 of detected staleness
  book_stale_hard_ms   1200 ms  → calibrated q95 of detected staleness

Method:
  - Streaming P² quantile estimator (O(1) memory, no sample buffer required).
  - Regime key: "{symbol_lower}:{session}" — matches project convention.
  - Warmup: min_samples per regime before calibrated values are used; until
    then (and when enforce=False), static defaults are returned (fail-open).
  - Spread observation: always fed when spread_bps ∈ [SPREAD_BPS_FLOOR,
    SPREAD_BPS_CEIL]; zero / NaN / ∞ are silently dropped.
  - Book-age observation: only fed when book_age_ms > 0 (i.e. actual staleness
    was detected by BookTradeConsistencyGate). This gives a conditional
    distribution "how bad is detected staleness?" rather than mixing in the
    common case of 0 ms (fresh book).
  - Shadow mode (enforce=False, default): observe + expose shadow_thresholds()
    for audit/telemetry, but thresholds() returns static defaults.
  - Enforce mode (enforce=True): thresholds() returns calibrated values once
    regime is warm.

Design invariants (aligned with BookRateCalibrator):
  - No IO: caller owns Redis; pure computation.
  - Stateless wrt wall clock: no time.time() calls.
  - Deterministic: same observe() sequence → same thresholds().
  - Hard rails: calibrated thresholds are always clamped to
    [SPREAD_BPS_FLOOR, SPREAD_BPS_CEIL] / [BOOK_AGE_MS_FLOOR, BOOK_AGE_MS_CEIL].
  - Monotonicity: hard threshold always ≥ soft threshold (within each metric).
"""

import json
import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ─────────────────────────────────────────────────────────────────
SPREAD_BPS_FLOOR: float = 1.0       # below: missing / zero spread
SPREAD_BPS_CEIL: float = 500.0      # above: exchange outage / bad tick

BOOK_AGE_MS_FLOOR: float = 10.0    # below: clock skew / rounding noise
BOOK_AGE_MS_CEIL: float = 30_000.0  # above: connectivity gap

# Static defaults — kept in sync with entry_policy_gate.py __init__ defaults.
DEFAULT_SPREAD_SHOCK_BPS: float = 35.0
DEFAULT_SPREAD_SHOCK_BPS_HARD: float = 60.0
DEFAULT_BOOK_STALE_SOFT_MS: float = 600.0
DEFAULT_BOOK_STALE_HARD_MS: float = 1200.0


@dataclass
class SpreadStalenessThresholds:
    """
    Calibrated (or static) gate thresholds for one (symbol × session) regime.

    spread_shock_bps      — tighten boundary (q90 of observed spread_bps)
    spread_shock_bps_hard — veto boundary   (q95 of observed spread_bps)
    book_stale_soft_ms    — soft staleness  (q90 of detected book_age_ms > 0)
    book_stale_hard_ms    — hard staleness  (q95 of detected book_age_ms > 0)
    n                     — total observations counted for this regime
    src                   — "static" if cold/shadow; "calib_q90q95" if enforced
    """
    spread_shock_bps: float
    spread_shock_bps_hard: float
    book_stale_soft_ms: float
    book_stale_hard_ms: float
    n: int
    src: str


class SpreadStalenessCalibrator:
    """
    Online calibrator for spread and book-staleness gate budgets.

    Usage:
        calib = SpreadStalenessCalibrator(min_samples=200, enforce=False)

        # once per signal (inside EntryPolicyGate.evaluate):
        calib.observe(regime="btcusdt:ny", spread_bps=3.2, book_age_ms=0.0)
        th = calib.thresholds(regime="btcusdt:ny")
        # use th.spread_shock_bps, th.spread_shock_bps_hard, ...

        # audit (always available, regardless of enforce):
        shadow = calib.shadow_thresholds(regime="btcusdt:ny")
    """

    def __init__(
        self,
        *,
        min_samples: int = 200,
        enforce: bool = False,
        max_spread_bps: float = SPREAD_BPS_CEIL,
        max_book_age_ms: float = BOOK_AGE_MS_CEIL,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.max_spread_bps = max_spread_bps
        self.max_book_age_ms = max_book_age_ms

        # per-regime P² estimators for spread_bps
        self._sp90: dict[str, P2Quantile] = {}
        self._sp95: dict[str, P2Quantile] = {}

        # per-regime P² estimators for book_age_ms (conditional: age > 0)
        self._ba90: dict[str, P2Quantile] = {}
        self._ba95: dict[str, P2Quantile] = {}

        # total observation count per regime
        self._n: dict[str, int] = {}

        # shadow proposals (always computed, even in shadow mode)
        self._shadow: dict[str, SpreadStalenessThresholds] = {}

    # ── public API ─────────────────────────────────────────────────────────────

    def observe(self, *, regime: str, spread_bps: float, book_age_ms: float) -> None:
        """
        Feed one observation.

        spread_bps   — current bid-ask spread in bps (_spread_bps_from_ctx);
                       fed when finite and in [SPREAD_BPS_FLOOR, max_spread_bps].
        book_age_ms  — detected staleness ms (book_trade_consistency_stale_book_ms);
                       fed only when > BOOK_AGE_MS_FLOOR so we build a conditional
                       distribution "how bad is detected staleness?" and avoid
                       poisoning the estimators with the common fresh-book zeros.
        """
        r = (regime or "na").lower()
        counted = False

        sp = spread_bps if math.isfinite(spread_bps) else 0.0
        if SPREAD_BPS_FLOOR <= sp <= self.max_spread_bps:
            self._get(self._sp90, r, 0.90).update(sp)
            self._get(self._sp95, r, 0.95).update(sp)
            counted = True

        ba = book_age_ms if math.isfinite(book_age_ms) else 0.0
        if BOOK_AGE_MS_FLOOR <= ba <= self.max_book_age_ms:
            self._get(self._ba90, r, 0.90).update(ba)
            self._get(self._ba95, r, 0.95).update(ba)
            counted = True

        if counted:
            self._n[r] = self._n.get(r, 0) + 1

    def thresholds(
        self,
        *,
        regime: str,
        default_spread_shock_bps: float = DEFAULT_SPREAD_SHOCK_BPS,
        default_spread_shock_bps_hard: float = DEFAULT_SPREAD_SHOCK_BPS_HARD,
        default_book_stale_soft_ms: float = DEFAULT_BOOK_STALE_SOFT_MS,
        default_book_stale_hard_ms: float = DEFAULT_BOOK_STALE_HARD_MS,
    ) -> SpreadStalenessThresholds:
        """
        Return thresholds for this regime.

        enforce=False or cold regime → static defaults (fail-open).
        Shadow proposal is always updated and accessible via shadow_thresholds().
        """
        r = (regime or "na").lower()
        n = self._n.get(r, 0)

        shadow = self._compute(
            r, n,
            default_spread_shock_bps, default_spread_shock_bps_hard,
            default_book_stale_soft_ms, default_book_stale_hard_ms,
        )
        self._shadow[r] = shadow

        if not self.enforce or n < self.min_samples:
            return SpreadStalenessThresholds(
                spread_shock_bps=default_spread_shock_bps,
                spread_shock_bps_hard=default_spread_shock_bps_hard,
                book_stale_soft_ms=default_book_stale_soft_ms,
                book_stale_hard_ms=default_book_stale_hard_ms,
                n=n,
                src="static",
            )

        return shadow

    def shadow_thresholds(self, *, regime: str) -> SpreadStalenessThresholds | None:
        """Last computed shadow (proposal), regardless of enforce mode."""
        return self._shadow.get((regime or "na").lower())

    # ── persistence ─────────────────────────────────────────────────────────────

    def dump_regime_state(
        self, *, symbol: str, regime: str, updated_ts_ms: int
    ) -> dict[str, Any]:
        r = (regime or "na").lower()
        return {
            "v": 1,
            "kind": "spread_staleness",
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "enforce": self.enforce,
            "n": self._n.get(r, 0),
            "sp90": (self._sp90[r].to_state() if r in self._sp90 else None),
            "sp95": (self._sp95[r].to_state() if r in self._sp95 else None),
            "ba90": (self._ba90[r].to_state() if r in self._ba90 else None),
            "ba95": (self._ba95[r].to_state() if r in self._ba95 else None),
        }

    def load_regime_state(self, state: Any) -> None:
        """Restore per-regime state from dump_regime_state(). Fail-open."""
        try:
            if not isinstance(state, dict):
                return
            r = str(state.get("regime") or "na").lower()
            self.min_samples = int(
                state.get("min_samples", self.min_samples) or self.min_samples
            )
            self._n[r] = int(state.get("n", 0) or 0)
            for attr, key in [
                ("_sp90", "sp90"),
                ("_sp95", "sp95"),
                ("_ba90", "ba90"),
                ("_ba95", "ba95"),
            ]:
                raw = state.get(key)
                if isinstance(raw, dict):
                    getattr(self, attr)[r] = P2Quantile.from_state(raw)
        except Exception:
            return

    @staticmethod
    def loads(raw: str) -> dict[str, Any] | None:
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else None
        except Exception:
            return None

    # ── internals ───────────────────────────────────────────────────────────────

    def _get(self, m: dict[str, P2Quantile], regime: str, p: float) -> P2Quantile:
        q = m.get(regime)
        if q is None:
            q = P2Quantile(p=p)
            m[regime] = q
        return q

    def _p2_val(self, m: dict[str, P2Quantile], r: str, default: float) -> float:
        """Read P² estimator value; fall back to default on missing / non-finite."""
        q = m.get(r)
        v = q.value() if q is not None else None
        if v is None or not math.isfinite(v) or v <= 0:
            return default
        return v

    def _compute(
        self,
        r: str,
        n: int,
        default_sp_soft: float,
        default_sp_hard: float,
        default_ba_soft: float,
        default_ba_hard: float,
    ) -> SpreadStalenessThresholds:
        """Propose thresholds from P² estimators (no enforce / warmup check)."""
        if n < self.min_samples:
            return SpreadStalenessThresholds(
                spread_shock_bps=default_sp_soft,
                spread_shock_bps_hard=default_sp_hard,
                book_stale_soft_ms=default_ba_soft,
                book_stale_hard_ms=default_ba_hard,
                n=n,
                src="static",
            )

        sp90 = self._p2_val(self._sp90, r, default_sp_soft)
        sp95 = self._p2_val(self._sp95, r, default_sp_hard)
        ba90 = self._p2_val(self._ba90, r, default_ba_soft)
        ba95 = self._p2_val(self._ba95, r, default_ba_hard)

        # clamp to rails; enforce hard ≥ soft
        sp_soft = max(SPREAD_BPS_FLOOR, min(SPREAD_BPS_CEIL, sp90))
        sp_hard = max(sp_soft, min(SPREAD_BPS_CEIL, sp95))
        ba_soft = max(BOOK_AGE_MS_FLOOR, min(BOOK_AGE_MS_CEIL, ba90))
        ba_hard = max(ba_soft, min(BOOK_AGE_MS_CEIL, ba95))

        return SpreadStalenessThresholds(
            spread_shock_bps=sp_soft,
            spread_shock_bps_hard=sp_hard,
            book_stale_soft_ms=ba_soft,
            book_stale_hard_ms=ba_hard,
            n=n,
            src="calib_q90q95",
        )
