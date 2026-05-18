from __future__ import annotations

"""
p_edge threshold calibrator — adaptive EV-aware cutoff for `EDGE_EV_P_MIN`
used in handlers/crypto_orderflow/utils/edge_cost_gate.py (lines 1093/1164/1990).

Replaces hardcoded global `ev_p_min = 0.55` with per-(symbol × regime) threshold
derived from realized (p_edge, r_multiple, win) on `trades:closed`.

Method (per roadmap project_autocalibrators_roadmap_2026_05_17.md, P0 item 2):
  - rolling 7d ring buffer per (symbol × regime × kind) of decision-time samples;
  - grid search τ ∈ tau_grid (default 0.40…0.80 step 0.02);
  - for each τ compute realized mean R for trades with p_edge ≥ τ;
  - pick smallest τ where mean_R ≥ target_ev AND n_kept ≥ min_kept_trades;
  - conformal floor: τ_cf = q_(1-α) of p_edge over LOSS-only trades — distribution-
    free upper bound on the threshold required to exclude losers at confidence α;
  - committed τ = max(grid_result, τ_cf, default_floor) clipped into [tau_grid[0],
    tau_grid[-1]];
  - hysteresis (abs_thresh, default 0.02), absolute jump-limit (max_jump_abs,
    default 0.03), hold_ms throttle between applies;
  - hierarchical fallback when finer key has not yet warmed up;
  - shadow_mode: record samples + expose `shadow_p_min()`, but `p_min_for()`
    returns `default_p_min` until promoted to enforce.

Stateless wrt wall clock — `now_ms` is supplied by the caller. Deterministic
over the sequence of (sample, now_ms) pairs.

Wiring contract (separate phase — not implemented here):
  - feed: `ml_outcome_calibration_tracker_v1` (or a sibling service) consumes
    `trades:closed`, parses {ml_prob, r_multiple, result, symbol, regime, kind,
    ts_close}, calls `observe()`;
  - read: `edge_cost_gate._p_min_for_kind()` reads `p_min_for(symbol, regime,
    kind)` from a shared instance, falling back to ENV `EDGE_EV_P_MIN`;
  - persistence: periodic `snapshot()` to Redis HSET, `load_state()` on
    service restart — buffers NOT persisted (only committed/shadow τ).
"""

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

# Default cutoff matches historic hardcoded EDGE_EV_P_MIN.
DEFAULT_P_MIN: float = 0.55

# Hard bounds for any committed threshold (sanity rails).
TAU_FLOOR: float = 0.40
TAU_CEIL: float = 0.80


def _default_tau_grid() -> list[float]:
    """Grid τ ∈ [0.40, 0.80] step 0.02 — 21 points."""
    return [round(TAU_FLOOR + 0.02 * i, 4) for i in range(0, 21)]


def _quantile(xs: list[float], q: float) -> float:
    """Linear-interpolated quantile on a sorted copy (no numpy)."""
    if not xs:
        return 0.0
    a = sorted(xs)
    if len(a) == 1:
        return a[0]
    q = min(0.999, max(0.0, q))
    i = q * (len(a) - 1)
    lo = math.floor(i)
    hi = math.ceil(i)
    if lo == hi:
        return a[lo]
    w = i - lo
    return a[lo] * (1.0 - w) + a[hi] * w


@dataclass
class _Sample:
    """Single observed closed trade: (p_edge, r_multiple, win, ts_ms)."""
    p: float
    r: float
    win: int  # 1 for WIN, 0 for LOSS, -1 for BE (excluded from EV math)
    ts_ms: int


# (symbol, regime, kind) — '*' is the aggregated wildcard
Key = tuple[str, str, str]


@dataclass
class _Bin:
    """Per-key rolling buffer + last computed thresholds."""
    buf: deque[_Sample] = field(default_factory=deque)
    p_min: float = 0.0          # committed (enforced) cutoff
    shadow_p_min: float = 0.0   # latest proposal regardless of enforce
    shadow_ev_at_pin: float = 0.0  # realized mean R at shadow τ (for reports)
    shadow_n_kept: int = 0      # samples retained at shadow τ
    last_recompute_ms: int = 0
    last_apply_ms: int = 0
    n_observed: int = 0


@dataclass
class PEdgeThresholdCalibrator:
    """
    Adaptive per-(symbol × regime × kind) cutoff for ML edge probability,
    targeting a realized expected R-multiple.

    Usage:
        c = PEdgeThresholdCalibrator(target_ev_r=0.10)
        # consumer of trades:closed calls observe() per closed trade:
        c.observe(symbol="BTCUSDT", regime="trend", kind="breakout",
                  p_edge=0.62, r_multiple=1.4, result="WIN", ts_ms=ts_close)
        # gate (during evaluate()) queries the calibrator:
        p_min = c.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout")

    Fallback hierarchy (when a finer bin is cold or below `min_kept_trades`):
        1. (symbol, regime, kind)
        2. (symbol, regime, "*")
        3. (symbol, "*",    "*")
        4. ("*",    regime, "*")
        5. ("*",    "*",    "*")
        6. `default_p_min`
    """

    # ----- target & grid -------------------------------------------------
    target_ev_r: float = 0.10        # require ≥ 0.10 R realized at chosen τ
    tau_grid: list[float] = field(default_factory=_default_tau_grid)
    default_p_min: float = DEFAULT_P_MIN

    # ----- conformal floor ----------------------------------------------
    conformal_alpha: float = 0.10    # τ_cf = q_(1-α) of LOSS-only p_edge
    conformal_min_losses: int = 30   # cf-floor disabled below this many losses

    # ----- buffering & sample policy ------------------------------------
    window_ms: int = 7 * 24 * 60 * 60 * 1000  # 7d rolling
    max_buf: int = 5000                       # absolute cap per bin
    min_kept_trades: int = 200                # min samples ABOVE τ to trust
    min_total_trades: int = 100               # min samples in bin to even try

    # ----- throttling ----------------------------------------------------
    recompute_gap_ms: int = 30_000   # min interval between grid recomputes
    hold_ms: int = 3_600_000         # min interval between applied updates (1h)

    # ----- safety --------------------------------------------------------
    abs_thresh: float = 0.02         # hysteresis: skip if |Δτ| < abs_thresh
    max_jump_abs: float = 0.03       # cap each commit at |τ_new - τ_prev|

    # ----- enforce flag (False → shadow only) ---------------------------
    enforce: bool = False

    # ----- internal state ------------------------------------------------
    bins: dict[Key, _Bin] = field(default_factory=dict)

    # ===== public API ====================================================

    def observe(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
        p_edge: float,
        r_multiple: float,
        result: str,
        ts_ms: int,
    ) -> None:
        """Append a closed-trade outcome to all relevant aggregated bins.

        `result` must be one of {"WIN","LOSS","BE"} (case-insensitive). BE is
        recorded for sample-count purposes but excluded from EV math.

        Boundary cast — any non-finite or out-of-domain input is silently
        dropped; calibrator must not crash on dirty inputs from upstream.
        """
        try:
            p = float(p_edge)
            r = float(r_multiple)
        except (TypeError, ValueError):
            return
        if not (math.isfinite(p) and math.isfinite(r)):
            return
        if not (0.0 <= p <= 1.0):
            return
        res_u = (result or "").strip().upper()
        if res_u not in ("WIN", "LOSS", "BE"):
            return
        win = 1 if res_u == "WIN" else (0 if res_u == "LOSS" else -1)

        sym = (symbol or "*").upper()
        reg = (regime or "*").lower()
        knd = (kind or "*").lower()

        sample = _Sample(p=p, r=r, win=win, ts_ms=int(ts_ms))

        keys: tuple[Key, ...] = (
            (sym, reg, knd),
            (sym, reg, "*"),
            (sym, "*", "*"),
            ("*", reg, "*"),
            ("*", "*", "*"),
        )
        for k in keys:
            b = self.bins.get(k)
            if b is None:
                b = _Bin(buf=deque(maxlen=self.max_buf))
                self.bins[k] = b
            b.buf.append(sample)
            b.n_observed += 1

        # Recompute thresholds on every level — observe is called per closed
        # trade (low frequency), so the cost of touching 5 bins is negligible
        # and the fallback hierarchy actually yields populated parents.
        for k in keys:
            self._maybe_recompute(k, now_ms=int(ts_ms))

    def p_min_for(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
    ) -> float:
        """Committed cutoff with hierarchical fallback.

        Returns `default_p_min` whenever `enforce` is False — preserves prod
        behavior during warm-up / shadow phase.
        """
        if not self.enforce:
            return self.default_p_min
        sym = (symbol or "*").upper()
        reg = (regime or "*").lower()
        knd = (kind or "*").lower()
        for k in (
            (sym, reg, knd),
            (sym, reg, "*"),
            (sym, "*", "*"),
            ("*", reg, "*"),
            ("*", "*", "*"),
        ):
            b = self.bins.get(k)
            if b is None or b.p_min <= 0.0:
                continue
            return b.p_min
        return self.default_p_min

    def shadow_p_min(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
    ) -> float:
        """Latest proposed cutoff regardless of enforce flag (for counterfactual
        reports). Returns 0.0 when no proposal exists yet."""
        sym = (symbol or "*").upper()
        reg = (regime or "*").lower()
        knd = (kind or "*").lower()
        for k in (
            (sym, reg, knd),
            (sym, reg, "*"),
            (sym, "*", "*"),
            ("*", reg, "*"),
            ("*", "*", "*"),
        ):
            b = self.bins.get(k)
            if b is None:
                continue
            if b.shadow_p_min > 0.0:
                return b.shadow_p_min
        return 0.0

    def snapshot(self) -> dict[str, Any]:
        """Compact dict for Redis publish / counterfactual reports.

        Schema:
            {
              "enforce": bool,
              "target_ev_r": float,
              "default_p_min": float,
              "bins": [
                {"symbol","regime","kind","n",
                 "p_min","shadow_p_min","shadow_ev_at_pin","shadow_n_kept",
                 "last_apply_ms","last_recompute_ms"},
                ...
              ]
            }
        """
        rows: list[dict[str, Any]] = []
        for (sym, reg, knd), b in self.bins.items():
            rows.append({
                "symbol": sym,
                "regime": reg,
                "kind": knd,
                "n": len(b.buf),
                "p_min": b.p_min,
                "shadow_p_min": b.shadow_p_min,
                "shadow_ev_at_pin": b.shadow_ev_at_pin,
                "shadow_n_kept": b.shadow_n_kept,
                "last_apply_ms": b.last_apply_ms,
                "last_recompute_ms": b.last_recompute_ms,
            })
        return {
            "enforce": self.enforce,
            "target_ev_r": self.target_ev_r,
            "default_p_min": self.default_p_min,
            "bins": rows,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        """Restore committed/shadow thresholds from snapshot.

        Buffers are NOT restored — only committed/shadow τ — to avoid round-
        tripping large sample arrays. Calibration resumes once new samples
        accumulate; `p_min_for()` keeps serving previously calibrated values
        via the fallback hierarchy.

        Boundary method — tolerant to malformed rows.
        """
        self.enforce = bool(state.get("enforce", self.enforce))
        try:
            self.target_ev_r = float(state.get("target_ev_r", self.target_ev_r))
        except (TypeError, ValueError):
            pass
        try:
            self.default_p_min = float(state.get("default_p_min", self.default_p_min))
        except (TypeError, ValueError):
            pass

        for row in state.get("bins", []) or []:
            try:
                k: Key = (
                    str(row["symbol"]).upper(),
                    str(row.get("regime", "*")).lower(),
                    str(row.get("kind", "*")).lower(),
                )
                b = self.bins.get(k) or _Bin(buf=deque(maxlen=self.max_buf))
                b.p_min = float(row.get("p_min", 0.0) or 0.0)
                b.shadow_p_min = float(row.get("shadow_p_min", 0.0) or 0.0)
                b.shadow_ev_at_pin = float(row.get("shadow_ev_at_pin", 0.0) or 0.0)
                b.shadow_n_kept = int(row.get("shadow_n_kept", 0) or 0)
                b.last_apply_ms = int(row.get("last_apply_ms", 0) or 0)
                b.last_recompute_ms = int(row.get("last_recompute_ms", 0) or 0)
                self.bins[k] = b
            except (KeyError, TypeError, ValueError):
                continue

    # ===== internals =====================================================

    def _prune_window(self, b: _Bin, now_ms: int) -> None:
        """Drop samples older than `window_ms`."""
        cutoff = now_ms - self.window_ms
        # deque.popleft is O(1); samples are appended in observe-order which
        # is generally near-monotonic in ts_ms. We only pop from the left.
        while b.buf and b.buf[0].ts_ms < cutoff:
            b.buf.popleft()

    def _maybe_recompute(self, key: Key, *, now_ms: int) -> None:
        b = self.bins.get(key)
        if b is None:
            return
        # First, prune stale samples (cheap when buffer is short).
        self._prune_window(b, now_ms)

        if len(b.buf) < self.min_total_trades:
            return
        # Recompute throttle — first recompute always runs.
        if b.last_recompute_ms > 0 and (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms

        # Build per-sample lists; exclude BE from EV math.
        eligible = [s for s in b.buf if s.win != -1]
        if len(eligible) < self.min_total_trades:
            return

        # ----- grid search: smallest τ where mean_R ≥ target AND n_kept ok
        chosen: float = 0.0
        chosen_ev: float = 0.0
        chosen_n: int = 0
        for tau in self.tau_grid:
            kept_r = [s.r for s in eligible if s.p >= tau]
            n_kept = len(kept_r)
            if n_kept < self.min_kept_trades:
                continue
            mean_r = sum(kept_r) / n_kept
            if mean_r >= self.target_ev_r:
                chosen = tau
                chosen_ev = mean_r
                chosen_n = n_kept
                break  # smallest τ satisfying constraint

        # ----- conformal floor on LOSS-only p_edge --------------------------
        loss_p = [s.p for s in eligible if s.win == 0]
        tau_cf = 0.0
        if len(loss_p) >= self.conformal_min_losses:
            tau_cf = _quantile(loss_p, 1.0 - self.conformal_alpha)

        # Combine: if grid found nothing, fall back to max(cf, default_floor).
        if chosen <= 0.0:
            new_tau = max(tau_cf, self.default_p_min)
        else:
            new_tau = max(chosen, tau_cf)

        # Clip into hard rails.
        new_tau = max(TAU_FLOOR, min(TAU_CEIL, new_tau))

        # Update shadow unconditionally — it's the latest proposal.
        b.shadow_p_min = new_tau
        b.shadow_ev_at_pin = chosen_ev
        b.shadow_n_kept = chosen_n

        # Apply-side throttle (skip only when we already have a committed τ).
        if b.p_min > 0.0 and (now_ms - b.last_apply_ms) < self.hold_ms:
            return

        # Hysteresis + jump-limit (only when a prior commit exists).
        if b.p_min > 0.0:
            if abs(new_tau - b.p_min) < self.abs_thresh:
                return
            delta = new_tau - b.p_min
            if delta > self.max_jump_abs:
                new_tau = b.p_min + self.max_jump_abs
            elif delta < -self.max_jump_abs:
                new_tau = b.p_min - self.max_jump_abs
            new_tau = max(TAU_FLOOR, min(TAU_CEIL, new_tau))

        b.p_min = new_tau
        b.last_apply_ms = now_ms
