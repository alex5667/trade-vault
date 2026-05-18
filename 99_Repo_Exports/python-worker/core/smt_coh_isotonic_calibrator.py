from __future__ import annotations

"""smt_coh_isotonic_calibrator.py  —  Phase 2 SMT coherence calibrator.

Phase 1 (smt_coherence_calibrator.py) derives coh_min from the conditional
distribution of coh values alone — no outcome data required.

Phase 2 (this file) uses realized trade outcomes:
  - observe(symbol, regime, coh, outcome=0/1) updates in-memory histogram bins.
  - Separately, update_smt_coh_curves() in reliability_calibrator.py writes bins
    to Redis on every trade close for persistence across restarts.
  - thresholds() reads in-memory bins (supplemented from Redis via TTL cache),
    applies IsotonicRegression (increasing=False) to smooth per-bucket win rates,
    then finds the smallest coh_thr where:
        P(loss | coh >= coh_thr) >= target_veto_precision
        AND n_above(coh_thr) >= min_samples_above
  - Returns calibrated coh_min or static default (fail-open).

Target precision lift
---------------------
target_veto_precision (default 0.60):
  Among countertrend signals that would be vetoed (coh >= coh_min),
  60% should be losers. This is a 33% precision lift over a 0.45-baseline loss rate.

Shadow → enforce (G5-style)
----------------------------
  enforce=False (default): serve static default; expose shadow_thresholds().
  enforce=True:            serve calibrated threshold with hysteresis + hold + jump cap.
  auto_enforce:            flip to True after n_stable_streak_required consecutive
                           shadow proposals with |Δ| < hysteresis (warm + stable).

Hard rails: [COH_FLOOR=0.30, COH_CEIL=0.98]
Redis key:  smtcoh:cal:v1:{SYMBOL_UPPER}:{regime_lower}
  HASH fields: b{bucket_pct}:n  (int count),  b{bucket_pct}:h  (int hits),  last_ts_ms
  bucket_pct = int(coh * 100) rounded to nearest BUCKET_STEP_PCT (5)

Wiring
------
  Write path:
    services.reliability_calibrator.update_smt_coh_curves()
    called from services.stats_aggregator on trade close (countertrend signals only).
  Read path:
    SmtCoherenceGate.evaluate() calls phase2.thresholds(symbol, regime)
    if n_total >= MIN_SAMPLES.

Fallback chain:
  Phase 2 warm + enforce  →  isotonic threshold
  Phase 2 cold / shadow   →  Phase 1 q80 threshold (passed as default_coh_min)
  Phase 1 cold            →  static 0.65
"""

import math
import time
from dataclasses import dataclass
from typing import Any

# ── constants ─────────────────────────────────────────────────────────────────
COH_FLOOR: float = 0.30
COH_CEIL: float = 0.98
BUCKET_STEP_PCT: int = 5          # 5% bins: 30, 35, 40, ..., 95
MIN_BUCKET_N: int = 10            # ignore sparse bins in isotonic fit
DEFAULT_COH_MIN: float = 0.65    # static fallback (synced with Phase 1)

TARGET_VETO_PRECISION: float = 0.60   # P(loss | coh >= thr)
MIN_SAMPLES: int = 300                # total countertrend observations before calibrating
MIN_SAMPLES_ABOVE: int = 30           # min signals above threshold to trust the rate

HYSTERESIS: float = 0.04         # skip commit if |Δ| < this
MAX_JUMP: float = 0.10           # cap each committed step
HOLD_SEC: float = 3600.0         # 1h min between committed updates
CACHE_TTL_SEC: float = 60.0      # re-read Redis at most once per minute per key
REDIS_KEY_PREFIX: str = "smtcoh:cal:v1"
N_STABLE_STREAK_REQUIRED: int = 3   # consecutive stable proposals before auto-enforce


# ── helpers ───────────────────────────────────────────────────────────────────

def _bucket_pct(coh: float) -> int:
    """Quantize coh → integer bucket start in percent units (e.g. 0.67 → 65)."""
    c = max(COH_FLOOR, min(COH_CEIL, coh))
    raw = int(c * 100)
    return (raw // BUCKET_STEP_PCT) * BUCKET_STEP_PCT


def _bucket_mid(bkt_pct: int) -> float:
    """Bucket start int-pct → midpoint float (e.g. 65 → 0.675)."""
    return (bkt_pct + BUCKET_STEP_PCT / 2) / 100.0


def _b2s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x) if x is not None else ""


# ── public data types ─────────────────────────────────────────────────────────

@dataclass
class SmtCohIsotonicThresholds:
    """Calibrated (or static) coh_min for SmtCoherenceGate.

    Fields
    ------
    coh_min          — threshold to apply in gate
    veto_precision   — P(loss | coh >= coh_min) at chosen threshold
    n_above          — sample count above threshold (data quality)
    n_total          — total observations for this (symbol, regime)
    src              — "static" | "isotonic_calib"
    """
    coh_min: float
    veto_precision: float
    n_above: int
    n_total: int
    src: str


# ── internal per-cluster state ────────────────────────────────────────────────

@dataclass
class _ClusterState:
    committed_coh_min: float = 0.0
    shadow_coh_min: float = 0.0
    prev_shadow_coh_min: float = 0.0   # previous shadow for streak comparison
    shadow_veto_precision: float = 0.0
    shadow_n_above: int = 0
    last_commit_sec: float = 0.0
    n_commits: int = 0
    n_stable_streak: int = 0
    cache_expires_sec: float = 0.0


# ── main calibrator ───────────────────────────────────────────────────────────

class SmtCohIsotonicCalibrator:
    """Phase 2 SMT coherence calibrator (isotonic regression on outcomes).

    Usage
    -----
        cal = SmtCohIsotonicCalibrator(redis_client=sync_redis)

        # In gate evaluate() — for online in-process accumulation (optional):
        if countertrend and outcome_available:
            cal.observe(symbol=sym, regime=reg, coh=coh, outcome=win)

        # In gate evaluate() — always:
        th = cal.thresholds(symbol=sym, regime=reg, default_coh_min=phase1_thr)
        if mode == "veto" and countertrend and leader_confirm and coh >= th.coh_min:
            return DENY
    """

    def __init__(
        self,
        *,
        redis_client: Any = None,
        enforce: bool = False,
        auto_enforce: bool = True,
        n_stable_streak_required: int = N_STABLE_STREAK_REQUIRED,
        target_veto_precision: float = TARGET_VETO_PRECISION,
        min_samples: int = MIN_SAMPLES,
        min_samples_above: int = MIN_SAMPLES_ABOVE,
        hysteresis: float = HYSTERESIS,
        max_jump: float = MAX_JUMP,
        hold_sec: float = HOLD_SEC,
        cache_ttl_sec: float = CACHE_TTL_SEC,
        key_prefix: str = REDIS_KEY_PREFIX,
    ) -> None:
        self.redis = redis_client
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.n_stable_streak_required = n_stable_streak_required
        self.target_veto_precision = target_veto_precision
        self.min_samples = min_samples
        self.min_samples_above = min_samples_above
        self.hysteresis = hysteresis
        self.max_jump = max_jump
        self.hold_sec = hold_sec
        self.cache_ttl_sec = cache_ttl_sec
        self.key_prefix = key_prefix

        # In-memory histogram: {(SYMBOL, regime) → {bucket_pct: (n, h)}}
        self._bins: dict[tuple[str, str], dict[int, tuple[int, int]]] = {}
        # Per-cluster committed + shadow state
        self._state: dict[tuple[str, str], _ClusterState] = {}

    # ── public API ────────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        symbol: str,
        regime: str,
        coh: float,
        outcome: int,
    ) -> None:
        """Accumulate a countertrend signal outcome in-process.

        Call ONLY for countertrend signals (leader_confirm=1, direction ≠ leader_dir).
        outcome: 1 = signal succeeded (TP hit / pnl>0), 0 = failed.
        NaN/Inf/out-of-range coh and invalid outcomes are silently ignored.
        """
        if not math.isfinite(coh):
            return
        if not (COH_FLOOR <= coh <= COH_CEIL):
            return
        if outcome not in (0, 1):
            return
        key = (symbol.upper(), regime.lower())
        bkt = _bucket_pct(coh)
        bins = self._bins.setdefault(key, {})
        n, h = bins.get(bkt, (0, 0))
        bins[bkt] = (n + 1, h + outcome)

    def thresholds(
        self,
        *,
        symbol: str,
        regime: str,
        default_coh_min: float = DEFAULT_COH_MIN,
    ) -> SmtCohIsotonicThresholds:
        """Return calibrated (or static) coh_min for gate.

        Shadow mode (enforce=False):
          Returns default_coh_min; shadow_thresholds() reflects the proposal.
        Enforce mode:
          Returns calibrated threshold with hysteresis + jump cap + hold.
        """
        key = (symbol.upper(), regime.lower())
        self._maybe_refresh_from_redis(key)

        bins = self._bins.get(key, {})
        n_total = sum(n for n, _ in bins.values())

        shadow = _compute_threshold(
            bins,
            target_veto_precision=self.target_veto_precision,
            min_samples=self.min_samples,
            min_samples_above=self.min_samples_above,
            default_coh_min=default_coh_min,
        )

        st = self._state.setdefault(key, _ClusterState())
        prev_shadow = st.shadow_coh_min  # capture before overwrite
        st.prev_shadow_coh_min = prev_shadow
        st.shadow_coh_min = shadow.coh_min
        st.shadow_veto_precision = shadow.veto_precision
        st.shadow_n_above = shadow.n_above

        warm = n_total >= self.min_samples

        # Auto-enforce: flip when warm and consecutive shadow proposals are stable.
        # Stability is shadow-to-shadow (not shadow-vs-committed) so the streak
        # builds even when the calibrated value differs from the static default.
        if self.auto_enforce and warm and not self.enforce and shadow.src == "isotonic_calib":
            if prev_shadow > 0.0 and abs(shadow.coh_min - prev_shadow) < self.hysteresis:
                st.n_stable_streak += 1
            else:
                st.n_stable_streak = max(0, st.n_stable_streak - 1)  # decay on change
            if st.n_stable_streak >= self.n_stable_streak_required:
                self.enforce = True

        if not (self.enforce and warm) or shadow.src != "isotonic_calib":
            return SmtCohIsotonicThresholds(
                coh_min=default_coh_min,
                veto_precision=0.0,
                n_above=shadow.n_above,
                n_total=n_total,
                src="static",
            )

        now = time.monotonic()
        proposed = shadow.coh_min
        prev = st.committed_coh_min if st.committed_coh_min > 0.0 else default_coh_min

        # Hold throttle
        if st.committed_coh_min > 0.0 and (now - st.last_commit_sec) < self.hold_sec:
            return SmtCohIsotonicThresholds(
                coh_min=prev,
                veto_precision=shadow.veto_precision,
                n_above=shadow.n_above,
                n_total=n_total,
                src="isotonic_calib",
            )

        # Hysteresis
        if abs(proposed - prev) < self.hysteresis:
            return SmtCohIsotonicThresholds(
                coh_min=prev,
                veto_precision=shadow.veto_precision,
                n_above=shadow.n_above,
                n_total=n_total,
                src="isotonic_calib",
            )

        # Jump cap
        delta = proposed - prev
        if abs(delta) > self.max_jump:
            proposed = prev + math.copysign(self.max_jump, delta)
        proposed = max(COH_FLOOR, min(COH_CEIL, proposed))

        st.committed_coh_min = proposed
        st.last_commit_sec = now
        st.n_commits += 1

        return SmtCohIsotonicThresholds(
            coh_min=proposed,
            veto_precision=shadow.veto_precision,
            n_above=shadow.n_above,
            n_total=n_total,
            src="isotonic_calib",
        )

    def shadow_thresholds(
        self, *, symbol: str, regime: str
    ) -> SmtCohIsotonicThresholds | None:
        """Latest proposed threshold for telemetry (ignores enforce flag)."""
        key = (symbol.upper(), regime.lower())
        st = self._state.get(key)
        if st is None or st.shadow_coh_min == 0.0:
            return None
        bins = self._bins.get(key, {})
        n_total = sum(n for n, _ in bins.values())
        return SmtCohIsotonicThresholds(
            coh_min=st.shadow_coh_min,
            veto_precision=st.shadow_veto_precision,
            n_above=st.shadow_n_above,
            n_total=n_total,
            src="isotonic_calib",
        )

    def n_total(self, *, symbol: str, regime: str) -> int:
        key = (symbol.upper(), regime.lower())
        return sum(n for n, _ in self._bins.get(key, {}).values())

    # ── persistence ───────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for (sym, reg), st in self._state.items():
            bins = self._bins.get((sym, reg), {})
            rows.append({
                "symbol": sym, "regime": reg,
                "committed_coh_min": st.committed_coh_min,
                "shadow_coh_min": st.shadow_coh_min,
                "shadow_veto_precision": st.shadow_veto_precision,
                "shadow_n_above": st.shadow_n_above,
                "n_total": sum(n for n, _ in bins.values()),
                "n_commits": st.n_commits,
            })
        return {
            "v": 1, "kind": "smt_coh_isotonic",
            "enforce": self.enforce,
            "target_veto_precision": self.target_veto_precision,
            "rows": rows,
        }

    def load_state(self, state: Any) -> None:
        """Restore committed thresholds. Fail-open (never raises)."""
        try:
            if not isinstance(state, dict):
                return
            if state.get("kind") != "smt_coh_isotonic":
                return
            self.enforce = bool(state.get("enforce", self.enforce))
            for row in state.get("rows", []) or []:
                sym = str(row.get("symbol") or "").upper()
                reg = str(row.get("regime") or "").lower()
                if not sym or not reg:
                    continue
                key = (sym, reg)
                st = self._state.get(key) or _ClusterState()
                st.committed_coh_min = float(row.get("committed_coh_min") or 0.0)
                st.shadow_coh_min = float(row.get("shadow_coh_min") or 0.0)
                st.shadow_veto_precision = float(row.get("shadow_veto_precision") or 0.0)
                st.shadow_n_above = int(row.get("shadow_n_above") or 0)
                st.n_commits = int(row.get("n_commits") or 0)
                self._state[key] = st
        except Exception:
            pass

    # ── Redis refresh ─────────────────────────────────────────────────────────

    def _maybe_refresh_from_redis(self, key: tuple[str, str]) -> None:
        """Refresh in-memory bins from Redis if TTL expired."""
        st = self._state.get(key)
        now = time.monotonic()
        if st is not None and now < st.cache_expires_sec:
            return
        if self.redis is None:
            return

        symbol, regime = key
        redis_key = f"{self.key_prefix}:{symbol}:{regime}"
        try:
            raw = self.redis.hgetall(redis_key) or {}
        except Exception:
            return

        if not isinstance(raw, dict) or not raw:
            if st is None:
                st = _ClusterState()
                self._state[key] = st
            st.cache_expires_sec = now + self.cache_ttl_sec
            return

        d = {_b2s(k): _b2s(v) for k, v in raw.items()}
        bins: dict[int, tuple[int, int]] = {}
        for fname, vstr in d.items():
            if not fname.startswith("b"):
                continue
            colon = fname.find(":")
            if colon < 2:
                continue
            try:
                bkt = int(fname[1:colon])
            except ValueError:
                continue
            suffix = fname[colon + 1:]
            n, h = bins.get(bkt, (0, 0))
            try:
                iv = int(vstr)
            except (ValueError, TypeError):
                continue
            if suffix == "n":
                bins[bkt] = (iv, h)
            elif suffix == "h":
                bins[bkt] = (n, iv)

        # Merge Redis bins with in-memory (take max to handle concurrent writes)
        existing = self._bins.get(key, {})
        for bkt, (rn, rh) in bins.items():
            en, eh = existing.get(bkt, (0, 0))
            existing[bkt] = (max(rn, en), max(rh, eh))
        self._bins[key] = existing

        if st is None:
            st = _ClusterState()
            self._state[key] = st
        st.cache_expires_sec = now + self.cache_ttl_sec


# ── core algorithm ────────────────────────────────────────────────────────────

def _compute_threshold(
    bins: dict[int, tuple[int, int]],
    *,
    target_veto_precision: float,
    min_samples: int,
    min_samples_above: int,
    default_coh_min: float,
) -> SmtCohIsotonicThresholds:
    """Compute veto threshold from histogram bins.

    1. Guard: not enough total data → return static default.
    2. Optional: isotonic-smooth per-bucket win rates.
    3. Scan ascending: find smallest coh_thr where
         P(loss | coh >= coh_thr) >= target_veto_precision
         AND n_above >= min_samples_above.
    4. Apply hard rails [COH_FLOOR, COH_CEIL].
    """
    n_total = sum(n for n, _ in bins.values())
    if n_total < min_samples:
        return SmtCohIsotonicThresholds(
            coh_min=default_coh_min,
            veto_precision=0.0,
            n_above=0,
            n_total=n_total,
            src="static",
        )

    try:
        smoothed = _isotonic_smooth(bins)
    except Exception:
        smoothed = bins
    result = _find_veto_threshold(
        smoothed,
        target_veto_precision=target_veto_precision,
        min_samples_above=min_samples_above,
    )

    if result is None:
        return SmtCohIsotonicThresholds(
            coh_min=default_coh_min,
            veto_precision=0.0,
            n_above=0,
            n_total=n_total,
            src="static",
        )

    raw_thr, veto_prec, n_above = result
    coh_min = max(COH_FLOOR, min(COH_CEIL, raw_thr))
    return SmtCohIsotonicThresholds(
        coh_min=coh_min,
        veto_precision=veto_prec,
        n_above=n_above,
        n_total=n_total,
        src="isotonic_calib",
    )


def _isotonic_smooth(
    bins: dict[int, tuple[int, int]],
) -> dict[int, tuple[int, int]]:
    """Apply IsotonicRegression (increasing=False) to per-bucket empirical rates.

    Enforces the expected monotone-decreasing relationship:
      higher coh → lower countertrend win rate.

    Falls back to original bins if sklearn unavailable, too few eligible bins,
    or any error occurs.
    """
    eligible = [
        (bkt, n, h)
        for bkt, (n, h) in sorted(bins.items())
        if n >= MIN_BUCKET_N
    ]
    if len(eligible) < 3:
        return bins

    try:
        from sklearn.isotonic import IsotonicRegression  # type: ignore[import]

        xs = [_bucket_mid(bkt) for bkt, _, _ in eligible]
        ys = [h / n for _, n, h in eligible]
        ws = [float(n) for _, n, _ in eligible]

        ir = IsotonicRegression(increasing=False, out_of_bounds="clip")
        y_iso = ir.fit_transform(xs, ys, sample_weight=ws)

        new_bins = dict(bins)
        for i, (bkt, n, _) in enumerate(eligible):
            smoothed_h = max(0, min(n, round(float(y_iso[i]) * n)))
            new_bins[bkt] = (n, smoothed_h)
        return new_bins
    except Exception:
        return bins


def _find_veto_threshold(
    bins: dict[int, tuple[int, int]],
    *,
    target_veto_precision: float,
    min_samples_above: int,
) -> tuple[float, float, int] | None:
    """Find smallest coh_thr where P(loss | coh >= coh_thr) >= target_veto_precision.

    Scans buckets in ascending order.  At each bucket as threshold, computes:
      cum_n = Σ n[bkt'] for bkt' >= bkt   (total above threshold)
      cum_h = Σ h[bkt'] for bkt' >= bkt   (wins above threshold)
      veto_prec = 1 - cum_h / cum_n        (fraction that are losses)

    Returns (coh_thr_float, veto_prec, n_above) for the SMALLEST valid threshold,
    or None if no threshold meets criteria.
    """
    if not bins:
        return None

    sorted_bkts = sorted(bins.keys())
    # Pre-compute cumulative from above for each starting bucket
    cum_n = sum(n for n, _ in bins.values())
    cum_h = sum(h for _, h in bins.values())

    for bkt in sorted_bkts:
        n_bkt, h_bkt = bins.get(bkt, (0, 0))

        if cum_n >= min_samples_above and cum_n > 0:
            p_success_above = cum_h / cum_n
            veto_prec = 1.0 - p_success_above
            if veto_prec >= target_veto_precision:
                return bkt / 100.0, veto_prec, cum_n

        cum_n -= n_bkt
        cum_h -= h_bkt

    return None
