from __future__ import annotations

"""
Confidence threshold calibrator — adaptive min_conf derived from
reliability_calibrator empirical hit-rate curves stored in Redis.

Method (roadmap project_autocalibrators_roadmap_2026_05_17.md, P0 item 4):
  - reliability_calibrator writes per-cluster bucket stats to Redis:
      key = {prefix}:{outcome}:{kind}:{symbol}:{venue}:{session}:{tf}:{regime}
      HASH fields: b{bucket}:n (count), b{bucket}:h (hits) per conf_pct bucket
  - read-side: for each cluster key, compute cumulative hit-rate ABOVE each
    descending threshold bucket; find the smallest T where:
        cum_hit_rate(T) = Σ h_{b≥T} / Σ n_{b≥T} ≥ target_wr
        AND n_above(T) ≥ min_samples_above
  - committed threshold = T clipped to [conf_floor, conf_ceil],
    with hysteresis, jump-limit, and hold-throttle
  - shadow_mode (enforce=False): propose thresholds + expose shadow_min_conf_for(),
    but min_conf_for() returns default_min_conf until enforce=True
  - hierarchical fallback when a fine-grained cluster is cold
  - Redis reads cached per cluster key for cache_ttl_sec (default 60s)

Wiring contract:
  - feed (write-side): services.reliability_calibrator.update_reliability_curves()
    already writes on every trade close — no changes needed
  - read (wire): ConfidenceThresholdFilter.evaluate() calls min_conf_for()
  - shadow→enforce: flip enforce=True after proof-streak (≥3 nights ECE↓ + WR≥target)
  - persistence: snapshot() → Redis HSET; load_state() on restart
"""

import time
from dataclasses import dataclass, field
from typing import Any

_DYN_ENFORCE_CACHE_TTL = 60.0  # re-read dynamic_cfg at most once per minute

# ── constants ──────────────────────────────────────────────────────────────────
DEFAULT_MIN_CONF: float = 50.0   # fallback when cluster is cold / enforce=False
CONF_FLOOR: float = 40.0         # never calibrate below this (hard rail)
CONF_CEIL: float = 90.0          # never calibrate above this (hard rail)

_NA = "na"                        # canonical "wildcard / not-available" dim value


# ── helpers ────────────────────────────────────────────────────────────────────

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _build_relcal_key(
    prefix: str,
    outcome: str,
    kind: str,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    regime: str,
) -> str:
    """Exact format from services.reliability_calibrator._build_key (all dims on)."""
    return ":".join([prefix, outcome, kind, symbol, venue, session, tf, regime])


def _parse_buckets(hash_data: dict[str, str]) -> dict[int, tuple[int, int]]:
    """
    Parse HASH fields {b{bucket}:n → count, b{bucket}:h → hits} into
    dict[bucket_start_int → (n, h)].
    """
    buckets: dict[int, tuple[int, int]] = {}
    for k, v in hash_data.items():
        if not k.startswith("b"):
            continue
        colon = k.find(":")
        if colon < 2:
            continue
        try:
            bkt = int(k[1:colon])
        except ValueError:
            continue
        suffix = k[colon + 1:]
        n, h = buckets.get(bkt, (0, 0))
        val = _safe_int(v)
        if suffix == "n":
            buckets[bkt] = (val, h)
        elif suffix == "h":
            buckets[bkt] = (n, val)
    return buckets


def _invert_curve(
    hash_data: dict[str, str],
    *,
    target_wr: float,
    min_samples_above: int,
) -> tuple[float, float, int] | None:
    """
    Invert empirical reliability curve.

    Scans buckets in DESCENDING order, accumulating (n, h) from the top.
    Returns (threshold, hit_rate, n_above) for the smallest bucket T where
    cumulative hit-rate above T >= target_wr AND n_above >= min_samples_above.
    Returns None if no such bucket found.

    The smallest valid T is chosen so the gate is as permissive as possible
    while still meeting the WR target — avoids over-filtering.
    """
    buckets = _parse_buckets(hash_data)
    if not buckets:
        return None

    sorted_bkts = sorted(buckets.keys(), reverse=True)

    best_t: float | None = None
    best_hr: float = 0.0
    best_n: int = 0

    cum_n = 0
    cum_h = 0

    for bkt in sorted_bkts:
        n, h = buckets[bkt]
        cum_n += n
        cum_h += h
        if cum_n < min_samples_above:
            continue
        hit_rate = cum_h / cum_n
        if hit_rate >= target_wr:
            # This is a valid candidate — keep scanning lower for smallest T
            best_t = float(bkt)
            best_hr = hit_rate
            best_n = cum_n

    if best_t is None:
        return None
    return best_t, best_hr, best_n


# ── per-cluster mutable state ──────────────────────────────────────────────────

@dataclass
class _BinState:
    min_conf: float = 0.0           # committed (enforced) threshold; 0 = not yet set
    shadow_min_conf: float = 0.0    # latest proposal regardless of enforce
    shadow_hit_rate: float = 0.0    # realized WR at shadow threshold
    shadow_n_above: int = 0         # samples above shadow threshold
    last_apply_sec: float = 0.0     # wall time of last committed update
    n_commits: int = 0              # total committed updates
    cache_expires_sec: float = 0.0  # when to re-read Redis for this cluster


# Cluster key: (kind, symbol, venue, session, tf, regime)
Key = tuple[str, str, str, str, str, str]


# ── calibrator ─────────────────────────────────────────────────────────────────

@dataclass
class ConfidenceThresholdCalibrator:
    """
    Adaptive per-cluster minimum confidence threshold calibrator.

    Reads reliability_calibrator Redis curves (write-side unchanged) and inverts
    them to find the smallest conf_pct threshold that achieves target_wr.

    Usage:
        cal = ConfidenceThresholdCalibrator(redis_client=sync_redis, enforce=False)
        # In gate evaluate():
        thr = cal.min_conf_for(symbol="BTCUSDT", kind="breakout", regime="trend",
                               session="us", venue="binance", tf="5m")
        # After proof period: cal.enforce = True
    """

    # ── injected deps ─────────────────────────────────────────────────────────
    redis_client: Any = field(default=None, repr=False)

    # ── calibration target ────────────────────────────────────────────────────
    target_wr: float = 0.55          # required cumulative WR above threshold
    outcome: str = "tp2"             # reliability_calibrator outcome to read
    rel_cal_prefix: str = "relcal"   # must match REL_CAL_PREFIX env
    min_samples_above: int = 50      # min signals above threshold to trust inversion

    # ── hard bounds ───────────────────────────────────────────────────────────
    conf_floor: float = CONF_FLOOR
    conf_ceil: float = CONF_CEIL
    default_min_conf: float = DEFAULT_MIN_CONF

    # ── safety / smoothing ────────────────────────────────────────────────────
    abs_thresh: float = 2.0          # hysteresis: skip commit if |Δ| < abs_thresh
    max_jump_abs: float = 5.0        # cap each committed update at |new - prev|
    hold_sec: float = 3600.0         # min interval between committed updates (1h)

    # ── Redis TTL cache ───────────────────────────────────────────────────────
    cache_ttl_sec: float = 60.0      # re-read Redis at most once per minute per key

    # ── enforce flag ──────────────────────────────────────────────────────────
    enforce: bool = False            # False → shadow only; True → serve calibrated

    # ── dynamic config (Redis override for enforce) ───────────────────────────
    dyn_cfg_key: str = "settings:dynamic_cfg"  # hget(dyn_cfg_key, "conf_cal_enforce")

    # ── internal state ────────────────────────────────────────────────────────
    bins: dict[Key, _BinState] = field(default_factory=dict)
    _dyn_enforce_cache: bool = field(default=False, init=False, repr=False)
    _dyn_enforce_cache_expires: float = field(default=0.0, init=False, repr=False)

    # =========================================================================
    # Public API
    # =========================================================================

    def min_conf_for(
        self,
        *,
        symbol: str,
        kind: str = _NA,
        venue: str = _NA,
        session: str = _NA,
        tf: str = _NA,
        regime: str = _NA,
    ) -> float:
        """
        Committed threshold with hierarchical fallback.
        Returns default_min_conf when enforce=False (shadow-only mode).
        Dynamic enforce can also be set via settings:dynamic_cfg → conf_cal_enforce=1.
        """
        now = time.monotonic()
        if now >= self._dyn_enforce_cache_expires and self.redis_client is not None:
            try:
                v = self.redis_client.hget(self.dyn_cfg_key, "conf_cal_enforce")
                self._dyn_enforce_cache = v in ("1", "true", "yes")
            except Exception:
                pass
            self._dyn_enforce_cache_expires = now + _DYN_ENFORCE_CACHE_TTL

        if not (self.enforce or self._dyn_enforce_cache):
            return self.default_min_conf

        sym = (symbol or "*").upper()
        knd = (kind or _NA).lower()
        ven = (venue or _NA).lower()
        ses = (session or _NA).lower()
        tf_ = (tf or _NA).lower()
        reg = (regime or _NA).lower()

        for k in self._fallback_chain(knd, sym, ven, ses, tf_, reg):
            b = self._get_or_refresh(k)
            if b is not None and b.min_conf > 0.0:
                return b.min_conf

        return self.default_min_conf

    def shadow_min_conf_for(
        self,
        *,
        symbol: str,
        kind: str = _NA,
        venue: str = _NA,
        session: str = _NA,
        tf: str = _NA,
        regime: str = _NA,
    ) -> float:
        """Latest proposed threshold for telemetry (ignores enforce flag)."""
        sym = (symbol or "*").upper()
        knd = (kind or _NA).lower()
        ven = (venue or _NA).lower()
        ses = (session or _NA).lower()
        tf_ = (tf or _NA).lower()
        reg = (regime or _NA).lower()

        for k in self._fallback_chain(knd, sym, ven, ses, tf_, reg):
            b = self._get_or_refresh(k)
            if b is not None and b.shadow_min_conf > 0.0:
                return b.shadow_min_conf

        return 0.0

    def snapshot(self) -> dict[str, Any]:
        """Compact dict for Redis persistence / counterfactual reports."""
        rows: list[dict[str, Any]] = []
        for (knd, sym, ven, ses, tf_, reg), b in self.bins.items():
            rows.append({
                "kind": knd, "symbol": sym, "venue": ven,
                "session": ses, "tf": tf_, "regime": reg,
                "min_conf": b.min_conf,
                "shadow_min_conf": b.shadow_min_conf,
                "shadow_hit_rate": b.shadow_hit_rate,
                "shadow_n_above": b.shadow_n_above,
                "last_apply_sec": b.last_apply_sec,
                "n_commits": b.n_commits,
            })
        return {
            "enforce": self.enforce,
            "target_wr": self.target_wr,
            "outcome": self.outcome,
            "default_min_conf": self.default_min_conf,
            "bins": rows,
        }

    def load_state(self, state: dict[str, Any]) -> None:
        """Restore committed/shadow thresholds from snapshot (buffers not persisted)."""
        self.enforce = bool(state.get("enforce", self.enforce))
        try:
            self.target_wr = float(state.get("target_wr", self.target_wr))
        except (TypeError, ValueError):
            pass
        try:
            self.default_min_conf = float(state.get("default_min_conf", self.default_min_conf))
        except (TypeError, ValueError):
            pass

        for row in state.get("bins", []) or []:
            try:
                k: Key = (
                    str(row.get("kind", _NA)).lower(),
                    str(row.get("symbol", "*")).upper(),
                    str(row.get("venue", _NA)).lower(),
                    str(row.get("session", _NA)).lower(),
                    str(row.get("tf", _NA)).lower(),
                    str(row.get("regime", _NA)).lower(),
                )
                b = self.bins.get(k) or _BinState()
                b.min_conf = float(row.get("min_conf", 0.0) or 0.0)
                b.shadow_min_conf = float(row.get("shadow_min_conf", 0.0) or 0.0)
                b.shadow_hit_rate = float(row.get("shadow_hit_rate", 0.0) or 0.0)
                b.shadow_n_above = int(row.get("shadow_n_above", 0) or 0)
                b.last_apply_sec = float(row.get("last_apply_sec", 0.0) or 0.0)
                b.n_commits = int(row.get("n_commits", 0) or 0)
                self.bins[k] = b
            except (AttributeError, KeyError, TypeError, ValueError):
                continue

    # =========================================================================
    # Internals
    # =========================================================================

    def _fallback_chain(
        self,
        kind: str, symbol: str, venue: str, session: str, tf: str, regime: str,
    ) -> list[Key]:
        """
        Ordered fallback: most specific → most general.
        Mirrors reliability_calibrator dimension structure.
        """
        return [
            (kind, symbol, venue, session, tf, regime),  # finest
            (kind, symbol, venue, session, tf, _NA),      # drop regime
            (kind, symbol, venue, session, _NA, _NA),     # drop regime+tf
            (kind, symbol, _NA, _NA, _NA, _NA),           # symbol+kind only
            (_NA, symbol, _NA, _NA, _NA, _NA),            # symbol only
            (_NA, "*", _NA, _NA, _NA, _NA),               # global wildcard
        ]

    def _redis_key_for(self, cluster: Key) -> str:
        kind, symbol, venue, session, tf, regime = cluster
        return _build_relcal_key(
            self.rel_cal_prefix, self.outcome,
            kind, symbol, venue, session, tf, regime,
        )

    def _get_or_refresh(self, cluster: Key) -> _BinState | None:
        """Return cached bin state, refreshing from Redis if TTL expired."""
        b = self.bins.get(cluster)
        now = time.monotonic()

        if b is not None and now < b.cache_expires_sec:
            return b  # cache hit — avoid Redis read

        if self.redis_client is None:
            return b  # no Redis; return stale (or None)

        redis_key = self._redis_key_for(cluster)
        try:
            hash_data: dict[str, str] = self.redis_client.hgetall(redis_key) or {}
        except Exception:
            return b  # fail-open: keep stale state

        if b is None:
            b = _BinState()
            self.bins[cluster] = b
        b.cache_expires_sec = now + self.cache_ttl_sec

        if not hash_data:
            return b  # key not yet written; retain prior state

        result = _invert_curve(
            hash_data,
            target_wr=self.target_wr,
            min_samples_above=self.min_samples_above,
        )
        if result is None:
            return b  # not enough data to invert

        raw_threshold, hit_rate, n_above = result

        # Update shadow (always — for telemetry)
        proposed = max(self.conf_floor, min(self.conf_ceil, raw_threshold))
        b.shadow_min_conf = proposed
        b.shadow_hit_rate = hit_rate
        b.shadow_n_above = n_above

        # Maybe commit with safety guards
        self._maybe_apply(b, proposed, now)
        return b

    def _maybe_apply(self, b: _BinState, proposed: float, now: float) -> None:
        """Commit proposed threshold subject to hysteresis, jump-limit, hold."""
        # Hold throttle — never commit faster than hold_sec if already committed
        if b.min_conf > 0.0 and (now - b.last_apply_sec) < self.hold_sec:
            return

        # Hysteresis — skip micro-updates
        if b.min_conf > 0.0 and abs(proposed - b.min_conf) < self.abs_thresh:
            return

        # Jump-limit — cap each step
        new_val = proposed
        if b.min_conf > 0.0:
            delta = proposed - b.min_conf
            if delta > self.max_jump_abs:
                new_val = b.min_conf + self.max_jump_abs
            elif delta < -self.max_jump_abs:
                new_val = b.min_conf - self.max_jump_abs
            new_val = max(self.conf_floor, min(self.conf_ceil, new_val))

        b.min_conf = new_val
        b.last_apply_sec = now
        b.n_commits += 1
