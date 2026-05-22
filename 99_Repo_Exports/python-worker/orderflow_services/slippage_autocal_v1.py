#!/usr/bin/env python3
"""
slippage_autocal_v1.py — streaming autocalibrtor for CostEdgeGate.slippage_bps.

Replaces the 6-hour batch job (cost_edge_slippage_calibrator_v1.py) with a
continuous XREAD loop: reads trades:closed, maintains rolling deque per
(symbol × session) with exponential decay weights, and publishes q75 snapshots
to Redis every SNAP_SEC.

Output: same Redis key as the batch calibrator → SlippageCalReader works unchanged.
  enforce-mode: slippage_bps_cal:v1
  shadow-mode:  slippage_bps_cal:v1:shadow

Algorithm (same as batch rolling_q75_v1):
  source: adverse_bps from close payload (adverse_bps_t[2000] or slippage_bps_est)
  grouping: (symbol, session) — session from decision or time-of-day
  weight: exp(-ln2 × age_days / HALF_LIFE_DAYS)  [half-life 7d]
  q75(weighted) → EWMA blend with prev committed → clamp [LOWER, UPPER]

ENV:
  SLIP_AC_REDIS_URL           default REDIS_URL or redis://redis-worker-1:6379/0
  SLIP_AC_GROUP               default slip-ac
  SLIP_AC_CONSUMER            default slip-ac-1
  SLIP_AC_PORT                default 9157
  SLIP_AC_BATCH               default 100
  SLIP_AC_WINDOW_DAYS         default 30
  SLIP_AC_HALF_LIFE_DAYS      default 7
  SLIP_AC_ALPHA               default 0.095   (EWMA blend weight for snapshots)
  SLIP_AC_LOWER               default 1.0     bps floor
  SLIP_AC_UPPER               default 30.0    bps cap
  SLIP_AC_MIN_N               default 20      min samples per group
  SLIP_AC_SNAP_SEC            default 60      publish interval
  SLIP_AC_ENFORCE             default 0       0=shadow key, 1=production key
  SLIP_AC_ADVERSE_KEY_MS      default 2000    which bucket to read from adverse_bps_t dict

Auto-promote (shadow → enforce without restart):
  SLIP_AC_PROMOTE_ENABLED     default 0     enable auto-promote logic
  SLIP_AC_PROMOTE_MIN_OBS     default 500   total observations required
  SLIP_AC_PROMOTE_MIN_GROUPS  default 5     min real (non-wildcard) groups
  SLIP_AC_PROMOTE_DWELL_SEC   default 3600  shadow dwell time before promote (1 hour)
  SLIP_AC_PROMOTE_MAX_DRIFT   default 30    max allowed % drift vs existing enforce key
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, Histogram, start_http_server  # type: ignore

from core.redis_keys import RS

logger = logging.getLogger("slip-ac")

LN2 = math.log(2.0)

CAL_KEY_ENFORCE = "slippage_bps_cal:v1"
CAL_KEY_SHADOW = "slippage_bps_cal:v1:shadow"

# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    return _env(k, "1" if d else "0").strip().lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

_trades_observed = Counter(
    "slip_ac_trades_observed_total",
    "Trades observed by slippage autocal",
    ["symbol", "session"],
)
_trades_skipped = Counter(
    "slip_ac_trades_skipped_total",
    "Trades skipped (missing slippage field)",
    ["reason"],
)
_snap_writes = Counter("slip_ac_snapshot_writes_total", "Redis snapshot writes")
_snap_errors = Counter("slip_ac_snapshot_errors_total", "Redis snapshot errors")
_buf_size = Gauge(
    "slip_ac_buffer_size",
    "Rolling buffer sample count",
    ["symbol", "session"],
)
_group_bps = Gauge(
    "slip_ac_calibrated_bps",
    "Last committed slippage_bps per group",
    ["symbol", "session"],
)
_lag_hist = Histogram(
    "slip_ac_event_lag_ms",
    "Lag from close_ts_ms to observe",
    buckets=[100, 500, 2000, 10000, 60000],
)
_enforce_mode = Gauge("slip_ac_enforce_mode", "1 = writing to enforce key, 0 = shadow")
_promote_obs = Gauge("slip_ac_promote_observations_total", "Total observations seen (for promote gate)")
_promote_ready = Gauge("slip_ac_promote_ready", "1 = promote criteria met, waiting for dwell")
_promoted = Gauge("slip_ac_promoted", "1 = auto-promoted to enforce this run")


# ---------------------------------------------------------------------------
# Core rolling calibrator
# ---------------------------------------------------------------------------


@dataclass
class _Sample:
    adverse_bps: float
    ts_ms: int
    half_life_days: float

    def weight(self, now_ms: int) -> float:
        age_days = max(0.0, (now_ms - self.ts_ms)) / 86_400_000.0
        return math.exp(-LN2 * age_days / self.half_life_days)


def _weighted_quantile(samples: list[_Sample], q: float, now_ms: int) -> float:
    """Weighted quantile — no numpy. Returns 0.0 on empty input."""
    pairs = [(s.adverse_bps, s.weight(now_ms)) for s in samples if s.adverse_bps > 0]
    if not pairs:
        return 0.0
    pairs.sort(key=lambda x: x[0])
    vals = [p[0] for p in pairs]
    wts = [p[1] for p in pairs]
    total_w = sum(wts)
    if total_w <= 0:
        return 0.0
    cum: list[float] = []
    c = 0.0
    for w in wts:
        c += w / total_w
        cum.append(c)
    q = max(0.0, min(1.0, q))
    if q <= cum[0]:
        return float(vals[0])
    if q >= cum[-1]:
        return float(vals[-1])
    for i in range(1, len(cum)):
        if cum[i] >= q:
            lo, hi = cum[i - 1], cum[i]
            if abs(hi - lo) < 1e-12:
                return float(vals[i])
            frac = (q - lo) / (hi - lo)
            return float(vals[i - 1] * (1.0 - frac) + vals[i] * frac)
    return float(vals[-1])


Key = tuple[str, str]  # (symbol, session)


class SlippageAutoCal:
    """Rolling q75 calibrator — pure in-memory, no DB, no async."""

    def __init__(
        self,
        window_days: int = 30,
        half_life_days: float = 7.0,
        alpha: float = 0.095,
        lower: float = 1.0,
        upper: float = 30.0,
        min_n: int = 20,
        default_bps: float = 4.0,
    ) -> None:
        self.window_days = window_days
        self.half_life_days = half_life_days
        self.alpha = alpha
        self.lower = lower
        self.upper = upper
        self.min_n = min_n
        self.default_bps = default_bps

        self._buffers: dict[Key, deque[_Sample]] = {}
        self._committed: dict[str, float] = {}  # "SYM:SESS" → calibrated bps

    def observe(self, symbol: str, session: str, adverse_bps: float, ts_ms: int) -> None:
        """Record one closed trade."""
        if not math.isfinite(adverse_bps) or adverse_bps <= 0:
            return
        key: Key = (symbol, session)
        if key not in self._buffers:
            self._buffers[key] = deque()
        self._buffers[key].append(_Sample(adverse_bps, ts_ms, self.half_life_days))
        # wildcard fallback aggregates
        for agg_key in ((symbol, "*"), ("*", "*")):
            if agg_key not in self._buffers:
                self._buffers[agg_key] = deque()
            self._buffers[agg_key].append(_Sample(adverse_bps, ts_ms, self.half_life_days))
        self._prune(ts_ms)

    def _prune(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_days * 86_400_000
        for buf in self._buffers.values():
            while buf and buf[0].ts_ms < cutoff:
                buf.popleft()

    def compute_groups(self, now_ms: int) -> dict[str, dict[str, Any]]:
        """Compute q75 per group → blend with committed → return groups dict."""
        out: dict[str, dict[str, Any]] = {}
        for (sym, sess), buf in self._buffers.items():
            samples = list(buf)
            n = len(samples)
            # min_n only enforced for real (non-wildcard) groups
            if n < self.min_n and "*" not in sym and "*" not in sess:
                continue
            if n < 3:
                continue
            q25 = _weighted_quantile(samples, 0.25, now_ms)
            q50 = _weighted_quantile(samples, 0.50, now_ms)
            q75 = _weighted_quantile(samples, 0.75, now_ms)
            key_str = f"{sym}:{sess}".upper()
            old_bps = self._committed.get(key_str, self.default_bps)
            new_bps = self._blend(q75, old_bps)
            out[key_str] = {
                "symbol": sym,
                "session": sess,
                "n": n,
                "q25": round(q25, 3),
                "q50": round(q50, 3),
                "q75": round(q75, 3),
                "old_bps": round(old_bps, 3),
                "new_bps": round(new_bps, 3),
            }
        return out

    def _blend(self, q75: float, old_bps: float) -> float:
        if not math.isfinite(old_bps) or old_bps <= 0:
            old_bps = self.default_bps
        if not math.isfinite(q75) or q75 <= 0:
            q75 = self.default_bps
        blended = (1.0 - self.alpha) * old_bps + self.alpha * q75
        return max(self.lower, min(self.upper, blended))

    def commit(self, groups: dict[str, dict[str, Any]]) -> None:
        for key_str, entry in groups.items():
            self._committed[key_str] = float(entry["new_bps"])

    def load_state(self, data: dict[str, Any]) -> None:
        """Restore committed values from a previous snapshot (buffers not restored)."""
        if data.get("schema_version") not in (1,):
            return
        for k, entry in data.get("groups", {}).items():
            try:
                bps = float(entry.get("new_bps") or entry.get("q75") or self.default_bps)
                if bps > 0:
                    self._committed[str(k).upper()] = bps
            except Exception:
                pass

    def sample_counts(self) -> dict[str, int]:
        return {f"{k[0]}:{k[1]}": len(v) for k, v in self._buffers.items()}


# ---------------------------------------------------------------------------
# Auto-promoter: shadow → enforce based on quality criteria
# ---------------------------------------------------------------------------


class _AutoPromoter:
    """
    Monitors shadow calibrator quality and promotes to enforce once ready.

    Criteria (ALL must pass):
      1. total_observations >= min_obs
      2. real_groups >= min_groups   (non-wildcard groups with n >= min_n)
      3. dwell_sec elapsed since startup
      4. if enforce key already exists: avg drift vs shadow <= max_drift_pct
    """

    def __init__(
        self,
        *,
        enabled: bool,
        min_obs: int,
        min_groups: int,
        dwell_sec: int,
        max_drift_pct: float,
    ) -> None:
        self.enabled = enabled
        self.min_obs = min_obs
        self.min_groups = min_groups
        self.dwell_sec = dwell_sec
        self.max_drift_pct = max_drift_pct

        self._start_time = time.monotonic()
        self._total_obs: int = 0
        self._promoted: bool = False

    def observe(self, n: int = 1) -> None:
        self._total_obs += n

    @property
    def promoted(self) -> bool:
        return self._promoted

    def check(
        self,
        *,
        groups: dict[str, Any],
        r: Any,
        min_n: int,
    ) -> bool:
        """Return True if should switch to enforce. Idempotent after promote."""
        if not self.enabled or self._promoted:
            return self._promoted

        elapsed = time.monotonic() - self._start_time
        real_groups = sum(
            1 for k, v in groups.items()
            if "*" not in k and v.get("n", 0) >= min_n
        )

        _promote_obs.set(self._total_obs)
        criteria_met = (
            self._total_obs >= self.min_obs
            and real_groups >= self.min_groups
            and elapsed >= self.dwell_sec
        )
        _promote_ready.set(1.0 if criteria_met else 0.0)

        if not criteria_met:
            logger.debug(
                "Promote not ready: obs=%d/%d groups=%d/%d dwell=%.0fs/%.0fs",
                self._total_obs, self.min_obs,
                real_groups, self.min_groups,
                elapsed, self.dwell_sec,
            )
            return False

        # Check drift vs existing enforce key (optional guard)
        if self.max_drift_pct < 100:
            try:
                raw = r.get(CAL_KEY_ENFORCE)
                if raw:
                    old_data = json.loads(raw)
                    old_groups = old_data.get("groups", {})
                    drifts = []
                    for k, entry in groups.items():
                        if "*" in k:
                            continue
                        if k in old_groups:
                            old_bps = old_groups[k].get("new_bps", 0)
                            new_bps = entry.get("new_bps", 0)
                            if old_bps > 0:
                                drifts.append(abs(new_bps - old_bps) / old_bps * 100)
                    if drifts:
                        avg_drift = sum(drifts) / len(drifts)
                        if avg_drift > self.max_drift_pct:
                            logger.warning(
                                "Auto-promote BLOCKED: avg drift %.1f%% > %.1f%%",
                                avg_drift, self.max_drift_pct,
                            )
                            return False
            except Exception as e:
                logger.warning("Drift check failed (non-blocking): %s", e)

        self._promoted = True
        _promoted.set(1.0)
        logger.info(
            "🚀 AUTO-PROMOTE: slip-ac shadow→enforce "
            "(obs=%d groups=%d dwell=%.0fs)",
            self._total_obs, real_groups, elapsed,
        )
        return True


# ---------------------------------------------------------------------------
# Session from timestamp
# ---------------------------------------------------------------------------

def _session_from_ts(ts_ms: int) -> str:
    """Derive approximate trading session from UTC timestamp."""
    h = (ts_ms // 3_600_000) % 24
    if 13 <= h < 21:
        return "us_main"
    elif 8 <= h < 16:
        return "european"
    elif 0 <= h < 8:
        return "asian"
    else:
        return "overnight"


# ---------------------------------------------------------------------------
# Parse trades:closed stream entry
# ---------------------------------------------------------------------------

def _extract_adverse_bps(
    payload: dict[str, Any],
    *,
    adverse_key_ms: int,
) -> float | None:
    """
    Extract a float adverse_bps estimate from the close payload.

    Priority:
    1. payload.close.adverse_bps_t[adverse_key_ms]  (dict bucket)
    2. payload.close.adverse_bps_t as float           (legacy column)
    3. indicators.adverse_bps_t (dict bucket)
    4. expected_slippage_bps from indicators
    5. slippage_bps_est from indicators
    """
    close_ev = payload.get("close") or {}
    if isinstance(close_ev, str):
        try:
            close_ev = json.loads(close_ev)
        except Exception:
            close_ev = {}

    # 1+2. adverse_bps_t from close event
    adv_raw = close_ev.get("adverse_bps_t")
    if adv_raw is not None:
        if isinstance(adv_raw, dict):
            try:
                # Try the target bucket first, then any non-zero value
                v = adv_raw.get(adverse_key_ms) or adv_raw.get(str(adverse_key_ms))
                if v is None:
                    v = next((vv for vv in adv_raw.values() if vv and float(vv) > 0), None)
                if v is not None:
                    fv = float(v)
                    if math.isfinite(fv) and fv > 0:
                        return fv
            except Exception:
                pass
        else:
            try:
                fv = float(adv_raw)
                if math.isfinite(fv) and fv > 0:
                    return fv
            except Exception:
                pass

    # 3. indicators.adverse_bps_t
    indicators = (
        payload.get("indicators")
        or (payload.get("decision") or {}).get("indicators")
        or {}
    )
    if isinstance(indicators, str):
        try:
            indicators = json.loads(indicators)
        except Exception:
            indicators = {}
    adv_ind = indicators.get("adverse_bps_t")
    if isinstance(adv_ind, dict):
        try:
            v = adv_ind.get(adverse_key_ms) or adv_ind.get(str(adverse_key_ms))
            if v is None:
                v = next((vv for vv in adv_ind.values() if vv and float(vv) > 0), None)
            if v is not None:
                fv = float(v)
                if math.isfinite(fv) and fv > 0:
                    return fv
        except Exception:
            pass
    elif adv_ind is not None:
        try:
            fv = float(adv_ind)
            if math.isfinite(fv) and fv > 0:
                return fv
        except Exception:
            pass

    # 4+5. slippage proxies from indicators (pre-execution estimates)
    for key in ("expected_slippage_bps", "slippage_bps_est", "final_expected_slippage_bps"):
        v = indicators.get(key) or close_ev.get(key)
        if v is not None:
            try:
                fv = float(v)
                if math.isfinite(fv) and fv > 0:
                    return fv
            except Exception:
                pass

    return None


def _parse_message(fields: dict[str, Any], adverse_key_ms: int) -> dict[str, Any] | None:
    """Parse trades:closed stream entry → {symbol, session, adverse_bps, ts_ms}."""
    def _d(k: str) -> str:
        v = fields.get(k, "")
        return v.decode() if isinstance(v, bytes) else str(v or "")

    symbol = _d("symbol").upper().strip()
    if not symbol:
        return None

    # ts_ms from stream
    ts_raw = _d("ts_ms") or _d("ts_close")
    try:
        ts_ms = int(float(ts_raw)) if ts_raw else int(time.time() * 1000)
    except Exception:
        ts_ms = int(time.time() * 1000)

    # Payload JSON
    payload_raw = _d("payload")
    try:
        payload = json.loads(payload_raw) if payload_raw else {}
    except Exception:
        payload = {}

    # `features` flat field (used by trade_monitor since 2026-05): contains
    # adverse_bps_t dict with buckets 100/200/400/800 ms. When `payload` has
    # no `close`/`indicators`, inject `features` as the indicators dict so
    # _extract_adverse_bps can reach adverse_bps_t.
    if not payload.get("close") and not payload.get("indicators"):
        features_raw = _d("features")
        if features_raw:
            try:
                payload = {"indicators": json.loads(features_raw)}
            except Exception:
                pass

    # Session: from decision, calib field, or time-of-day derivation
    session = (
        _d("session").lower()
        or str((payload.get("decision") or {}).get("session") or "").lower()
        or str((payload.get("close") or {}).get("session") or "").lower()
        or _session_from_ts(ts_ms)
    )
    if not session or session == "none":
        session = _session_from_ts(ts_ms)

    # Adverse BPS — primary: adverse_bps_t from features/payload.
    # Fallback: realized_slippage_bps (post-trade measured slippage).
    adverse_bps = _extract_adverse_bps(payload, adverse_key_ms=adverse_key_ms)
    if adverse_bps is None:
        rs_raw = _d("realized_slippage_bps")
        try:
            rs = float(rs_raw)
            if math.isfinite(rs) and rs > 0:
                adverse_bps = rs
        except Exception:
            pass
    if adverse_bps is None:
        return None

    return {"symbol": symbol, "session": session, "adverse_bps": adverse_bps, "ts_ms": ts_ms}


# ---------------------------------------------------------------------------
# Snapshot publish
# ---------------------------------------------------------------------------

def _publish(
    r: Any,
    cal: SlippageAutoCal,
    *,
    enforce: bool,
    now_ms: int,
    promoter: "_AutoPromoter | None" = None,
) -> dict[str, Any]:
    """Publish snapshot. Returns groups dict (empty on error)."""
    try:
        groups = cal.compute_groups(now_ms)
        if not groups:
            return {}
        cal.commit(groups)

        # Auto-promote check: switches enforce flag for this call and future ones
        if promoter is not None and not enforce:
            if promoter.check(groups=groups, r=r, min_n=cal.min_n):
                enforce = True

        _enforce_mode.set(1.0 if enforce else 0.0)

        payload = json.dumps(
            {
                "schema_version": 1,
                "calibrated_ms": now_ms,
                "method": "streaming_q75_v1",
                "enforce": enforce,
                "n_groups": len(groups),
                "alpha": round(cal.alpha, 6),
                "slip_lower": cal.lower,
                "slip_upper": cal.upper,
                "half_life_days": cal.half_life_days,
                "groups": groups,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

        key = CAL_KEY_ENFORCE if enforce else CAL_KEY_SHADOW
        r.set(key, payload)
        _snap_writes.inc()

        for k, entry in groups.items():
            parts = k.split(":", 1)
            sym = parts[0] if parts else k
            sess = parts[1] if len(parts) > 1 else "*"
            _group_bps.labels(symbol=sym, session=sess).set(entry["new_bps"])

        for (sym, sess), buf in cal._buffers.items():
            _buf_size.labels(symbol=sym, session=sess).set(len(buf))

        return groups

    except Exception as e:
        _snap_errors.inc()
        logger.error("Snapshot error: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(
    redis_url: str,
    *,
    group: str,
    consumer: str,
    batch: int,
    window_days: int,
    half_life_days: float,
    alpha: float,
    lower: float,
    upper: float,
    min_n: int,
    snap_sec: int,
    enforce: bool,
    adverse_key_ms: int,
    default_bps: float,
    promote_enabled: bool,
    promote_min_obs: int,
    promote_min_groups: int,
    promote_dwell_sec: int,
    promote_max_drift_pct: float,
) -> None:
    r = redis.from_url(redis_url, decode_responses=True)  # type: ignore[assignment]
    stream = RS.TRADES_CLOSED

    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception:
        pass

    cal = SlippageAutoCal(
        window_days=window_days,
        half_life_days=half_life_days,
        alpha=alpha,
        lower=lower,
        upper=upper,
        min_n=min_n,
        default_bps=default_bps,
    )

    promoter = _AutoPromoter(
        enabled=promote_enabled and not enforce,
        min_obs=promote_min_obs,
        min_groups=promote_min_groups,
        dwell_sec=promote_dwell_sec,
        max_drift_pct=promote_max_drift_pct,
    ) if promote_enabled and not enforce else None

    # Bootstrap from existing batch calibrator output
    src_key = CAL_KEY_ENFORCE if enforce else CAL_KEY_SHADOW
    try:
        raw = r.get(src_key)
        if not raw:
            raw = r.get(CAL_KEY_ENFORCE)  # always try enforce as seed
        if raw:
            cal.load_state(json.loads(raw))
            logger.info("State bootstrapped from %s (%d groups)", src_key, len(cal._committed))
    except Exception as e:
        logger.warning("Bootstrap failed: %s", e)

    _enforce_mode.set(1.0 if enforce else 0.0)
    last_snap = time.monotonic()
    logger.info(
        "slip-ac started: stream=%s group=%s enforce=%s promote=%s "
        "window=%dd half_life=%gd snap=%ds",
        stream, group, enforce, promote_enabled,
        window_days, half_life_days, snap_sec,
    )

    _stop = False

    def _sig(_n: int, _f: Any) -> None:
        nonlocal _stop
        _stop = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    while not _stop:
        try:
            msgs = r.xreadgroup(group, consumer, {stream: ">"}, count=batch, block=2000)
        except Exception as e:
            logger.error("XREADGROUP error: %s", e)
            time.sleep(2)
            continue

        if not msgs:
            pass
        else:
            now_ms = int(time.time() * 1000)
            for _sk, entries in msgs:
                for msg_id, raw in entries:
                    try:
                        fields = {
                            (k.decode() if isinstance(k, bytes) else k):
                            (v.decode() if isinstance(v, bytes) else v)
                            for k, v in raw.items()
                        }
                        trade = _parse_message(fields, adverse_key_ms)
                        if trade is None:
                            _trades_skipped.labels(reason="no_adverse_bps").inc()
                        else:
                            _lag_hist.observe(now_ms - trade["ts_ms"])
                            cal.observe(
                                symbol=trade["symbol"],
                                session=trade["session"],
                                adverse_bps=trade["adverse_bps"],
                                ts_ms=trade["ts_ms"],
                            )
                            _trades_observed.labels(
                                symbol=trade["symbol"], session=trade["session"]
                            ).inc()
                            if promoter is not None:
                                promoter.observe()
                        r.xack(stream, group, msg_id)
                    except Exception as e:
                        logger.error("Processing %s: %s", msg_id, e)
                        r.xack(stream, group, msg_id)

        now = time.monotonic()
        if now - last_snap >= snap_sec:
            current_enforce = enforce or (promoter.promoted if promoter else False)
            _publish(
                r, cal,
                enforce=current_enforce,
                now_ms=int(time.time() * 1000),
                promoter=promoter,
            )
            last_snap = now

    logger.info("Shutting down slip-ac")
    current_enforce = enforce or (promoter.promoted if promoter else False)
    _publish(r, cal, enforce=current_enforce, now_ms=int(time.time() * 1000), promoter=None)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    port = _env_int("SLIP_AC_PORT", 9157)
    try:
        start_http_server(port)
        logger.info("Metrics: :%d/metrics", port)
    except Exception as e:
        logger.warning("Metrics server: %s", e)

    run(
        redis_url=_env("SLIP_AC_REDIS_URL") or _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        group=_env("SLIP_AC_GROUP", "slip-ac"),
        consumer=_env("SLIP_AC_CONSUMER", "slip-ac-1"),
        batch=_env_int("SLIP_AC_BATCH", 100),
        window_days=_env_int("SLIP_AC_WINDOW_DAYS", 30),
        half_life_days=_env_float("SLIP_AC_HALF_LIFE_DAYS", 7.0),
        alpha=_env_float("SLIP_AC_ALPHA", 0.095),
        lower=_env_float("SLIP_AC_LOWER", 1.0),
        upper=_env_float("SLIP_AC_UPPER", 30.0),
        min_n=_env_int("SLIP_AC_MIN_N", 20),
        snap_sec=_env_int("SLIP_AC_SNAP_SEC", 60),
        enforce=_env_bool("SLIP_AC_ENFORCE", False),
        adverse_key_ms=_env_int("SLIP_AC_ADVERSE_KEY_MS", 2000),
        default_bps=_env_float("SLIP_AC_DEFAULT_BPS", 4.0),
        promote_enabled=_env_bool("SLIP_AC_PROMOTE_ENABLED", False),
        promote_min_obs=_env_int("SLIP_AC_PROMOTE_MIN_OBS", 500),
        promote_min_groups=_env_int("SLIP_AC_PROMOTE_MIN_GROUPS", 5),
        promote_dwell_sec=_env_int("SLIP_AC_PROMOTE_DWELL_SEC", 3600),
        promote_max_drift_pct=_env_float("SLIP_AC_PROMOTE_MAX_DRIFT", 30.0),
    )


if __name__ == "__main__":
    main()
