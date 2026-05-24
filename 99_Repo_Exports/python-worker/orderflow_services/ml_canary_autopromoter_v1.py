"""ml_canary_autopromoter_v1.py — auto-promote ML_SCORER_CANARY_RATE on quality lift.

Loop service that compares trades opened under the canary cohort:
  - `ml_canary_enforce` (ML conf01 used)  vs
  - `canary_shadow`     (rule-based used, ML logged)

If the ML cohort shows statistically-significant lift in mean R-multiple over
the control cohort (with enough sample and dwell time), the canary rate is
promoted one step up the ladder. On regression, demoted.

Promotion ladder: 0.05 → 0.10 → 0.20 → 0.40 (hard ceiling — manual to 1.0).

Decision gates (all must hold to promote):
  - enforce_n  >= MIN_SAMPLES
  - shadow_n   >= MIN_SAMPLES
  - mean_diff_r > MIN_LIFT_R
  - p_value    < MAX_PVALUE
  - dwell_h    >= DWELL_HOURS  (consecutive eval windows passing gate)
  - cooldown_h >= COOLDOWN_HOURS since last promotion

Demote when mean_diff_r < -MIN_LIFT_R (halve rate, floor 0.05).

State persisted as HMAC-signed JSON in Redis key `autocal:ml_canary:state`.
Reader: `core/ml_canary_runtime_overrides.py`.

Master switches:
  ML_CANARY_AUTOCAL_ENABLE=1   — service does work (default 1)
  ML_CANARY_AUTOCAL_ENFORCE=0  — writes enforce=1 to override (default 0 → shadow)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import os
import signal
import sys
import time
from typing import Any

import redis

from core.redis_keys import RedisStreams as RS

logger = logging.getLogger("ml_canary_autopromoter")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ── Configuration ──────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
STATE_KEY = os.getenv("AUTOCAL_ML_CANARY_KEY", "autocal:ml_canary:state")
SIGNALS_STREAM = os.getenv("ML_CANARY_SIGNALS_STREAM", "signals:of:inputs")
TRADES_STREAM = os.getenv("ML_CANARY_TRADES_STREAM", RS.TRADES_CLOSED)

ENABLE = os.getenv("ML_CANARY_AUTOCAL_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
ENFORCE = os.getenv("ML_CANARY_AUTOCAL_ENFORCE", "0").strip().lower() in ("1", "true", "yes", "on")

INTERVAL_S = int(os.getenv("ML_CANARY_AUTOCAL_INTERVAL_S", "900"))  # 15 min
LOOKBACK_H = float(os.getenv("ML_CANARY_AUTOCAL_LOOKBACK_H", "24"))
MIN_SAMPLES = int(os.getenv("ML_CANARY_AUTOCAL_MIN_SAMPLES", "200"))
MIN_LIFT_R = float(os.getenv("ML_CANARY_AUTOCAL_MIN_LIFT_R", "0.05"))
MAX_PVALUE = float(os.getenv("ML_CANARY_AUTOCAL_MAX_PVALUE", "0.10"))
DWELL_HOURS = float(os.getenv("ML_CANARY_AUTOCAL_DWELL_H", "24.0"))
COOLDOWN_HOURS = float(os.getenv("ML_CANARY_AUTOCAL_COOLDOWN_H", "12.0"))
PROMOTION_LADDER: tuple[float, ...] = (0.05, 0.10, 0.20, 0.40)
RATE_FLOOR = float(os.getenv("ML_CANARY_AUTOCAL_RATE_FLOOR", "0.05"))
RATE_CEILING = float(os.getenv("ML_CANARY_AUTOCAL_RATE_CEILING", str(PROMOTION_LADDER[-1])))

HMAC_SECRET = (
    os.getenv("ML_CANARY_AUTOCAL_HMAC_SECRET", "")
    or os.getenv("LAYERS_CAL_HMAC_SECRET", "")
    or os.getenv("RECS_HMAC_SECRET", "")
)

METRICS_PORT = int(os.getenv("METRICS_PORT", "9864"))

# ── Prometheus metrics ─────────────────────────────────────────────────────────

try:
    from prometheus_client import Counter, Gauge, start_http_server

    _CYCLES_TOTAL = Counter(
        "ml_canary_autocal_cycles_total",
        "Autocal cycles run",
        ["outcome"],  # promote / demote / hold / no_data / error
    )
    _CURRENT_RATE = Gauge(
        "ml_canary_autocal_current_rate",
        "Currently recommended canary rate (env default if no override)",
    )
    _PROPOSED_RATE = Gauge(
        "ml_canary_autocal_proposed_rate",
        "Proposed canary rate from last evaluation",
    )
    _ENFORCE_FLAG = Gauge(
        "ml_canary_autocal_enforce",
        "1 if override is being enforced (consumed by reader); 0 = shadow",
    )
    _ENFORCE_N = Gauge(
        "ml_canary_autocal_enforce_n",
        "Sample count of ml_canary_enforce trades in last window",
    )
    _SHADOW_N = Gauge(
        "ml_canary_autocal_shadow_n",
        "Sample count of canary_shadow trades in last window",
    )
    _MEAN_DIFF_R = Gauge(
        "ml_canary_autocal_mean_diff_r",
        "mean(r_enforce) - mean(r_shadow)",
    )
    _PVALUE = Gauge(
        "ml_canary_autocal_pvalue",
        "Welch's t-test p-value (two-sided)",
    )
    _DWELL_H = Gauge(
        "ml_canary_autocal_dwell_h",
        "Consecutive hours the promotion gate has been passing",
    )
    _METRICS_OK = True
except Exception:
    _CYCLES_TOTAL = None
    _CURRENT_RATE = None
    _PROPOSED_RATE = None
    _ENFORCE_FLAG = None
    _ENFORCE_N = None
    _SHADOW_N = None
    _MEAN_DIFF_R = None
    _PVALUE = None
    _DWELL_H = None
    start_http_server = None  # type: ignore
    _METRICS_OK = False

# ── Helpers ────────────────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(time.time() * 1000)


def _norm_sid(raw: str | None) -> str | None:
    """Normalise sid to `SYMBOL:TS` for cross-stream join.

    Strips kind-prefix (`of:`, `iceberg:`, `crypto-of:`, …) and direction
    suffix (`:L`/`:S`/`:LONG`/`:SHORT`).  Anything that doesn't shape up
    returns None.
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) < 3:
        return None
    # Drop kind prefix when first token is lowercase alpha (e.g. "of", "iceberg",
    # "crypto-of") AND the second token looks like a symbol (uppercase letters).
    sym_idx = 0
    if (
        parts[0].replace("-", "").isalpha()
        and parts[0] == parts[0].lower()
        and len(parts) >= 3
        and parts[1].isalnum()
        and parts[1] == parts[1].upper()
    ):
        sym_idx = 1
    if sym_idx >= len(parts):
        return None
    symbol = parts[sym_idx]
    if not symbol or not symbol.isalnum():
        return None
    ts_idx = sym_idx + 1
    if ts_idx >= len(parts):
        return None
    ts = parts[ts_idx]
    if not ts.isdigit():
        return None
    return f"{symbol.upper()}:{ts}"


def _welch_ttest(
    n1: int, mean1: float, var1: float, n2: int, mean2: float, var2: float
) -> tuple[float, float]:
    """Two-sample Welch's t-test (unequal variances). Returns (t, p_two_sided).

    Uses Welch–Satterthwaite df + Student t survival via incomplete beta.
    """
    if n1 < 2 or n2 < 2 or (var1 <= 0 and var2 <= 0):
        return 0.0, 1.0
    se_sq = (var1 / n1) + (var2 / n2)
    if se_sq <= 0:
        return 0.0, 1.0
    t = (mean1 - mean2) / math.sqrt(se_sq)
    # Welch–Satterthwaite degrees of freedom
    num = se_sq ** 2
    denom = ((var1 / n1) ** 2) / max(1, (n1 - 1)) + ((var2 / n2) ** 2) / max(1, (n2 - 1))
    if denom <= 0:
        return t, 1.0
    df = num / denom
    p = _student_t_sf_two_sided(abs(t), df)
    return t, p


def _student_t_sf_two_sided(t: float, df: float) -> float:
    """Two-sided survival function P(|T| >= t) under Student-t(df).

    Uses regularised incomplete beta:
      P(|T| >= t) = I_{df/(df+t^2)}(df/2, 1/2)
    Implemented via continued-fraction expansion (Numerical Recipes).
    """
    if t <= 0 or df <= 0:
        return 1.0
    x = df / (df + t * t)
    a = df / 2.0
    b = 0.5
    try:
        return _betai(a, b, x)
    except Exception:
        return 1.0


def _betai(a: float, b: float, x: float) -> float:
    """Regularised incomplete beta function I_x(a,b)."""
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    # log Beta normalisation
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    front = math.exp(lbeta + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for incomplete beta (NR)."""
    MAXIT = 200
    EPS = 3.0e-7
    FPMIN = 1.0e-30
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < FPMIN:
        d = FPMIN
    d = 1.0 / d
    h = d
    for m in range(1, MAXIT + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN:
            d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN:
            c = FPMIN
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN:
            d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN:
            c = FPMIN
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < EPS:
            return h
    return h


def _ladder_step(current: float, direction: int) -> float:
    """Return next rate on ladder. direction=+1 promote, -1 demote, 0 hold."""
    if direction == 0:
        return current
    if direction > 0:
        for v in PROMOTION_LADDER:
            if v > current + 1e-9:
                return min(v, RATE_CEILING)
        return min(current, RATE_CEILING)
    # demote — halve, snap to ladder floor
    halved = max(RATE_FLOOR, current / 2.0)
    return halved


# ── Data layer ─────────────────────────────────────────────────────────────────


def _load_signals_scorer_mode(
    r: redis.Redis, since_ms: int, batch: int = 5000
) -> dict[str, str]:
    """Walk signals:of:inputs from `since_ms` and return sid_norm → scorer_mode."""
    out: dict[str, str] = {}
    cursor = f"{since_ms}-0"
    while True:
        try:
            chunk = r.xrange(SIGNALS_STREAM, min=cursor, count=batch)
        except Exception as e:
            logger.warning("signals XRANGE error: %s", e)
            break
        if not chunk:
            break
        last_id = chunk[-1][0]
        for entry_id, fields in chunk:
            payload = fields.get("payload") if isinstance(fields, dict) else None
            if not payload:
                continue
            try:
                p = json.loads(payload)
            except Exception:
                continue
            inner = p.get("data", p) if isinstance(p, dict) else p
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    continue
            if not isinstance(inner, dict):
                continue
            sid_raw = inner.get("sid") or inner.get("signal_id")
            sid = _norm_sid(sid_raw)
            if not sid:
                continue
            ind = inner.get("indicators") or {}
            cb = ind.get("confidence_breakdown") or {} if isinstance(ind, dict) else {}
            mode = cb.get("scorer_mode")
            if not isinstance(mode, str):
                continue
            # Only canary-cohort signals matter
            if mode not in ("ml_canary_enforce", "canary_shadow", "ml_canary_fallback"):
                continue
            # Treat fallback as enforce (ML was selected but failed → still counts as
            # an "ML decision" that wasted the canary slot — bias signal of model fitness).
            if mode == "ml_canary_fallback":
                mode = "ml_canary_enforce"
            out[sid] = mode
        if len(chunk) < batch:
            break
        # advance cursor past last id
        cursor = _next_xrange_cursor(last_id)
    return out


def _next_xrange_cursor(entry_id: str) -> str:
    """Return next-after cursor for XRANGE pagination."""
    try:
        ms, seq = entry_id.split("-")
        return f"{ms}-{int(seq) + 1}"
    except Exception:
        return entry_id


def _load_trades_r(
    r: redis.Redis, since_ms: int, batch: int = 5000
) -> dict[str, float]:
    """Walk trades:closed from `since_ms` and return sid_norm → r_multiple."""
    out: dict[str, float] = {}
    cursor = f"{since_ms}-0"
    while True:
        try:
            chunk = r.xrange(TRADES_STREAM, min=cursor, count=batch)
        except Exception as e:
            logger.warning("trades XRANGE error: %s", e)
            break
        if not chunk:
            break
        last_id = chunk[-1][0]
        for entry_id, fields in chunk:
            if not isinstance(fields, dict):
                continue
            # trades:closed uses flat fields, NOT a payload JSON wrapper
            sid_raw = fields.get("sid") or fields.get("signal_id")
            sid = _norm_sid(sid_raw)
            if not sid:
                continue
            r_raw = fields.get("r_multiple")
            if r_raw is None or r_raw == "":
                continue
            try:
                rv = float(r_raw)
            except Exception:
                continue
            if not math.isfinite(rv):
                continue
            # Clip extreme values to keep variance bounded (rare blow-outs)
            rv = max(-5.0, min(5.0, rv))
            out[sid] = rv
        if len(chunk) < batch:
            break
        cursor = _next_xrange_cursor(last_id)
    return out


# ── Stats ──────────────────────────────────────────────────────────────────────


def _summary(values: list[float]) -> tuple[int, float, float, float]:
    """Return (n, mean, var, hit_rate_03) for list of r-multiples."""
    n = len(values)
    if n == 0:
        return 0, 0.0, 0.0, 0.0
    mean = sum(values) / n
    if n < 2:
        var = 0.0
    else:
        s = sum((v - mean) ** 2 for v in values)
        var = s / (n - 1)
    hits = sum(1 for v in values if v >= 0.3)
    return n, mean, var, hits / n


# ── Decision ───────────────────────────────────────────────────────────────────


def _decide(
    enforce_n: int,
    enforce_mean: float,
    enforce_var: float,
    shadow_n: int,
    shadow_mean: float,
    shadow_var: float,
    prev: dict[str, Any],
) -> tuple[str, float, float, float, float]:
    """Return (action, proposed_rate, t_stat, p_value, dwell_h_new).

    action ∈ {"promote", "demote", "hold", "no_data"}
    """
    cur_rate = _clamp_rate(float(prev.get("current_rate", RATE_FLOOR)))
    if enforce_n < MIN_SAMPLES or shadow_n < MIN_SAMPLES:
        return "no_data", cur_rate, 0.0, 1.0, 0.0

    mean_diff = enforce_mean - shadow_mean
    t, p = _welch_ttest(
        enforce_n, enforce_mean, enforce_var,
        shadow_n, shadow_mean, shadow_var,
    )

    # Demote on regression — immediate (no dwell)
    if mean_diff < -MIN_LIFT_R and p < MAX_PVALUE:
        new_rate = _ladder_step(cur_rate, -1)
        return ("demote" if new_rate < cur_rate - 1e-9 else "hold"), new_rate, t, p, 0.0

    # Promotion gate: lift + p-value + dwell + cooldown
    gate_passes = (mean_diff > MIN_LIFT_R) and (p < MAX_PVALUE)
    prev_dwell = float(prev.get("dwell_h", 0.0))
    last_eval_ts = int(prev.get("last_eval_ts_ms", 0))
    now_ms = _now_ms()
    delta_h = ((now_ms - last_eval_ts) / 3600_000.0) if last_eval_ts else 0.0
    delta_h = min(delta_h, INTERVAL_S / 3600.0 * 1.5)  # cap delta
    new_dwell = (prev_dwell + delta_h) if gate_passes else 0.0

    last_promo_ts = int(prev.get("last_promotion_ts_ms", 0))
    cooldown_ok = (
        last_promo_ts == 0
        or (now_ms - last_promo_ts) / 3600_000.0 >= COOLDOWN_HOURS
    )

    if gate_passes and new_dwell >= DWELL_HOURS and cooldown_ok:
        new_rate = _ladder_step(cur_rate, +1)
        if new_rate > cur_rate + 1e-9:
            return "promote", new_rate, t, p, 0.0  # reset dwell after promote
        return "hold", cur_rate, t, p, new_dwell

    return "hold", cur_rate, t, p, new_dwell


def _clamp_rate(v: float) -> float:
    return max(RATE_FLOOR, min(RATE_CEILING, float(v)))


# ── State I/O ──────────────────────────────────────────────────────────────────


def _load_state(r: redis.Redis) -> dict[str, Any]:
    try:
        raw = r.get(STATE_KEY)
        if not raw:
            return {}
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        data = json.loads(raw)
        if HMAC_SECRET and "sig" in data:
            sig = data.pop("sig")
            canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
            actual = hmac.new(HMAC_SECRET.encode(), canon, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(actual, str(sig)):
                logger.warning("state HMAC mismatch — starting fresh")
                return {}
        return data
    except Exception as e:
        logger.warning("state load failed: %s", e)
        return {}


def _save_state(r: redis.Redis, state: dict[str, Any]) -> None:
    body = dict(state)
    body["ts_ms"] = _now_ms()
    if HMAC_SECRET:
        canon = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        body["sig"] = hmac.new(HMAC_SECRET.encode(), canon, hashlib.sha256).hexdigest()
    try:
        r.set(STATE_KEY, json.dumps(body))
    except Exception as e:
        logger.warning("state save failed: %s", e)


# ── Main loop ──────────────────────────────────────────────────────────────────


def run_once(r: redis.Redis) -> dict[str, Any]:
    """One evaluation cycle. Returns the new state dict (for tests + logs)."""
    since_ms = _now_ms() - int(LOOKBACK_H * 3600 * 1000)
    signals = _load_signals_scorer_mode(r, since_ms)
    trades = _load_trades_r(r, since_ms)

    enforce_r: list[float] = []
    shadow_r: list[float] = []
    for sid, rv in trades.items():
        mode = signals.get(sid)
        if mode == "ml_canary_enforce":
            enforce_r.append(rv)
        elif mode == "canary_shadow":
            shadow_r.append(rv)

    en_n, en_mean, en_var, en_hr = _summary(enforce_r)
    sh_n, sh_mean, sh_var, sh_hr = _summary(shadow_r)

    prev = _load_state(r)
    action, proposed_rate, t_stat, p_value, dwell_new = _decide(
        en_n, en_mean, en_var, sh_n, sh_mean, sh_var, prev,
    )

    now_ms = _now_ms()
    new_state: dict[str, Any] = {
        "current_rate": proposed_rate,
        "previous_rate": _clamp_rate(float(prev.get("current_rate", RATE_FLOOR))),
        "enforce": 1 if ENFORCE else 0,
        "last_action": action,
        "last_eval_ts_ms": now_ms,
        "last_promotion_ts_ms": (
            now_ms if action == "promote"
            else int(prev.get("last_promotion_ts_ms", 0))
        ),
        "dwell_h": dwell_new,
        "enforce_n": en_n,
        "enforce_mean_r": en_mean,
        "enforce_hit_rate": en_hr,
        "shadow_n": sh_n,
        "shadow_mean_r": sh_mean,
        "shadow_hit_rate": sh_hr,
        "mean_diff_r": en_mean - sh_mean,
        "t_stat": t_stat,
        "p_value": p_value,
        "lookback_h": LOOKBACK_H,
        "promotion_count": int(prev.get("promotion_count", 0)) + (1 if action == "promote" else 0),
        "demotion_count": int(prev.get("demotion_count", 0)) + (1 if action == "demote" else 0),
    }

    _save_state(r, new_state)
    _publish_metrics(new_state)
    logger.info(
        "cycle: action=%s rate=%.3f→%.3f en_n=%d sh_n=%d diff_r=%+.4f p=%.4f dwell_h=%.2f",
        action, new_state["previous_rate"], new_state["current_rate"],
        en_n, sh_n, new_state["mean_diff_r"], p_value, dwell_new,
    )
    return new_state


def _publish_metrics(state: dict[str, Any]) -> None:
    if not _METRICS_OK:
        return
    if _CYCLES_TOTAL is not None:
        outcome = state.get("last_action", "hold")
        _CYCLES_TOTAL.labels(outcome=outcome).inc()
    if _CURRENT_RATE is not None:
        _CURRENT_RATE.set(float(state.get("current_rate", 0.0)))
    if _PROPOSED_RATE is not None:
        _PROPOSED_RATE.set(float(state.get("current_rate", 0.0)))
    if _ENFORCE_FLAG is not None:
        _ENFORCE_FLAG.set(float(state.get("enforce", 0)))
    if _ENFORCE_N is not None:
        _ENFORCE_N.set(float(state.get("enforce_n", 0)))
    if _SHADOW_N is not None:
        _SHADOW_N.set(float(state.get("shadow_n", 0)))
    if _MEAN_DIFF_R is not None:
        _MEAN_DIFF_R.set(float(state.get("mean_diff_r", 0.0)))
    if _PVALUE is not None:
        _PVALUE.set(float(state.get("p_value", 1.0)))
    if _DWELL_H is not None:
        _DWELL_H.set(float(state.get("dwell_h", 0.0)))


_running = True


def _on_signal(signum, _frame) -> None:
    global _running
    logger.info("received signal %d — shutdown", signum)
    _running = False


def main() -> None:
    if not ENABLE:
        logger.info("ML_CANARY_AUTOCAL_ENABLE=0 — service idle (sleeping)")
        # Stay alive so docker doesn't restart-loop
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
        while _running:
            time.sleep(60)
        return

    if _METRICS_OK and start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
            logger.info("prometheus metrics on :%d", METRICS_PORT)
        except Exception as e:
            logger.warning("metrics server failed: %s", e)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    r = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info(
        "starting: stream=%s trades=%s interval=%ds lookback=%.1fh "
        "enforce=%s ladder=%s",
        SIGNALS_STREAM, TRADES_STREAM, INTERVAL_S, LOOKBACK_H,
        ENFORCE, PROMOTION_LADDER,
    )
    while _running:
        try:
            run_once(r)
        except Exception as e:
            logger.exception("cycle failed: %s", e)
            if _CYCLES_TOTAL is not None:
                _CYCLES_TOTAL.labels(outcome="error").inc()
        # interruptible sleep
        slept = 0
        while _running and slept < INTERVAL_S:
            time.sleep(min(5, INTERVAL_S - slept))
            slept += 5
    logger.info("stopped")


if __name__ == "__main__":
    main()
    sys.exit(0)
