"""tp1_adaptive_metrics.py — Prometheus + XADD emit for AdaptiveTP1Policy decisions.

Called by signals/level_enricher.py once per decision. Two side-effects:

  1. Prometheus counters/histograms recorded against the live OF service registry
     (port 8000 + per-service scrape labels). Always best-effort: if
     prometheus_client is missing or any label cardinality blows up, the call
     becomes a no-op.

  2. XADD an envelope to `stream:tp1_adaptive_shadow_events` on the worker-1
     Redis. Persister `services/tp1_adaptive_shadow_persister.py` consumes the
     stream and writes to `tp1_adaptive_shadow` hypertable in scanner_analytics.

Design constraints:
  - **Never raise**. Emit is a best-effort observation: a failed XADD must NOT
    break signal publication.
  - **Lazy Redis client**. We don't want every signal path to require a Redis
    handle. A module singleton lazily connects to REDIS_URL on first emit.
  - **TTL-bounded stream**. MAXLEN ~ 200k entries (see RS.TP1_ADAPTIVE_SHADOW in
    core/redis_keys.py).
  - **Master switch**. `TP1_ADAPTIVE_EMIT_ENABLED=0` short-circuits both metric
    increments and XADD (default 1).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prometheus metrics (best-effort)
# ---------------------------------------------------------------------------

_DECISIONS_TOTAL: Any = None
_APPLY_TOTAL: Any = None
_EV_DELTA_HIST: Any = None
_RR_SELECTED_HIST: Any = None
_P_HIT_HIST: Any = None
_SAMPLES_HIST: Any = None
_EMIT_ERRORS: Any = None
_METRICS_READY = False


def _init_metrics_once() -> None:
    global _DECISIONS_TOTAL, _APPLY_TOTAL, _EV_DELTA_HIST, _RR_SELECTED_HIST
    global _P_HIT_HIST, _SAMPLES_HIST, _EMIT_ERRORS, _METRICS_READY
    if _METRICS_READY:
        return
    try:
        from prometheus_client import Counter, Histogram  # type: ignore
        _DECISIONS_TOTAL = Counter(
            "tp1_adaptive_decision_total",
            "AdaptiveTP1Policy decisions by reason and mode",
            ["reason", "mode"],
        )
        _APPLY_TOTAL = Counter(
            "tp1_adaptive_apply_total",
            "AdaptiveTP1Policy decisions that resulted in apply=True",
        )
        _EV_DELTA_HIST = Histogram(
            "tp1_adaptive_ev_delta_r",
            "ev_adaptive_r - ev_baseline_r distribution",
            buckets=(-1.0, -0.5, -0.25, -0.1, -0.05, 0.0, 0.05, 0.10, 0.25, 0.50, 1.0),
        )
        _RR_SELECTED_HIST = Histogram(
            "tp1_adaptive_rr_selected",
            "TP1_R picked by AdaptiveTP1Policy",
            buckets=(0.5, 0.65, 0.8, 1.0, 1.15, 1.3, 1.5, 2.0),
        )
        _P_HIT_HIST = Histogram(
            "tp1_adaptive_p_hit",
            "P_hit value used for the picked TP1_R",
            buckets=(0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0),
        )
        _SAMPLES_HIST = Histogram(
            "tp1_adaptive_samples",
            "n_total of the bucket used in the decision",
            buckets=(50, 100, 200, 500, 1000, 2500, 5000, 10000),
        )
        _EMIT_ERRORS = Counter(
            "tp1_adaptive_emit_errors_total",
            "Errors during metrics or XADD emit",
            ["stage"],
        )
        _METRICS_READY = True
    except Exception as exc:  # pragma: no cover — prometheus optional
        logger.debug("tp1_adaptive_metrics: prom init failed (fail-open): %s", exc)


# ---------------------------------------------------------------------------
# Redis emit (lazy singleton)
# ---------------------------------------------------------------------------

_REDIS_CLIENT: Any = None
_REDIS_LOCK = threading.Lock()
_REDIS_INIT_FAILED = False


def _get_redis_client() -> Any | None:
    global _REDIS_CLIENT, _REDIS_INIT_FAILED
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if _REDIS_INIT_FAILED:
        return None
    with _REDIS_LOCK:
        if _REDIS_CLIENT is not None:
            return _REDIS_CLIENT
        if _REDIS_INIT_FAILED:
            return None
        try:
            import redis  # type: ignore
            url = (
                os.getenv("TP1_ADAPTIVE_EMIT_REDIS_URL")
                or os.getenv("TP1_PHIT_READ_REDIS_URL")
                or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            )
            _REDIS_CLIENT = redis.from_url(url, decode_responses=True)
        except Exception as exc:
            logger.debug("tp1_adaptive_metrics: redis init failed: %s", exc)
            _REDIS_INIT_FAILED = True
            return None
    return _REDIS_CLIENT


def reset_redis_for_tests() -> None:
    """Test-only: drop lazy singleton."""
    global _REDIS_CLIENT, _REDIS_INIT_FAILED
    with _REDIS_LOCK:
        _REDIS_CLIENT = None
        _REDIS_INIT_FAILED = False


# ---------------------------------------------------------------------------
# Envelope builder
# ---------------------------------------------------------------------------


def _env_on(name: str, default: str = "1") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v.strip() else default
    except Exception:
        return default


def build_envelope(
    *,
    decision: Any,
    symbol: str,
    kind: str,
    side: str,
    regime: str,
    entry_price: float,
    sl_price: float,
    baseline_tp1_price: float,
    baseline_tp1_rr: float | None,
    adaptive_tp1_price: float | None,
    spread_bps: float,
    slippage_bps: float,
    fee_bps: float,
    ts_ms: int | None = None,
    sid: str | None = None,
) -> dict[str, Any]:
    """Build a flat dict envelope suitable for XADD field-pairs.

    Always present: ts_ms, sid, symbol, kind, side, regime, entry_price,
    sl_price, baseline_tp1_price, baseline_tp1_rr, reason_code, mode.
    Optional / NULL-able downstream: adaptive_tp1_*, p_hit_*, ev_*, cost_r,
    samples.
    """
    now_ms = int(time.time() * 1000)
    ts = int(ts_ms) if ts_ms else now_ms
    s = sid or f"shadow:{symbol}:{ts}:{kind}:{side}"
    env: dict[str, Any] = {
        "ts_ms": ts,
        "sid": s,
        "symbol": symbol,
        "kind": kind or "",
        "side": side,
        "regime": regime or "",
        "entry_price": float(entry_price),
        "sl_price": float(sl_price),
        "baseline_tp1_price": float(baseline_tp1_price),
        "baseline_tp1_rr": float(baseline_tp1_rr) if baseline_tp1_rr is not None else None,
        "adaptive_tp1_price": float(adaptive_tp1_price) if adaptive_tp1_price is not None else None,
        "adaptive_tp1_rr": float(decision.tp1_rr) if decision.tp1_rr is not None else None,
        "p_hit_baseline": float(decision.p_hit_baseline) if decision.p_hit_baseline is not None else None,
        "p_hit_adaptive": float(decision.p_hit) if decision.p_hit is not None else None,
        "ev_baseline_r": float(decision.ev_baseline_r),
        "ev_adaptive_r": float(decision.ev_adaptive_r),
        "ev_delta_r": float(decision.ev_delta_r),
        "cost_r": float(decision.cost_r),
        "spread_bps": float(spread_bps),
        "slippage_bps": float(slippage_bps),
        "fee_bps": float(fee_bps),
        "samples": int(decision.samples),
        "reason_code": str(decision.reason),
        "mode": str(decision.mode),
    }
    return env


def _flatten_for_xadd(env: dict[str, Any]) -> dict[str, str]:
    """Coerce envelope into Redis-stream field-pair shape (str values)."""
    out: dict[str, str] = {}
    for k, v in env.items():
        if v is None:
            out[k] = ""
            continue
        if isinstance(v, (int, str)):
            out[k] = str(v)
        elif isinstance(v, float):
            out[k] = f"{v:.10g}"
        else:
            out[k] = json.dumps(v, separators=(",", ":"))
    return out


# ---------------------------------------------------------------------------
# Public API: emit_decision
# ---------------------------------------------------------------------------


def emit_decision(
    *,
    decision: Any,
    symbol: str,
    kind: str,
    side: str,
    regime: str,
    entry_price: float,
    sl_price: float,
    baseline_tp1_price: float,
    baseline_tp1_rr: float | None,
    adaptive_tp1_price: float | None,
    spread_bps: float,
    slippage_bps: float,
    fee_bps: float,
    ts_ms: int | None = None,
    sid: str | None = None,
) -> None:
    """Record Prometheus metrics + XADD envelope to shadow stream.

    Never raises. Disabled when `TP1_ADAPTIVE_EMIT_ENABLED=0` (default 1).
    Skips skip_disabled reasons to avoid metric pollution when the policy is
    inert for the whole service.
    """
    if not _env_on("TP1_ADAPTIVE_EMIT_ENABLED", "1"):
        return
    if decision is None:
        return
    # Avoid counting decisions while the policy itself is disabled.
    reason = str(decision.reason or "")
    if reason == "tp1_adaptive_skip_disabled":
        return

    _init_metrics_once()

    # ── Prometheus ────────────────────────────────────────────────────────
    try:
        if _DECISIONS_TOTAL is not None:
            _DECISIONS_TOTAL.labels(reason=reason, mode=str(decision.mode or "")).inc()
        if decision.apply and _APPLY_TOTAL is not None:
            _APPLY_TOTAL.inc()
        if _EV_DELTA_HIST is not None:
            _EV_DELTA_HIST.observe(float(decision.ev_delta_r))
        if decision.tp1_rr is not None and _RR_SELECTED_HIST is not None:
            _RR_SELECTED_HIST.observe(float(decision.tp1_rr))
        if decision.p_hit is not None and _P_HIT_HIST is not None:
            _P_HIT_HIST.observe(float(decision.p_hit))
        if decision.samples and _SAMPLES_HIST is not None:
            _SAMPLES_HIST.observe(float(decision.samples))
    except Exception as exc:
        logger.debug("tp1_adaptive_metrics: prom emit failed: %s", exc)
        if _EMIT_ERRORS is not None:
            try:
                _EMIT_ERRORS.labels(stage="prom").inc()
            except Exception:
                pass

    # ── XADD ──────────────────────────────────────────────────────────────
    rd = _get_redis_client()
    if rd is None:
        return
    try:
        env = build_envelope(
            decision=decision,
            symbol=symbol, kind=kind, side=side, regime=regime,
            entry_price=entry_price, sl_price=sl_price,
            baseline_tp1_price=baseline_tp1_price,
            baseline_tp1_rr=baseline_tp1_rr,
            adaptive_tp1_price=adaptive_tp1_price,
            spread_bps=spread_bps, slippage_bps=slippage_bps, fee_bps=fee_bps,
            ts_ms=ts_ms, sid=sid,
        )
        maxlen = _env_int("TP1_ADAPTIVE_SHADOW_MAXLEN", 200_000)
        rd.xadd(
            RS.TP1_ADAPTIVE_SHADOW,
            _flatten_for_xadd(env),
            maxlen=maxlen,
            approximate=True,
        )
    except Exception as exc:
        logger.debug("tp1_adaptive_metrics: xadd failed: %s", exc)
        if _EMIT_ERRORS is not None:
            try:
                _EMIT_ERRORS.labels(stage="xadd").inc()
            except Exception:
                pass
