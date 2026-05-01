from __future__ import annotations
"""python-worker/core/horizon_metrics.py

Phase 0 — Prometheus metrics для horizon-aware контракта.

Все метрики emit-only (observability). Не влияют на торговую логику.
Включаются через ATR_HORIZON_EMIT_METRICS=1 (default).
"""


import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ─── Lazy registration ────────────────────────────────────────────────────────
# Используем lazy-init чтобы не падать при import в тестах без prometheus_client

_METRICS_INITIALIZED = False
_COUNTERS: dict = {}
_GAUGES: dict = {}
_HISTS: dict = {}


def _init_metrics() -> None:
    global _METRICS_INITIALIZED, _COUNTERS, _GAUGES
    if _METRICS_INITIALIZED:
        return
    try:
        from prometheus_client import Counter, Gauge
        _COUNTERS["emitted"] = Counter(
            "trade_horizon_contract_emitted_total",
            "Horizon contract emitted to payload meta",
            ["symbol", "kind", "mode"],
        )
        _COUNTERS["missing"] = Counter(
            "trade_horizon_contract_missing_total",
            "Required horizon contract fields missing",
            ["field"],
        )
        _COUNTERS["profile_source"] = Counter(
            "trade_horizon_profile_source_total",
            "Horizon profile source distribution",
            ["source"],
        )
        _COUNTERS["reason"] = Counter(
            "trade_horizon_reason_total",
            "Horizon reason code distribution",
            ["reason_code"],
        )
        _COUNTERS["atr_source"] = Counter(
            "trade_atr_profile_source_total",
            "ATR profile source distribution",
            ["source"],
        )
        _COUNTERS["contract_ver"] = Counter(
            "trade_contract_version_total",
            "Signal payload contract version distribution",
            ["contract_ver"],
        )
        _COUNTERS["signal_too_old"] = Counter(
            "trade_signal_too_old_total",
            "Signals rejected as too old relative to max_signal_age_ms",
            ["symbol", "kind"],
        )
        _GAUGES["atr_tf_ms"] = Gauge(
            "trade_atr_tf_ms_gauge",
            "ATR timeframe in ms at last signal",
            ["symbol", "kind"],
        )
        _GAUGES["hold_target_ms"] = Gauge(
            "trade_hold_target_ms_gauge",
            "Horizon hold target in ms at last signal",
            ["symbol", "kind"],
        )
        _GAUGES["alpha_half_life_ms"] = Gauge(
            "trade_alpha_half_life_ms_gauge",
            "Horizon alpha half-life in ms at last signal",
            ["symbol", "kind"],
        )
        _GAUGES["max_signal_age_ms"] = Gauge(
            "trade_max_signal_age_ms_gauge",
            "Horizon max signal age in ms at last signal",
            ["symbol", "kind"],
        )
        _METRICS_INITIALIZED = True
    except Exception as e:
        logger.debug("horizon_metrics: prometheus_client unavailable — metrics disabled: %r", e)
        _METRICS_INITIALIZED = True  # skip retry


def emit_horizon_contract_metrics(
    *,
    symbol: str,
    kind: str,
    risk_profile: Any,
) -> None:
    """Emit all Phase 0 Prometheus metrics for a horizon contract snapshot.

    Fail-open: never raises, never blocks hot path.
    Only active when ATR_HORIZON_EMIT_METRICS=1 (default).
    """
    try:
        from core.horizon_contract import _ENV
        if not _ENV.emit_metrics():
            return
    except Exception:
        return

    try:
        _init_metrics()

        sym = str(symbol or "unknown")
        knd = str(kind or "unknown")

        atr = getattr(risk_profile, "atr", None)
        hz = getattr(risk_profile, "horizon", None)

        if atr is None or hz is None:
            _safe_counter_inc("missing", field="atr_or_horizon")
            return

        mode = str(getattr(atr, "mode", "legacy"))
        atr_source = str(getattr(atr, "atr_source", "legacy"))
        profile_source = str(getattr(hz, "profile_source", "static_bootstrap"))
        reason_code = str(getattr(hz, "reason_code", "HZ_STATIC_BOOTSTRAP"))
        contract_ver = str(getattr(hz, "contract_ver", 2))
        atr_tf_ms = int(getattr(atr, "atr_tf_ms", 60_000))
        hold_ms = int(getattr(hz, "hold_target_ms", 0))
        alpha_ms = int(getattr(hz, "alpha_half_life_ms", 0))
        max_age_ms = int(getattr(hz, "max_signal_age_ms", 0))

        _safe_counter_inc("emitted", symbol=sym, kind=knd, mode=mode)
        _safe_counter_inc("profile_source", source=profile_source)
        _safe_counter_inc("reason", reason_code=reason_code)
        _safe_counter_inc("atr_source", source=atr_source)
        _safe_counter_inc("contract_ver", contract_ver=contract_ver)
        _safe_gauge_set("atr_tf_ms", atr_tf_ms, symbol=sym, kind=knd)
        _safe_gauge_set("hold_target_ms", hold_ms, symbol=sym, kind=knd)
        _safe_gauge_set("alpha_half_life_ms", alpha_ms, symbol=sym, kind=knd)
        _safe_gauge_set("max_signal_age_ms", max_age_ms, symbol=sym, kind=knd)

    except Exception:
        pass  # never block hot path


def emit_missing_field(field: str) -> None:
    """Increment missing-field counter (fail-open)."""
    try:
        _init_metrics()
        _safe_counter_inc("missing", field=str(field))
    except Exception:
        pass


def emit_signal_too_old(symbol: str, kind: str) -> None:
    """Signal was rejected because ts exceeds max_signal_age_ms."""
    try:
        _init_metrics()
        _safe_counter_inc("signal_too_old", symbol=str(symbol), kind=str(kind))
    except Exception:
        pass


# ─── Safe helpers ─────────────────────────────────────────────────────────────

def _safe_counter_inc(name: str, **labels: str) -> None:
    try:
        c = _COUNTERS.get(name)
        if c is not None:
            c.labels(**labels).inc()
    except Exception:
        pass


def _safe_gauge_set(name: str, value: float, **labels: str) -> None:
    try:
        g = _GAUGES.get(name)
        if g is not None:
            g.labels(**labels).set(value)
    except Exception:
        pass
