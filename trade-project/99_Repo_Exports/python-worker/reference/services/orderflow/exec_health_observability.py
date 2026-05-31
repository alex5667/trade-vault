from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Single-writer observability helpers for ExecHealth.

This module is intentionally side-effect free unless the caller explicitly passes
rollups/decision from the already computed SoT path. It never re-reads Redis.
That keeps observability aligned with the exact data/decision used by:
  - EdgeCostGate
  - EntryPolicyService
  - SignalPipeline
"""

from collections.abc import Mapping
from typing import Any

from services.orderflow.exec_health_rollups import (
    ExecHealthDecision,
    get_exec_health_policy_from_env,
)
from services.orderflow.metrics_exec_health_p6 import (
    exec_health_apply_total,
    exec_health_decision_total,
    exec_health_flag_total,
    exec_health_last_event_ts_ms,
    exec_health_policy_mode,
    exec_health_policy_threshold_bps,
    exec_health_reader_errors_total,
    exec_health_rollup_present,
    exec_health_rollup_value_bps,
    exec_health_rollup_worst_delta_sec,
    exec_health_tighten_add_bps,
    exec_health_tighten_add_bps_scoped,
    exec_health_tighten_k,
    exec_health_veto_total,
)

_MODE_SET = ("off", "monitor", "tighten", "veto")
_THRESHOLD_METRICS = (
    ("is_p95_bps", "max_is_p95_bps"),
    ("perm_impact_p95_bps", "max_perm_impact_p95_bps"),
    ("realized_spread_p50_bps", "min_realized_spread_p50_bps"),
)
_ROLLUP_METRICS = ("is_p95_bps", "perm_impact_p95_bps", "realized_spread_p50_bps")
_DELTA_METRICS = (
    ("perm_impact_p95_bps", "perm_impact_p95_bps_delta_sec"),
    ("realized_spread_p50_bps", "realized_spread_p50_bps_delta_sec"),
)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return d
    if v != v or v in (float("inf"), float("-inf")):
        return d
    return float(v)


def _safe_set(metric: Any, *, labels: dict[str, Any], value: float) -> None:
    try:
        if metric is not None:
            metric.labels(**labels).set(float(value))
    except Exception:
        pass


def _safe_inc(metric: Any, *, labels: dict[str, Any], value: float = 1.0) -> None:
    try:
        if metric is not None:
            metric.labels(**labels).inc(float(value))
    except Exception:
        pass


def _safe_observe(metric: Any, *, labels: dict[str, Any], value: float) -> None:
    try:
        if metric is not None:
            metric.labels(**labels).observe(float(value))
    except Exception:
        pass


def record_exec_health_reader_error(*, scope: str, where: str) -> None:
    _safe_inc(exec_health_reader_errors_total, labels={"scope": (scope or "unknown"), "where": (where or "unknown")})


def record_exec_health_observability(
    *,
    symbol: str,
    scope: str,
    profile: str,
    rollups: Mapping[str, Any] | None = None,
    decision: ExecHealthDecision | None = None,
    now_ms: int | None = None,
) -> None:
    """Publish a single canonical ExecHealth telemetry sample.

    Inputs must come from the already evaluated SoT path. The function only emits
    Prometheus metrics; it does not mutate trading payloads and does not access Redis.
    """
    sym = (symbol or "UNKNOWN").upper()
    sc = (scope or "unknown")
    pol = get_exec_health_policy_from_env(profile=profile, scope=sc)
    thr = pol.thresholds
    ts_ms = int(now_ms or round(get_ny_time_millis()))

    _safe_set(exec_health_last_event_ts_ms, labels={"scope": sc, "symbol": sym}, value=ts_ms)

    for mode in _MODE_SET:
        _safe_set(exec_health_policy_mode, labels={"scope": sc, "mode": mode}, value=1.0 if pol.mode == mode else 0.0)

    for metric_name, attr_name in _THRESHOLD_METRICS:
        _safe_set(
            exec_health_policy_threshold_bps,
            labels={"scope": sc, "metric": metric_name},
            value=_f(getattr(thr, attr_name, 0.0), 0.0),
        )

    roll = dict(rollups or {})
    for metric_name in _ROLLUP_METRICS:
        present = 1.0 if metric_name in roll else 0.0
        _safe_set(exec_health_rollup_present, labels={"scope": sc, "symbol": sym, "metric": metric_name}, value=present)
        if present:
            _safe_set(
                exec_health_rollup_value_bps,
                labels={"scope": sc, "symbol": sym, "metric": metric_name},
                value=_f(roll.get(metric_name), 0.0),
            )

    for metric_name, delta_key in _DELTA_METRICS:
        val = roll.get(delta_key)
        if val is not None:
            _safe_set(
                exec_health_rollup_worst_delta_sec,
                labels={"scope": sc, "symbol": sym, "metric": metric_name},
                value=_f(val, 0.0),
            )

    if decision is None:
        return

    outcome = "veto" if bool(decision.veto) else ("apply" if bool(decision.apply) else "pass")
    reason = str(decision.reason_code or "NONE")
    mode = str(decision.mode or pol.mode or "unknown")
    _safe_inc(
        exec_health_decision_total,
        labels={"scope": sc, "symbol": sym, "mode": mode, "outcome": outcome, "reason": reason},
    )

    if bool(decision.apply):
        _safe_inc(exec_health_apply_total, labels={"symbol": sym, "mode": mode})
    if bool(decision.veto):
        _safe_inc(exec_health_veto_total, labels={"symbol": sym, "reason": reason})

    for flag in list(decision.flags or []):
        _safe_inc(exec_health_flag_total, labels={"scope": sc, "symbol": sym, "flag": str(flag)})

    add_bps = _f(getattr(decision, "tighten_add_bps", 0.0), 0.0)
    if add_bps > 0.0:
        _safe_observe(exec_health_tighten_add_bps_scoped, labels={"scope": sc, "symbol": sym}, value=add_bps)
        _safe_observe(exec_health_tighten_add_bps, labels={"symbol": sym}, value=add_bps)

    _safe_set(
        exec_health_tighten_k,
        labels={"scope": sc, "symbol": sym},
        value=_f(getattr(decision, "tighten_k_mult", 1.0), 1.0),
    )
