from __future__ import annotations

"""Prometheus metrics registry for ExecHealth observability.

Design goals
------------
- One registry module shared by all ExecHealth consumers.
- Low-cardinality labels only: {scope, symbol, metric, mode, reason, flag, where}.
- Backward-compatible legacy counters/histogram kept for existing queries.
"""


import logging
from collections.abc import Sequence
from typing import TypeVar

try:
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram  # type: ignore
    from prometheus_client.registry import Collector  # type: ignore
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = object  # type: ignore
    REGISTRY = None  # type: ignore
    Collector = object  # type: ignore

logger = logging.getLogger("orderflow_metrics_exec_health")

TCollector = TypeVar("TCollector", bound="Collector")  # type: ignore


def _get_or_create[TCollector: "Collector"](  # type: ignore
    name: str,
    ctor: type[TCollector],
    documentation: str,
    labelnames: Sequence[str] = (),
    **kwargs,
):
    if REGISTRY is None:  # pragma: no cover
        return None
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        return existing
    try:
        return ctor(name, documentation, labelnames=tuple(labelnames), **kwargs)
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to create metric %s: %s", name, exc)
        return None


# ---------------------------------------------------------------------------
# Legacy metrics (keep names stable for backward compatibility).
# ---------------------------------------------------------------------------
exec_health_apply_total = _get_or_create(
    "exec_health_apply_total",
    Counter,
    "ExecutionHealthGate applied (legacy aggregate without scope)",
    labelnames=("symbol", "mode"),
)

exec_health_veto_total = _get_or_create(
    "exec_health_veto_total",
    Counter,
    "ExecutionHealthGate veto count (legacy aggregate without scope)",
    labelnames=("symbol", "reason"),
)

exec_health_tighten_add_bps = _get_or_create(
    "exec_health_tighten_add_bps",
    Histogram,
    "Added slippage (bps) due to execution health tighten (legacy aggregate without scope)",
    labelnames=("symbol",),
    buckets=(0.5, 1, 2, 3, 4, 6, 8, 10, 15),
)


# ---------------------------------------------------------------------------
# Scoped SoT observability metrics.
# ---------------------------------------------------------------------------
exec_health_decision_total = _get_or_create(
    "exec_health_decision_total",
    Counter,
    "ExecHealth decisions by scope/mode/outcome/reason",
    labelnames=("scope", "symbol", "mode", "outcome", "reason"),
)

exec_health_flag_total = _get_or_create(
    "exec_health_flag_total",
    Counter,
    "ExecHealth flag hits by scope/symbol/flag",
    labelnames=("scope", "symbol", "flag"),
)

exec_health_reader_errors_total = _get_or_create(
    "exec_health_reader_errors_total",
    Counter,
    "ExecHealth rollup/telemetry read errors",
    labelnames=("scope", "where"),
)

exec_health_rollup_value_bps = _get_or_create(
    "exec_health_rollup_value_bps",
    Gauge,
    "Latest ExecHealth rollup value in bps",
    labelnames=("scope", "symbol", "metric"),
)

exec_health_rollup_present = _get_or_create(
    "exec_health_rollup_present",
    Gauge,
    "Whether a given ExecHealth rollup metric was present (1/0)",
    labelnames=("scope", "symbol", "metric"),
)

exec_health_rollup_worst_delta_sec = _get_or_create(
    "exec_health_rollup_worst_delta_sec",
    Gauge,
    "Delta window (sec) that produced the worst-case multi-delta rollup",
    labelnames=("scope", "symbol", "metric"),
)

exec_health_policy_threshold_bps = _get_or_create(
    "exec_health_policy_threshold_bps",
    Gauge,
    "Configured ExecHealth threshold in bps by scope/metric",
    labelnames=("scope", "metric"),
)

exec_health_policy_mode = _get_or_create(
    "exec_health_policy_mode",
    Gauge,
    "ExecHealth effective mode one-hot by scope/mode",
    labelnames=("scope", "mode"),
)

exec_health_tighten_add_bps_scoped = _get_or_create(
    "exec_health_tighten_add_bps_scoped",
    Histogram,
    "Added slippage (bps) due to execution health tighten by scope",
    labelnames=("scope", "symbol"),
    buckets=(0.5, 1, 2, 3, 4, 6, 8, 10, 15),
)

exec_health_tighten_k = _get_or_create(
    "exec_health_tighten_k",
    Gauge,
    "Current ExecHealth tighten_k multiplier",
    labelnames=("scope", "symbol"),
)

exec_health_last_event_ts_ms = _get_or_create(
    "exec_health_last_event_ts_ms",
    Gauge,
    "Last ExecHealth event timestamp in epoch ms",
    labelnames=("scope", "symbol"),
)

# ---------------------------------------------------------------------------
# P6: Hard freeze hook metrics (consumer-side enforcement of autoguard key).
# ---------------------------------------------------------------------------
exec_health_freeze_hook_active = _get_or_create(
    "exec_health_freeze_hook_active",
    Gauge,
    "Whether ExecHealth auto-freeze hook is currently active in this consumer scope",
    labelnames=("scope",),
)

exec_health_freeze_hook_freeze_until_ts_ms = _get_or_create(
    "exec_health_freeze_hook_freeze_until_ts_ms",
    Gauge,
    "Freeze-until timestamp observed by ExecHealth hard consumer hook",
    labelnames=("scope",),
)

exec_health_freeze_hook_state_age_seconds = _get_or_create(
    "exec_health_freeze_hook_state_age_seconds",
    Gauge,
    "Age of freeze state payload observed by ExecHealth consumer hook",
    labelnames=("scope",),
)

exec_health_freeze_hook_block_total = _get_or_create(
    "exec_health_freeze_hook_block_total",
    Counter,
    "How many publish/entry attempts were blocked by ExecHealth auto-freeze hard hook",
    labelnames=("scope", "reason"),
)

exec_health_freeze_hook_reader_errors_total = _get_or_create(
    "exec_health_freeze_hook_reader_errors_total",
    Counter,
    "Redis/key read errors while checking ExecHealth auto-freeze hard hook",
    labelnames=("scope", "where"),
)
