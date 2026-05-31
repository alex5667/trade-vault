from __future__ import annotations

"""Prometheus metrics for inline execution-health (P1).

These metrics are intentionally low-cardinality. ``symbol`` is included because
P1 rollout is expected to start on 1–2 symbols. If you widen coverage, consider
sampling or reducing labels.
"""

from typing import Any

try:  # pragma: no cover - metrics are optional in tests
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore


class _Noop:
    def labels(self, *args: Any, **kwargs: Any) -> _Noop:
        return self
    def inc(self, *args: Any, **kwargs: Any) -> None:
        return None
    def set(self, *args: Any, **kwargs: Any) -> None:
        return None


if Counter is None or Gauge is None:  # pragma: no cover
    inline_exec_rollup_updates_total = _Noop()
    inline_exec_rollup_p95_bps = _Noop()
    inline_exec_rollup_count = _Noop()
    inline_exec_edge_tighten_total = _Noop()
    inline_exec_edge_veto_total = _Noop()
    inline_exec_edge_last_p95_bps = _Noop()
else:
    inline_exec_rollup_updates_total = Counter(
        "inline_exec_rollup_updates_total",
        "Total inline execution-health rollup updates written by tca_worker.",
        ["symbol", "side"],
    )
    inline_exec_rollup_p95_bps = Gauge(
        "inline_exec_rollup_p95_bps",
        "Latest inline implementation shortfall p95 in bps.",
        ["symbol", "side"],
    )
    inline_exec_rollup_count = Gauge(
        "inline_exec_rollup_count",
        "Latest bounded sample count for inline execution-health rollup.",
        ["symbol", "side"],
    )
    inline_exec_edge_tighten_total = Counter(
        "inline_exec_edge_tighten_total",
        "EdgeCostGate tighten decisions caused by inline execution-health.",
        ["symbol", "side"],
    )
    inline_exec_edge_veto_total = Counter(
        "inline_exec_edge_veto_total",
        "EdgeCostGate veto decisions caused by inline execution-health.",
        ["symbol", "side"],
    )
    inline_exec_edge_last_p95_bps = Gauge(
        "inline_exec_edge_last_p95_bps",
        "Latest inline execution-health p95 observed by EdgeCostGate.",
        ["symbol", "side"],
    )
