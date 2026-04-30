"""
news_pipeline/p6_metrics.py — Extended Prometheus metrics for P6.

Covers:
  - JSON repair success tracking (ratio via PromQL)
  - Overbudget call/USD counters
  - Stream pending (consumer group backlog) gauge
  - Budget used/limit gauges
  - DLQ total by reason
  - Cost USD total (conservative reserve-based)

These complement the metrics in news_pipeline/analyzer_service.py.
Import from here instead of redefining.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge

# ── LLM cost / budget ─────────────────────────────────────────────────────────

news_llm_cost_usd_total = Counter(
    "news_llm_cost_usd_total"
    "Estimated LLM cost in USD (conservative, reserve-based)"
    ["provider", "model"]
)

news_llm_overbudget_total = Counter(
    "news_llm_overbudget_total"
    "LLM calls rejected due to budget enforcement"
    ["kind"],   # kind ∈ {"calls", "usd"}
)

news_budget_calls_used = Gauge(
    "news_budget_calls_used"
    "LLM calls consumed today"
    []
)

news_budget_calls_limit = Gauge(
    "news_budget_calls_limit"
    "LLM calls/day hard cap"
    []
)

news_budget_usd_used = Gauge(
    "news_budget_usd_used"
    "LLM USD consumed today (reserve-adjusted)"
    []
)

news_budget_usd_limit = Gauge(
    "news_budget_usd_limit"
    "LLM USD/day hard cap"
    []
)

# ── JSON repair ───────────────────────────────────────────────────────────────

news_llm_repair_attempts_total = Counter(
    "news_llm_repair_attempts_total"
    "LLM repair prompt attempts (initial parse failed)"
    ["provider", "model"]
)

news_json_repair_success_total = Counter(
    "news_json_repair_success_total"
    "JSON repair successes (initial invalid → repair valid)"
    ["provider", "model"]
)

# ── DLQ ───────────────────────────────────────────────────────────────────────

news_dlq_total = Counter(
    "news_dlq_total"
    "Messages written to DLQ by reason"
    ["reason"]
)

# ── Stream backlog / lag ───────────────────────────────────────────────────────

news_stream_lag_ms = Gauge(
    "news_stream_lag_ms"
    "Approx stream lag (ms): last-entry-ms minus now"
    ["stream", "group"]
)

news_stream_pending_n = Gauge(
    "news_stream_pending_n"
    "Pending entries in consumer group (XINFO GROUPS pending)"
    ["stream", "group"]
)
