from prometheus_client import Counter, Gauge

sentiment_ctx_missing_total = Counter(
    "sentiment_ctx_missing_total"
    "Total times sentiment context was missing"
)

sentiment_ctx_stale_total = Counter(
    "sentiment_ctx_stale_total"
    "Total times sentiment context was stale"
)

sentiment_ctx_gate_monitor_hit_total = Counter(
    "sentiment_ctx_gate_monitor_hit_total"
    "Total monitor hits for sentiment gate"
    ["profile"]
)

sentiment_ctx_gate_tighten_total = Counter(
    "sentiment_ctx_gate_tighten_total"
    "Total tighten actions triggered by sentiment gate"
    ["reason"]
)

sentiment_risk_multiplier = Gauge(
    "sentiment_risk_multiplier"
    "Current applied sentiment risk multiplier"
)
