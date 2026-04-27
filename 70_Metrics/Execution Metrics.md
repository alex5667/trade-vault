---
type: metrics
name: Execution Metrics
scope: execution
owners:
  - alex
tags:
  - metrics
  - execution
updated_at: 2026-04-18
---

# Execution Metrics

## Key metrics
- `orders_published_total`
- `orders_ack_total`
- `orders_rejected_total{reason}`
- `duplicate_order_prevented_total`
- `ack_latency_ms`
- `fill_latency_ms`
- `slippage_bps`
- `slippage_ema_bps`
- `symbol_mapping_error_total`
- `paper_vs_live_mix`

## Required dashboards
- signal count vs execution count
- reject reasons over time
- slippage by symbol / venue / session
- ack latency p50/p95/p99
- live vs paper separation

## Alerts
- reject rate spike
- ack latency breaches budget
- duplicate prevention starts firing
- live orders appear on wrong venue/path
- slippage materially exceeds rolling expectation

## Links
- [[MT5 Executor]]
- [[Execution Bridge Cutover]]
- [[orders:queue:mt5]]
