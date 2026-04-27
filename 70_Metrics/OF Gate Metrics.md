---
type: metrics
name: OF Gate Metrics
scope: gates
owners:
  - alex
tags:
  - metrics
  - gates
updated_at: 2026-04-18
---

# OF Gate Metrics

## Key metrics
- `signals_veto_total{reason_code,kind,symbol}`
- `allow_total`
- `diagnostic_publish_total`
- `reason_code_coverage_ratio`
- `spread_bps`
- `slippage_ema_bps`
- `book_age_ms`
- `atr_missing_total`
- `drift_detected_total`
- `smt_diverged_total`

## Required views
- veto reasons by symbol / regime / kind
- allow-vs-veto trend
- top changing reason codes after threshold changes
- spread and execution-cost overlays

## Alerts
- sharp rise in single veto reason across many symbols
- reason-code coverage drops
- diagnostic stream fails while vetoes continue
- cost gate blocks surge unexpectedly

## Rollout usage
Use this dashboard before and after every threshold change to validate intent vs effect.

## Links
- [[Pre-Publish Gates]]
- [[Gate Threshold Change]]
