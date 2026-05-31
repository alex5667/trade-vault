---
type: metrics
name: Service SLOs
scope: platform
owners:
  - alex
tags:
  - metrics
  - slo
updated_at: 2026-04-18
---

# Service SLOs

## SLO philosophy
Use freshness-aware SLOs across the pipeline, not only uptime. A service can be technically up but operationally stale.

## Recommended service-level objectives

### Ingestion
- websocket continuity healthy for hot symbols
- publish freshness within target budget
- reconnect storms remain rare and short

### Preprocessing
- tick processing latency p95/p99 within budget
- stale/future/duplicate drop ratios below thresholds
- freeze/quarantine not persistent for major symbols

### Detection / confirm / gates
- decision latency p95/p99 stable
- reason-code coverage near 100%
- missing/abstain/error ratios bounded

### Execution
- signal-to-order conversion traceable
- order ack latency within venue budget
- reject/duplicate rate near zero

### Post-trade analytics
- trade state updates fresh
- slippage metrics recorded consistently
- closed-trade persistence complete

## Core dimensions
- service
- symbol
- regime
- scenario
- stream
- consumer group

## Alerting policy
Page only when SLO breach requires operator action and risks live decisions, execution, or forensic visibility.

## Links
- [[Data Quality Metrics]]
- [[Execution Metrics]]
- [[Redis Stream Health]]
