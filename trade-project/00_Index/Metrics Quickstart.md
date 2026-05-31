---
type: index
title: Metrics Quickstart
tags: [index, metrics, observability]
updated_at: 2026-04-18
---

# Metrics Quickstart

## Read first
- [[Service SLOs]]
- [[Data Quality Metrics]]
- [[OF Gate Metrics]]
- [[ML Confirm Metrics]]
- [[Execution Metrics]]
- [[Redis Stream Health]]

## Golden dashboards
1. Traffic / throughput
2. Freshness / lag
3. Error rate
4. Gate pass / block / abstain
5. Execution latency and slippage
6. Stream lag and pending backlog

## Minimum panels by service
### Ingestion
- ws reconnects
- ticks published
- Redis write p95 / p99

### Orderflow runtime
- dropped ticks by reason
- symbol freeze state
- consumer lag
- runtime processing latency

### ML / gates
- p_edge distribution
- missing ratio
- block rate
- veto reasons

### Execution
- queue lag
- order send success / failure
- slippage bps
- mt5 / broker connectivity
