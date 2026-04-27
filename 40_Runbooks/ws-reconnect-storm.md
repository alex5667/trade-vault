---
type: runbook
name: WS Reconnect Storm
severity: high
service: go ingestion
trigger: reconnect rate high
tags:
  - runbook
  - websocket
  - ingestion
updated_at: 2026-04-18
---

# WS Reconnect Storm

## Symptoms
- reconnect counters rising fast
- missing ticks/books
- bursty backfill activity

## Fast checks
- inspect exchange connectivity
- verify DNS / TLS / proxy issues
- compare one symbol vs all symbols

## Likely causes
- exchange instability
- network path issue
- handshake/read timeout too aggressive
- process resource exhaustion

## Safe actions
- verify backoff works
- isolate failing worker/timeframe
- confirm nofile / socket exhaustion
- reduce symbol set temporarily if needed

## Unsafe actions
- disable reconnect backoff
- flood exchange with reconnect loops

## Metrics
- ws_reconnects_total
- handshake_failures_total
- backfill_requests_total
- tick_publish_rate

## Links
- [[go-worker-ingestion]]
