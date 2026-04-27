---
type: incident
id: RCA-2026-04-18-ws-reconnect-storm
severity: SEV2
service: go-worker-ingestion
status: template-filled
tags:
  - incident
  - rca
  - websocket
  - ingestion
updated_at: 2026-04-18
---

# RCA-2026-04-18-ws-reconnect-storm

## Summary
Go ingestion workers enter repeated reconnect loops, causing intermittent data gaps, backfill pressure, and freshness degradation.

## Impact
- symbols flap between healthy and degraded
- increased backfill usage
- duplicated or gapped market data risk
- downstream churn in DQ, detector, and gate layers

## Facts
- reconnect with backoff is expected during transient failures
- reconnect storm becomes incident when repeated disconnects consume freshness budget or create gaps
- low-latency path depends on stable websocket continuity

## Assumptions
- upstream exchange instability
- TCP keepalive / read timeout tuning mismatch
- local network or DNS instability
- per-symbol overload or too many subscriptions on a worker

## Detection
- `ws_reconnects_total` slope spikes
- book/tick publish rate oscillates
- downstream stale/gap metrics rise
- backfill requests surge

## Timeline
- confirm scope: one symbol / one worker / many workers
- compare exchange status vs local network telemetry
- inspect reconnect backoff progression
- decide whether to reduce symbol load / shard workers

## Root cause
Continuous websocket churn broke the normal freshness assumptions of the pipeline.

## Contributing factors
- shared workers may carry too many hot symbols
- dashboards focus on reconnect count but not freshness impact
- no explicit runbook threshold for cutover / shard / temporary disable

## Corrective actions
1. Alert on reconnect rate + freshness combination
2. Review worker sharding and symbol assignment
3. Add decision rule for temporary symbol quarantine during sustained reconnect storm
4. Record reconnect reason codes when available

## Prevention
- capacity review for hot workers
- network health checks on deployment nodes
- canary symbol load before broad symbol adds

## Linked docs
- [[WS Reconnect Storm]]
- [[Go Worker Ingestion]]
- [[New Symbol Onboarding]]
- [[Data Quality Metrics]]
