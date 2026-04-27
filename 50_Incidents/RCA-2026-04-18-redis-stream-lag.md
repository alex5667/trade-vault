---
type: incident
id: RCA-2026-04-18-redis-stream-lag
severity: SEV2
service: redis-streams
status: template-filled
tags:
  - incident
  - rca
  - redis
  - lag
updated_at: 2026-04-18
---

# RCA-2026-04-18-redis-stream-lag

## Summary
Consumer lag or write-side saturation in Redis Streams causes delayed processing across ingestion, confirm, or execution queues.

## Impact
- delayed candidate generation
- stale books / stale ticks at consumers
- increased execution latency
- possible out-of-date signals reaching downstream systems

## Facts
- Redis Streams are the reliability path for core event transport
- Lag should be measured in both message count and time/freshness
- Actionable paging should trigger only when lag threatens decisions or execution timing

## Assumptions
- pool exhaustion
- consumer crash or restart storm
- oversized batch or slow handler
- storage pressure / fsync / persistence issue

## Detection
- rising stream lag time
- pending entries growth
- consumer idle time anomalies
- increased publish latency p95/p99

## Timeline
- identify stream and consumer group
- inspect `XPENDING`, `XINFO GROUPS`, `XLEN`
- correlate with CPU/memory/network on Redis node
- inspect slow consumer service logs

## Root cause
Transport layer lost freshness budget because one or more producers/consumers could not keep up with sustained event volume.

## Contributing factors
- no freshness-based SLO tied to alerting
- too few per-stream dashboards
- insufficient backpressure signaling to upstream services

## Corrective actions
1. Add per-stream freshness SLO
2. Add lag dashboard per stream/group/consumer
3. Cap producer burst or shard streams where needed
4. Ensure consumers expose handler latency and retry metrics

## Prevention
- load test hot streams before rollout
- retention and maxlen sizing review
- recoverability drills for consumer group lag

## Linked docs
- [[Redis Lag]]
- [[Redis Stream Health]]
- [[Service SLOs]]
