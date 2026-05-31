---
type: adr
adr_id: ADR-0001
title: Redis Streams for reliable trading path
status: accepted
date: 2026-04-18
tags: [adr, redis, streams, reliability]
updated_at: 2026-04-18
---

# ADR-0001 Redis Streams for reliable trading path

## Context
The trading path needs replayability, lag visibility, consumer coordination, and bounded durability. Critical services read ticks, books, confirm inputs, and execution requests.

## Decision
Use Redis Streams + consumer groups for critical path delivery. Reserve Pub/Sub only for non-critical fan-out where message loss is acceptable.

## Consequences
### Positive
- replay and forensic debugging possible
- consumer lag measurable
- pending entries recoverable
- idempotency easier to reason about

### Negative
- retention and trimming require explicit policy
- stream lag dashboards and alarms are mandatory
- hot streams need maxlen discipline

## Operational notes
- every critical stream note must define idempotency key
- use per-stream health dashboards
- alarm on sustained lag, not single spikes

## Links
- [[Streams Index]]
- [[Redis Stream Health]]
- [[orders_queue_mt5]]
