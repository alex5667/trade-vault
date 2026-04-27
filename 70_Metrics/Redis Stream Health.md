---
type: metrics
name: Redis Stream Health
scope: redis
owners:
  - alex
tags:
  - metrics
  - redis
updated_at: 2026-04-18
---

# Redis Stream Health

## Core metrics
- `xlen`
- `xpending`
- consumer idle time
- handler latency
- publish latency
- freshness lag ms
- retry queue depth
- redis pool utilization / connection errors

## Streams to watch
- `stream:tick_<symbol>`
- `stream:book_<symbol>`
- `signals:of:inputs`
- `signals:of:confirm`
- `orders:queue:mt5`
- `notify:telegram`

## Required views
- lag in messages and lag in time
- pending growth by consumer group
- hot streams by throughput
- publish latency vs consume latency

## Alerts
- freshness lag breaches SLO on hot streams
- pending entries grow without recovery
- producer publish latency spikes
- retry queue depth rises persistently

## Links
- [[Redis Lag]]
- [[RCA-2026-04-18-redis-stream-lag]]
- [[Service SLOs]]
