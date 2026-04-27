---
type: service
title: signal-dispatch
service: signal-dispatch
language: python
criticality: high
inputs: [tradeable_signal, diagnostics, notify_events]
outputs: [signals:crypto:raw, orders:queue, orders:queue:mt5, notify:telegram]
source_paths:
  - python-worker/services/async_signal_publisher.py
tags: [python, dispatch, redis, outbox, dedup]
updated_at: 2026-04-18
---

# signal-dispatch

## Purpose
Собрать финальный signal payload, защитить систему от дублей и корректно разложить сообщение по downstream consumers.

## Responsibilities
- produce stable `signal_id`
- semantic dedup
- publish to raw streams
- route to execution queue
- route to telegram notify stream
- route diagnostics separately
- do safe xadd with retry path

## Signal payload starter contract
Required fields:
- `signal_id`
- `symbol`
- `kind`
- `side`
- `entry_price`
- `sl_price`
- `tp1_price`
- `confidence`
- `ts_ms`
- `venue`
- `source`
- `meta{}`

## Dedup model
### Cooldown dedup
- by symbol / side / kind
- prevents repeated market idea spam

### Semantic dedup
- stable hash over key fields
- bucketed by time window
- used for replay-safe identity

## Streams
### Tradeable path
- `signals:crypto:raw`
- `orders:queue`
- `orders:queue:mt5`

### Notify path
- `notify:telegram`

### Diagnostic path
- `stream:signals:diagnostics`

## Failure modes
- duplicate orders from missed dedup
- Redis xadd failures
- diagnostics mixed into tradeable path
- telegram spam
- unstable signal_id across equivalent events
- missing payload fields in downstream executor

## Metrics
- published total by stream
- dedup hit total
- publish errors total
- retry queue depth
- notify sent / skipped total
- diagnostic publish total
- raw-to-execution ratio

## Alerts
- orders published without matching raw event
- sudden dedup collapse
- retry queue saturation
- notify stream failures
- missing mandatory payload fields

## Rollout / rollback
### Rollout
- verify signal_id stability on replay
- test semantic dedup under burst
- confirm diagnostics and tradeable streams separated

### Rollback
- reduce execution routing first
- keep raw stream publication for visibility
- disable notify if noisy, but preserve execution path if safe

## Linked notes
- [[pre-publish-gates]]
- [[mt5-executor]]
- [[System Map]]
