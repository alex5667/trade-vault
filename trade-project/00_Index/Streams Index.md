---
type: index
title: Streams Index
tags: [index, streams, redis]
updated_at: 2026-04-18
---

# Streams Index

## Market data
- [[stream_tick_symbol]] — raw trade ticks per symbol
- [[stream_book_symbol]] — order book snapshots per symbol

## ML / signal pipeline
- [[signals_of_inputs]] — inputs to confirm / replay layer
- [[signals_of_confirm]] — confirmed output with decision metadata

## Execution / notification
- [[orders_queue_mt5]] — execution queue for MT5 bridge
- [[notify_telegram]] — operator / bot notifications

## What every stream note must document
- producer(s)
- consumer(s)
- required fields
- retention / maxlen policy
- idempotency key
- replay usage
- observability and lag signals

## Cross-links
- [[System Map]]
- [[signal-dispatch]]
- [[mt5-executor]]
- [[Redis Stream Health]]
