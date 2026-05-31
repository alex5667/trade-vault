---
type: service
title: go-worker-ingestion
service: go-worker-ingestion
language: go
criticality: high
inputs: [exchange_websocket, exchange_rest_backfill]
outputs: [stream:tick_<symbol>, stream:book_<symbol>, candles:data]
source_paths:
  - scanner_infra/go-workers
tags: [go, ingestion, websocket, redis]
updated_at: 2026-04-18
---

# go-worker-ingestion

## Purpose
Получать market data с биржи с минимальной задержкой и публиковать её в Redis Streams в контрактном формате.

## Responsibilities
- manage WebSocket lifecycle
- subscribe to trades / depth / kline streams
- reconnect with backoff
- do REST backfill after gaps / disconnects
- filter symbols
- publish to Redis with bounded latency
- expose Prometheus metrics

## Inputs
- exchange WebSocket frames
- REST backfill responses
- ENV symbol filters / stream config

## Outputs
- `stream:tick_<symbol>`
- `stream:book_<symbol>`
- `candles:data`

## Hot path
1. receive WS event
2. parse
3. normalize fields
4. optionally enrich with symbol metadata
5. publish to Redis
6. metric observe

## State
- current WS connections
- subscribed symbols
- reconnect backoff state
- Redis pool
- health / counters

## Failure modes
- WS reconnect storm
- REST backfill timeout
- Redis write latency spike
- symbol explosion / too many pairs
- too many open files
- malformed payload from source

## ENV to document later
- timeframe / stream settings
- WS timeouts
- ping / keepalive
- REST backfill timeout
- min trades filter
- Redis pool size
- retry backoff

## Required metrics
- ticks published total
- books published total
- redis write latency p50/p95/p99
- ws reconnects total
- rest backfill count / failures
- active symbols
- parse errors
- dropped source messages

## Alerts
- reconnect storm
- Redis write p99 too high
- publish rate collapse on critical symbols
- no data for critical symbol beyond threshold
- open file / socket exhaustion risk

## Rollout / rollback
### Rollout
- add symbols gradually
- watch publish rate and Redis write latency
- verify backfill path before full traffic

### Rollback
- reduce symbol list
- disable expensive streams first
- revert ENV causing handshake/read timeouts

## Linked notes
- [[System Map]]
- [[Pipeline Overview]]
- [[python-crypto-orderflow-service]]
