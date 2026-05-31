# ADR-0001: Use Redis Streams for Market Data

## Status
Accepted

## Context
Our trade scanner requires a high-throughput, low-latency pipeline to ingest market data (ticks, klines, orderbook snapshots) from Go workers and distribute it to Python workers for signal generation. We previously considered Redis Pub/Sub, but it lacks persistence. If a Python worker restarts, network drops, or processes are too slow, data is permanently lost. This causes gaps in signal generation, non-deterministic backfills, and loss of replayability. We need a messaging system that guarantees at-least-once delivery, allows consumer groups for horizontal scaling, and provides a buffer for burst loads.

## Decision
We will use **Redis Streams** as the primary messaging bus for all market data events between Go collectors and Python signal workers, replacing standard Redis Pub/Sub.

## Alternatives considered
1. **Redis Pub/Sub**: Rejected. No persistence, data loss on disconnect, no concept of consumer tracking.
2. **Apache Kafka**: Rejected. High operational complexity, requires JVM tuning, excessive overhead for our current latency budgets, and overkill for our scale.
3. **RabbitMQ**: Rejected. Routing is powerful, but stream replayability is weaker than Redis Streams, and maintaining a separate broker infrastructure adds maintenance cost.

## Consequences
Positive:
- **Persistence & Replayability**: Events are stored in the stream, allowing us to replay data for backtesting and ML model validation.
- **Consumer Groups**: Multiple Python workers can process messages concurrently, supporting horizontal scaling.
- **Reliability**: If a consumer crashes, pending messages remain in the PEL (Pending Entries List) and can be reclaimed (XAUTOCLAIM).

Negative:
- **Memory Usage**: Redis Streams consume RAM. We must strictly configure `MAXLEN` to prevent OOM.
- **Complexity**: Handling XACK, consumer group creation, and PEL reclamation requires more complex code than a simple Pub/Sub listener.

## Risks
- **OOM (Out of Memory)**: Unbounded streams will crash the Redis node. Mitigation: Enforce strict `MAXLEN` policies (`~ 10000`) for hot data.
- **PEL Bloat**: Dead consumers will leave unacknowledged messages forever. Mitigation: Monitor `redis_stream_pending_count` and implement active PEL reclamation.

## Observability
Metrics:
- `redis_stream_len`: Monitor the total length of the stream.
- `redis_stream_pending_count`: Ensure messages are being acknowledged.
- `redis_stream_consumer_idle_ms`: Detect dead consumers.

Logs:
- Log `XAUTOCLAIM` events to track consumer instability.
- Log failures to acknowledge (`XACK` errors).

Alerts:
- `RedisPendingGrowing`: Page if pending count > threshold for 5m.
- `StreamConsumerDead`: Page if consumer idle time > 2m.

## Rollout
- Implement Go producer using `XADD` with `MAXLEN ~` limit.
- Create Python consumer groups and transition from Pub/Sub to `XREADGROUP`.
- Rollout in shadow mode for one symbol first, compare latency, then full enforce.

## Rollback
- Revert Go worker to use `PUBLISH`.
- Revert Python worker to use `PSUBSCRIBE`.
