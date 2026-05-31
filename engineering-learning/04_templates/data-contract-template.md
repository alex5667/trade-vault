# Event Contract: Name v1

## Producer
...

## Consumers
...

## Schema
```json
{
}
```

## Time policy
- Exchange timestamps: epoch ms.
- Internal timestamps: epoch ms.
- UI formatting: user timezone only at presentation layer.
- Reject/quarantine if event_time_ms is in the future beyond allowed skew.

## Bad data policy
Detect → sanitize/quarantine → metric → reason code.

## Idempotency
Key: `source:symbol:timeframe:event_time_ms`.

## Replay
Raw event must be replayable from Redis/Timescale without changing derived signal output.
