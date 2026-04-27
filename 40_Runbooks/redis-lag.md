---
type: runbook
name: Redis Lag
severity: high
service: signal pipeline
trigger: consumer lag / stream growth
tags:
  - runbook
  - redis
  - latency
updated_at: 2026-04-18
---

# Redis Lag

## Symptoms
- stream length rapidly grows
- consumer lag increases
- signal freshness degrades
- publish latency spikes

## Fast checks
```bash
redis-cli XLEN stream:tick_BTCUSDT
redis-cli XINFO GROUPS stream:tick_BTCUSDT
redis-cli XPENDING stream:tick_BTCUSDT <group>
```

## Likely causes
- slow consumer
- network/storage hiccup
- restart storm
- blocked downstream publish path

## Safe actions
- confirm which consumer/group lags
- scale or restart only failing consumer
- reduce noisy diagnostics if they saturate Redis
- verify retention / MAXLEN policy

## Unsafe actions
- deleting active streams blindly
- ACKing pending entries without root cause
- restarting all pipeline stages at once

## Metrics
- consumer_lag_ms
- stream_xlen
- publish_latency_ms
- freshness_ms

## Links
- [[python-crypto-orderflow-service]]
- [[signal-dispatch]]
