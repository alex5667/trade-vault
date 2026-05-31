---
type: stream
stream: signals:of:confirm
layer: ml-gate
transport: redis-streams
producer:
  - ml-confirm-gate
consumer:
  - signal-dispatch
  - analytics
  - replay-jobs
schema_ver: v3
retention: medium
idempotency: key by sid
tags:
  - contracts
  - streams
  - confirm
updated_at: 2026-04-18
---

# signals:of:confirm

## Purpose
Финальный confirm result после rules + ML layer.

## Required fields
- `sid`
- `symbol`
- `ts_ms`
- `direction`
- `scenario`
- `ok`
- `score`
- `have`
- `need`
- `reason`

## Recommended fields
- `evidence`
- `contrib`
- `gate_bits`
- `ml`
- `latency_us`

## Example
```json
{
  "sid": "abc123",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000000,
  "direction": "BUY",
  "scenario": "continuation",
  "ok": 1,
  "score": 0.78,
  "have": 3,
  "need": 2,
  "reason": "above_p_min"
}
```

## Invariants
- deterministic per identical input
- `ok ∈ {0,1}`
- `score ∈ [0,1]`
- `reason` mandatory
- evidence compact and replay-friendly

## Reason codes
- `below_hard_floor`
- `below_p_min`
- `abstain_band`
- `missing_failopen`
- `missing_failclosed`
- `n_features_mismatch`

## Links
- [[ml-confirm-gate]]
- [[signal-dispatch]]
- [[ML Decision]]
