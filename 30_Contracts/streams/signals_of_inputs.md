---
type: stream
stream: signals:of:inputs
layer: ml-gate
transport: redis-streams
producer:
  - signal-dispatch
  - of-confirm-engine
consumer:
  - ml-confirm-gate
  - replay-jobs
schema_ver: v1
retention: medium
idempotency: key by sid
tags:
  - contracts
  - streams
  - ml
updated_at: 2026-04-18
---

# signals:of:inputs

## Purpose
Канонический вход для ML confirm / replay. Содержит rule-level decision context перед финальным confirm.

## Required fields
- `sid`
- `symbol`
- `ts_ms`
- `direction`
- `scenario`
- `rule_score`
- `rule_have`
- `rule_need`
- `ok_rule`
- `payload`

## Payload expectation
`payload` хранит JSON с индикаторами и подтверждениями, достаточный для offline replay без Redis.

## Invariants
- deterministic feature extraction
- enough fields for replayability
- one decision context per `sid`

## Reason codes
- `missing_payload`
- `schema_mismatch`
- `feature_missing`
- `replay_unusable`

## Links
- [[ml-confirm-gate]]
- [[ML Decision]]
- [[signal-dispatch]]
