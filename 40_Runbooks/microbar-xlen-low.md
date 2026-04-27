---
type: runbook
name: Microbar XLEN Low
severity: medium
service: feature pipeline
trigger: microbar history below required length
tags:
  - runbook
  - features
  - history
updated_at: 2026-04-18
---

# Microbar XLEN Low

## Symptoms
- low history warnings
- missing indicators / weak features
- confirm gate abstains more often

## Fast checks
- inspect stream/history length
- verify retention / MAXLEN
- verify bootstrap/replay source

## Likely causes
- retention too small
- recent restart with cold state
- symbol newly onboarded
- consumer falling behind and trimming races

## Safe actions
- increase retention or bootstrap horizon
- keep symbol in warmup mode
- disable live trading for underfilled history

## Unsafe actions
- treating partially warmed history as production-ready
- force-enabling signals on cold start

## Metrics
- microbar_xlen
- feature_missing_n
- warmup_state
- abstain_rate

## Links
- [[python-crypto-orderflow-service]]
- [[ml-confirm-gate]]
