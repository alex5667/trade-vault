---
type: dto
dto: ML Decision
schema_ver: v1
tags:
  - contracts
  - dto
  - ml
updated_at: 2026-04-18
---

# ML Decision

## Purpose
Нормализованный результат ML confirm gate.

## Fields
- `mode`
- `allow`
- `should_enforce`
- `bucket`
- `p_edge`
- `p_min_used`
- `share_used`
- `model_ver`
- `err`
- `missing`
- `latency_ms`
- `abstain`
- `conf`
- `p_margin`
- `status`

## Status enum
- `ALLOW`
- `BLOCK`
- `ABSTAIN_BAND`
- `ABSTAIN_LOWCONF`
- `MISSING_FAILOPEN`
- `MISSING_FAILCLOSED`
- `SHADOW`
- `NO_ENFORCE`
- `OFF`

## Invariants
- `p_edge ∈ [0,1]`
- `allow ∈ {true,false}`
- `status` mandatory
- missing-model path explicitly observable

## Links
- [[ml-confirm-gate]]
- [[signals_of_confirm]]
