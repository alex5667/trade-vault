---
type: dto
dto: Candidate
schema_ver: v1
tags:
  - contracts
  - dto
  - detector
updated_at: 2026-04-18
---

# Candidate

## Purpose
Базовый объект detector layer до финального scoring/confirm/publish.

## Fields
- `kind`
- `direction`
- `raw_score`
- `level_key`
- `reasons`
- `quality_flags`

## Invariants
- `direction ∈ {+1,-1}`
- `kind` from approved enum
- `raw_score` deterministic for same runtime snapshot
- quality flags append-only in pipeline

## Typical kinds
- `breakout`
- `absorption`
- `extreme`
- `obi_spike`

## Links
- [[detector-runtime]]
- [[pre-publish-gates]]
