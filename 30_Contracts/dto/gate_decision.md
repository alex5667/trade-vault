---
type: dto
dto: Gate Decision
schema_ver: v1
tags:
  - contracts
  - dto
  - gates
updated_at: 2026-04-18
---

# Gate Decision

## Purpose
Нормализованный verdict для любого pre-publish gate.

## Fields
- `decision`
- `reason`
- `flags`
- `gate_name`
- `tradeable`
- `ts_ms`

## Decision enum
- `ALLOW`
- `DENY`
- `SOFT_ALLOW`
- `TIGHTEN`

## Invariants
- `reason` always set on non-ALLOW path
- gate flags explain decision
- decision is auditable and replayable

## Common reasons
- `book_stale`
- `atr_unavailable`
- `tick_gap_critical`
- `spread_too_wide`
- `negative_ev`
- `smt_diverged`
- `drift_hard`

## Links
- [[pre-publish-gates]]
