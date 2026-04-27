---
type: rollout
name: Rollback Policy
service: platform
status: ready
risk_level: high
tags:
  - rollout
  - rollback
updated_at: 2026-04-18
---

# Rollback Policy

## Principles
- rollback must be faster than investigation
- prefer reversible config toggles over code hotfixes
- preserve evidence before cleanup
- never hide failure by disabling metrics

## Standard rollback order
1. stop blast radius growth
2. revert trade-impacting path to safe mode
3. confirm metrics still flowing
4. preserve logs, payloads, and config snapshot
5. open incident and assign owner

## Safe rollback levers
- ML mode: `ENFORCE -> SHADOW -> OFF`
- publish path: `tradeable -> diagnostics-only`
- symbol-level disable
- queue consumer pause
- feature threshold reversion

## Unsafe rollback patterns
- deleting evidence streams before export
- changing multiple configs without snapshot
- disabling alerting while incident remains open

## Required evidence
- config before/after
- exact ts_ms of rollback
- affected symbols / streams / services
- reason codes and primary metrics around event

## Links
- [[Service SLOs]]
- [[Incidents Index]]
