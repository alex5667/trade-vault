---
type: research_register
title: AB Tests Register
tags: [research, ab-test, register]
updated_at: 2026-04-18
---

# AB Tests Register

## Standard policy
- deterministic sticky assignment
- same symbol/session must stay in same arm
- share and scope explicit
- winner criteria pre-declared
- stop criteria pre-declared

## Current A/B tests
| ID | Status | Arms | Allocation | Sticky key | Winner metric | Guardrails |
|---|---|---|---|---|---|---|
| AB-001 | design | champion vs challenger ML gate | 90/10 | symbol|session | net pnl after costs | missing ratio, latency p99 |
| AB-002 | design | spread gate old vs new | 95/5 | symbol|kind | profit factor | signal volume collapse |
| AB-003 | backlog | execution retry policy A vs B | 50/50 shadow metrics | signal_id | duplicate open rate | queue lag |

## Evaluation template
- sample size
- exposure by symbol
- by-scenario performance
- costs included
- slippage included
- operational incidents during test
- final decision linked in [[Decision Log]]
