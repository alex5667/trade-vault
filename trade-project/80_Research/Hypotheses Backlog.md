---
type: research_register
title: Hypotheses Backlog
tags: [research, hypothesis, backlog]
updated_at: 2026-04-18
---

# Hypotheses Backlog

## Status legend
- `idea`
- `ready`
- `running`
- `accepted`
- `rejected`
- `parked`

## Active hypotheses
| ID | Status | Hypothesis | Primary metric | Guardrail | Owner | Links |
|---|---|---|---|---|---|---|
| H-001 | ready | Dynamic `p_min` by symbol regime reduces false blocks without increasing slippage-adjusted drawdown | block precision / net pnl | block-rate shock | alex | [[ml-confirm-gate]] |
| H-002 | idea | Spread-aware breakout gating improves EV during thin conditions | pnl_bps net | missed winners | alex | [[pre-publish-gates]] |
| H-003 | ready | Per-symbol stream lag alarms reduce MTTR more than global lag alarms | MTTR | alert noise | alex | [[Redis Stream Health]] |
| H-004 | idea | Execution bridge queue cutover with per-broker idempotency key lowers duplicate opens | duplicate open rate | queue latency | alex | [[mt5-executor]] |

## Required fields for every hypothesis
- statement
- affected services
- metric of success
- metric of failure / guardrail
- replay dataset or live scope
- decision date
- rollback trigger
