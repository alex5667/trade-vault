---
type: research_register
title: Experiments Register
tags: [research, experiment, register]
updated_at: 2026-04-18
---

# Experiments Register

## Current / recent experiments
| ID | Status | Scope | Method | Success metric | Guardrail | Notes |
|---|---|---|---|---|---|---|
| EXP-001 | planned | BTCUSDT, ETHUSDT | offline replay | pnl_bps net uplift | false positive rate | dynamic p_min by bucket |
| EXP-002 | planned | SOLUSDT, DOGEUSDT | shadow live | veto reason stability | latency p99 | spread-aware breakout gate |
| EXP-003 | running | all critical streams | dashboard + alert tuning | MTTR reduction | page volume | lag alarm rework |

## Experiment checklist
- research question written
- dataset / stream scope frozen
- config snapshot saved
- replay reproducible
- decision owner assigned
- result summarized in [[Decision Log]]
