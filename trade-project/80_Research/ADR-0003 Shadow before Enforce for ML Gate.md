---
type: adr
adr_id: ADR-0003
title: Shadow before Enforce for ML Gate
status: accepted
date: 2026-04-18
tags: [adr, ml, rollout, shadow, enforce]
updated_at: 2026-04-18
---

# ADR-0003 Shadow before Enforce for ML Gate

## Context
ML gate decisions can directly block trading opportunities. A misconfigured model, schema mismatch, or threshold error can cause invisible damage quickly.

## Decision
Any new ML model or threshold bundle must:
1. run in `SHADOW`,
2. produce metrics and counterfactual decisions,
3. pass operational checks,
4. only then move to `ENFORCE` under controlled share / symbol scope.

## Success criteria before ENFORCE
- missing ratio stable
- no schema mismatch
- latency p99 within budget
- no unexplained block-rate jump
- rollback switch validated

## Failure triggers
- high missing / error rate
- n_features mismatch
- cost-adjusted PnL worse in counterfactual review
- symbol-specific regression

## Links
- [[ml-confirm-gate]]
- [[ML Shadow to Enforce]]
- [[ML Confirm Metrics]]
