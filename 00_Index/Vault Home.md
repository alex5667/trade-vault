---
type: index
title: Vault Home
tags: [index, navigation, home]
updated_at: 2026-04-18
---

# Vault Home

## Start here
- [[System Map]]
- [[Pipeline Overview]]
- [[Time Model]]
- [[Data Quality Model]]

## Core navigation
- [[Services Index]]
- [[Streams Index]]
- [[Env Index]]
- [[Runbooks Index]]
- [[Metrics Quickstart]]
- [[Decision Log Index]]
- [[Research Index]]
- [[ADR Index]]
- [[Dashboard Home]]
- [[Views Home]]

## Operating flow
1. Ingestion → [[go-worker-ingestion]]
2. Preprocessing → [[python-crypto-orderflow-service]]
3. Detection → [[detector-runtime]]
4. ML / gates → [[ml-confirm-gate]] + [[pre-publish-gates]]
5. Dispatch → [[signal-dispatch]]
6. Execution → [[mt5-executor]]
7. Risk / feedback → [[Execution Metrics]] + [[Service SLOs]]

## Critical invariants
- epoch `ts_ms` everywhere
- stale / future / duplicate / gap detection required
- idempotent publish and execution
- replayability for candidate → gate → dispatch path
- explicit reason codes and veto reasons
- production changes require rollout + rollback notes

## Fast paths
### I need the payload / contract
- [[signal_payload]]
- [[candidate]]
- [[ml_decision]]
- [[gate_decision]]
- [[orders_queue_mt5]]
- [[signals_of_confirm]]

### I need an ops procedure
- [[Runbooks Index]]
- [[Rollback Policy]]
- [[ML Shadow to Enforce]]
- [[Incident Review Queue]]

### I need research history
- [[Hypotheses Backlog]]
- [[Experiments Register]]
- [[AB Tests Register]]
- [[Decision Log]]
- [[ADR Index]]
- [[Weekly Research Review]]

## Dashboards
- [[Dashboard Home]]
- [[Ops Control Center]]
- [[Incidents Dashboard]]
- [[Rollouts Dashboard]]
- [[ADR Dashboard]]
- [[Runbooks Dashboard]]
- [[Metrics Coverage Dashboard]]
- [[Services Coverage Dashboard]]

## Views / review cadence
- [[Views Home]]
- [[Weekly Ops Review]]
- [[Weekly Research Review]]
- [[Weekly Governance Review]]
- [[Incident Review Queue]]
- [[Experiment Review Queue]]
- [[Rollout Follow-up Queue]]
- [[Kanban Setup]]

## Automation
- [[Automation Index]]
- [[Naming Rules]]
- [[Note Lint Checklist]]
- [[Export Policy]]
- [[Vault Workflow]]
