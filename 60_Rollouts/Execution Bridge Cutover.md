---
type: rollout
name: Execution Bridge Cutover
service: mt5-executor
status: ready
risk_level: high
tags:
  - rollout
  - execution
  - mt5
updated_at: 2026-04-18
---

# Execution Bridge Cutover

## Goal
Cut over execution path or materially change execution settings without causing duplicate orders, symbol mismatch, or unbounded live risk.

## Scope
- `orders:queue:mt5`
- MT5 bridge / advisor
- broker symbol mapping
- paper vs live separation

## Preconditions
- idempotency key = `signal_id` verified
- symbol mapping validated on target broker
- order sizing formula and minimum lot rules checked
- paper/live credentials isolated
- rollback path to paper or paused queue confirmed

## Rollout steps
1. verify queue payload contract
2. dry-run on paper / demo path
3. enable limited live cutover with single symbol or small risk budget
4. observe fills, slippage, and duplicate prevention
5. expand only when stable

## Abort criteria
- duplicate or missing execution acknowledgements
- wrong symbol / wrong side / bad SL-TP placement
- slippage or reject rate breaches target

## Rollback
- disable live queue consumer
- revert to previous bridge
- preserve outbound order evidence for replay/audit

## Post-rollout verification
- compare signal count vs execution count
- verify fill timestamps and latency
- review reject reason codes

## Links
- [[MT5 Executor]]
- [[orders:queue:mt5]]
- [[Execution Metrics]]
