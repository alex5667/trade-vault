---
type: service
title: mt5-executor
service: mt5-executor
language: mql5
criticality: critical
inputs: [orders:queue:mt5]
outputs: [broker_orders, fills, execution_logs]
source_paths:
  - scanner_infra/mt5/OrderExecutorAdvanced.mq5
tags: [mt5, execution, bridge, risk]
updated_at: 2026-04-18
---

# mt5-executor

## Purpose
Материализовать торговый signal в реальный order в MT5 / у брокера с контролем риска и явными retry rules.

## Responsibilities
- read execution queue
- parse order payload
- map symbols to broker notation
- calculate lot size from risk and stop distance
- place order
- handle requotes / connection errors
- set SL / TP
- log execution result

## Queue contract
Input stream:
- `orders:queue:mt5`

Required starter fields:
- `signal_id`
- `action`
- `symbol`
- `side`
- `entry_price`
- `sl_price`
- `tp1_price`
- `risk_pct`

## Risk rules
- lot sizing must depend on account balance and stop distance
- minimum / step / broker volume constraints must be respected
- no order without valid stop context
- retry only for explicit retriable errors
- idempotency by `signal_id` is mandatory

## Retriable classes
- requote
- temporary connection issue

## Non-retriable classes
- invalid volume
- invalid symbol
- malformed request
- broker rule rejection not marked transient

## Failure modes
- duplicate execution intent
- broker symbol mismatch
- invalid lot step rounding
- no SL / TP attached
- repeated requote storm
- MT5 terminal disconnected

## Metrics / logs
At minimum record:
- order attempts total
- order success total
- order failure total by retcode
- retry count
- average place latency
- rejected due to invalid payload
- duplicate signal ignored

## Alerts
- success rate collapse
- repeated connection retcodes
- repeated duplicate signal_id
- order latency spike
- MT5 terminal unavailable

## Rollout / rollback
### Rollout
- paper or demo first
- tiny risk_pct first
- verify symbol mapping
- verify lot calculation against broker rules
- test SL / TP placement end-to-end

### Rollback
- stop queue consumer
- keep logging and preserve raw order intents
- switch route to paper simulation if needed

## Linked notes
- [[signal-dispatch]]
- [[System Map]]
