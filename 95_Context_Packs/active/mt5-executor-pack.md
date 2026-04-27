---
type: context_pack
tags: [context-pack, generated, llm]
topic: "MT5 executor review"
source_notes:
  - 20_Services/mt5-executor.md
  - 30_Contracts/streams/orders_queue_mt5.md
  - 70_Metrics/Execution Metrics.md
updated_at: auto
---

# Context Pack: MT5 executor review

## Task
Подготовить compact context pack по MT5 execution bridge, order execution и связанным рискам.

## Summary
Auto-generated pack from selected notes. Review and tighten before sending to an external model.

## Relevant notes
- [[20_Services/mt5-executor.md]]
- [[30_Contracts/streams/orders_queue_mt5.md]]
- [[70_Metrics/Execution Metrics.md]]

## Key excerpts

### 20_Services/mt5-executor.md
```text
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
```
### 30_Contracts/streams/orders_queue_mt5.md
```text
# orders:queue:mt5

## Purpose
Очередь приказов в MT5 bridge для реального или paper-like исполнения через советник.

## Required fields
- `signal_id`
- `action`
- `symbol`
- `side`
- `sl_price`

## Recommended fields
- `entry_price`
- `tp1_price`
- `tp2_price`
- `risk_pct`
- `comment`

## Example
```json
{
  "signal_id": "4fac31a",
  "action": "OPEN",
  "symbol": "BTCUSD",
  "side": "BUY",
  "entry_price": 64500.5,
  "sl_price": 64000.0,
  "tp1_price": 65500.0,
  "risk_pct": 1.0
}
```

## Invariants
- idempotency key = `signal_id`
- no order without stop
- broker symbol mapping explicit
- execution reason-code must survive round-trip

## Reason codes
- `duplicate_signal`
- `invalid_symbol`
- `invalid_side`
- `missing_sl`
- `bridge_unavailable`
- `broker_requote`
- `broker_connection_error`

## Links
- [[mt5-executor]]
- [[signal-dispatch]]
```
### 70_Metrics/Execution Metrics.md
```text
# Execution Metrics

## Key metrics
- `orders_published_total`
- `orders_ack_total`
- `orders_rejected_total{reason}`
- `duplicate_order_prevented_total`
- `ack_latency_ms`
- `fill_latency_ms`
- `slippage_bps`
- `slippage_ema_bps`
- `symbol_mapping_error_total`
- `paper_vs_live_mix`

## Required dashboards
- signal count vs execution count
- reject reasons over time
- slippage by symbol / venue / session
- ack latency p50/p95/p99
- live vs paper separation

## Alerts
- reject rate spike
- ack latency breaches budget
- duplicate prevention starts firing
- live orders appear on wrong venue/path
- slippage materially exceeds rolling expectation

## Links
- [[MT5 Executor]]
- [[Execution Bridge Cutover]]
- [[orders:queue:mt5]]
```

## Ask for external model
Use only this context pack. Preserve contracts and invariants. Return:
- goal
- facts
- assumptions
- risks
- plan
- tests
- metrics/alerts
- rollout/rollback
