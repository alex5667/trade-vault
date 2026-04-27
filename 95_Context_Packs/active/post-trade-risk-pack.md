---
type: context_pack
tags: [context-pack, generated, llm]
topic: "Post-trade risk review"
source_notes:
  - 70_Metrics/Execution Metrics.md
  - 40_Runbooks/atr-bad.md
updated_at: auto
---

# Context Pack: Post-trade risk review

## Task
Подготовить compact context pack по post-trade risk management, trailing, SLQ и slippage feedback.

## Summary
Auto-generated pack from selected notes. Review and tighten before sending to an external model.

## Relevant notes
- [[70_Metrics/Execution Metrics.md]]
- [[40_Runbooks/atr-bad.md]]

## Key excerpts

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
### 40_Runbooks/atr-bad.md
```text
# ATR Bad

## Symptoms
- `atr_unavailable`
- wide percentage of `ATR bad`
- stops not generated

## Fast checks
- verify ATR cache/source freshness
- compare current ATR vs recent history
- inspect missing candle gaps

## Likely causes
- HTF candles missing
- bad source selection
- reset after restart without bootstrap
- overly strict sanity thresholds

## Safe actions
- inspect ATR source freshness and fallback chain
- quarantine symbols with bad ATR
- keep execution closed until ATR valid

## Unsafe actions
- trading without stop model
- forcing constant ATR in volatile regime

## Metrics
- atr_age_ms
- atr_bad_pct
- atr_source_selected
- dq_veto_total{reason="atr_unavailable"}

## Links
- [[Time Model]]
- [[Data Quality Model]]
- [[pre-publish-gates]]
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
