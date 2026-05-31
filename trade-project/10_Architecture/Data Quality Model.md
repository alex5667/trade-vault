---
type: architecture
title: Data Quality Model
project: trade
tags: [data-quality, dq, architecture]
updated_at: 2026-04-18
---

# Data Quality Model

## Purpose
Зафиксировать, какие классы плохих данных система умеет обнаруживать, как на них реагирует и где это влияет на сигнал.

## DQ dimensions
### Freshness
- данные не должны быть stale относительно configured thresholds

### Completeness
- critical fields must exist
- ATR / book / tick fields required for downstream decision

### Consistency
- side / price / qty / timestamps should satisfy contract

### Uniqueness
- duplicate ticks and duplicated execution intents must be suppressed

### Sequence integrity
- gaps / reorder beyond allowed window should be visible

### Source health
- WS reconnect storms
- consumer lag
- stream lag
- missing book updates

## Severity classes
### Informational
- sample-only anomaly
- low-frequency unknown side

### Soft degradation
- signal allowed, but flagged
- model or delta ignores uncertain input

### Hard veto
- missing ATR
- stale book
- critical tick gap
- impossible timestamp state

### Freeze / quarantine
- repeated time violations
- persistent source corruption

## DQ actions
- detect
- classify
- sanitize
- quarantine
- metric
- alert if actionable
- attach reason code to decision path

## Mandatory reason codes (starter set)
- `book_stale`
- `atr_unavailable`
- `tick_gap_critical`
- `tick_stale`
- `tick_future`
- `tick_duplicate`
- `unknown_side`
- `consumer_lag_high`
- `source_reconnect_storm`
- `bootstrap_not_ready`

## Where DQ is enforced
- preprocessing layer
- hard data quality gate
- optional model input masking
- execution block when signal prerequisites not satisfied

## DQ observability
Minimal metrics:
- freshness
- drop counts by reason
- dedupe counts
- quarantine counts
- freeze active symbols
- fraction of signals blocked by DQ
- fraction of signals allowed with degraded quality

## Operator posture
- page only on actionable production issues
- do not page on isolated sampled anomalies
- high stale ratio across multiple symbols is page-worthy
- repeated single-symbol freeze may be ticket-worthy, not page-worthy, depending on criticality

## Linked notes
- [[Time Model]]
- [[pre-publish-gates]]
- [[python-crypto-orderflow-service]]
