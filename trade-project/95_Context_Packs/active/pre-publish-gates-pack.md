---
type: context_pack
tags: [context-pack, generated, llm]
topic: "Pre-publish gates review"
source_notes:
  - 20_Services/pre-publish-gates.md
  - 40_Runbooks/stale-book.md
  - 70_Metrics/Data Quality Metrics.md
updated_at: auto
---

# Context Pack: Pre-publish gates review

## Task
Подготовить compact context pack по pre-publish gates, DQ/Regime/Drift/SMT/Edge Cost gate.

## Summary
Auto-generated pack from selected notes. Review and tighten before sending to an external model.

## Relevant notes
- [[20_Services/pre-publish-gates.md]]
- [[40_Runbooks/stale-book.md]]
- [[70_Metrics/Data Quality Metrics.md]]

## Key excerpts

### 20_Services/pre-publish-gates.md
```text
# pre-publish-gates

## Purpose
Финальный chain-of-responsibility слой перед тем, как candidate становится боевым сигналом.

## Gate families
### Hard Data Quality Gate
Блокирует signal при проблемах входных данных:
- stale book
- missing ATR
- critical tick gap

### Regime / Session Gate
Проверяет совместимость candidate kind с текущим market regime.

### Feature Drift Gate
Обнаруживает drift текущего рынка от distributions, на которых model / thresholds были калиброваны.

### SMT Coherence Gate
Проверяет, не идёт ли signal против лидеров сектора с высокой уверенностью.

### Edge Cost Gate
Сравнивает expected edge против fees + spread + expected slippage.

### Min Interval Gate
Защищает от signal spam на одном и том же паттерне.

## Decision contract
- `allow: bool`
- `veto_reason: str`
- `flags: dict`
- `tradeable: bool`
- `diagnostic_payload` when blocked

## Non-negotiable rules
- каждый veto обязан иметь reason code
- veto reasons должны быть агрегируемыми по Prometheus / analytics
- gates order must be explicit and stable
- if a gate mutates context, it must be documented
- diagnostics stream must never be treated as tradeable path

## Starter reason codes
- `book_stale`
- `atr_unavailable`
- `tick_gap_critical`
- `kind_not_allowed_for_regime`
- `chop_weak_obi`
- `feature_drift`
- `smt_diverged`
- `negative_ev`
- `spread_too_wide`
- `min_interval`

## Failure modes
- overblocking because thresholds too strict
- inconsistent reason codes
- drift gate false positives
- missing leader signal cache
- execution cost assumptions stale
- diagnostic stream missing

## Metrics
- veto total by reason
- pass rate by symbol / kind
- gate latency
- edge cost veto rate
- drift veto rate
- regime mismatch rate
- DQ veto rate

## Alerts
- block rate spike after rollout
- missing diagnostics on veto
- wide-spread veto spike on majors
- sudden drift veto across market regime switch

## Rollout / rollback
### Rollout
- ship new gate in shadow-like observe mode if possible
- compare veto rates by symbol
- verify reason codes appear in metrics and diagnostics

### Rollback
- disable latest gate
- revert threshold to known good baseline
- keep counters for retrospective analysis

## Linked notes
- [[Data Quality Model]]
- [[ml-confirm-gate]]
- [[signal-dispatch]]
```
### 40_Runbooks/stale-book.md
```text
# Stale Book

## Symptoms
- repeated `book_stale` veto
- OBI / depth metrics freeze
- execution disabled by gates

## Fast checks
- compare `now_ms - book_ts_ms`
- inspect book_rate_hz
- inspect websocket reconnect counters

## Likely causes
- book stream stalled
- exchange depth feed degraded
- parser stopped updating runtime
- clock skew

## Safe actions
- verify `stream:book_<symbol>` receiving messages
- compare exchange event time vs ingest time
- isolate one symbol before broader restart

## Unsafe actions
- overriding stale gate in production
- using last known book for live execution

## Metrics
- book_age_ms
- book_rate_hz
- ws_reconnects_total
- gate_veto_total{reason="book_stale"}

## Links
- [[stream_book_symbol]]
- [[pre-publish-gates]]
```
### 70_Metrics/Data Quality Metrics.md
```text
# Data Quality Metrics

## Key metrics
- `ticks_dropped_total{reason="stale"}`
- `ticks_dropped_total{reason="future"}`
- `tick_dedup_drop_total`
- `unknown_side_total`
- `quarantine_events_total`
- `symbol_freeze_total`
- `freshness_ms`
- `ingest_ts_minus_event_ts_ms`
- `gap_detected_total`

## Required dashboards
- per-symbol freshness
- stale/future/dup rate by symbol
- age/skew distribution
- quarantine volume over time
- top bad symbols in current window

## Alerts
- freshness exceeds budget for major symbols
- stale/future rate breaches threshold
- freeze triggered repeatedly on same symbol
- unknown side spike threatens CVD quality

## Notes
- keep epoch ms everywhere
- measure both count and duration of freezes
- chart bad-time trigger streaks where available

## Links
- [[Time Model]]
- [[Data Quality Model]]
- [[RCA-2026-04-18-time-skew-freeze]]
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
