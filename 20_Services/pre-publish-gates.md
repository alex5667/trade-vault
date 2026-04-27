---
type: service
title: pre-publish-gates
service: pre-publish-gates
language: python
criticality: high
inputs: [candidate, runtime_context, ml_result]
outputs: [allow_or_veto, reason_code, diagnostics]
source_paths:
  - python-worker/handlers/crypto_orderflow/utils/pre_publish_gates.py
tags: [python, gates, risk, dq, validation]
updated_at: 2026-04-18
---

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
