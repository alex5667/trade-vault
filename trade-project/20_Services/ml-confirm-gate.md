---
type: service
title: ml-confirm-gate
service: ml-confirm-gate
language: python
criticality: high
inputs: [candidate_features, indicators, rule_score, scenario]
outputs: [ml_decision, metrics:ml_confirm]
source_paths:
  - python-worker/core/of_confirm_engine.py
  - ml_confirm_gate_dynamic_v8_stack_schema_v3.py
  - ml_feature_schema_v5_of.py
tags: [python, ml, gate, rollout, shadow, enforce]
updated_at: 2026-04-18
---

# ml-confirm-gate

## Purpose
Второй эшелон проверки signal quality: rule-based candidate дополнительно проверяется ML-моделью до публикации / исполнения.

## Operating modes
- `OFF`
- `SHADOW`
- `ENFORCE`

## Expected inputs
- symbol
- ts_ms
- direction
- scenario
- indicators
- rule_score
- rule_have
- rule_need
- cancel_spike_veto
- optional confirmations

## Outputs
Minimal decision fields:
- `mode`
- `allow`
- `should_enforce`
- `bucket`
- `p_edge`
- `p_min_used`
- `share_used`
- `model_ver`
- `abstain`
- `conf`
- `p_margin`
- `status`
- `err`
- `missing`

## Core policies
### SHADOW
- decision logged
- not blocking production path

### ENFORCE
- may block if `p_edge < p_min_used`
- can abstain in band / low confidence zone
- may fail-open or fail-closed depending on config

### A/B / champion-challenger
- deterministic routing by sticky key
- challenger compared on subset / split

## Schema discipline
- feature schema version must be explicit
- no silent reorder of features
- model / schema mismatch must emit visible error status
- missing features cannot be silently ignored without metric

## Failure modes
- no model loaded
- config missing
- feature schema mismatch
- Redis cfg reload issues
- metrics stream write failure
- high missing ratio
- challenger drift from champion

## Required metrics
- decision count by mode / status
- allow / block / abstain rate
- missing count
- latency_us / latency_ms
- p_edge distribution
- p_margin distribution
- by-symbol enforcement share
- cfg reload failures

## Alerts
- missing / error rate spike
- status `n_features_mismatch`
- 100% no-config / no-model
- latency p99 regression
- sudden block-rate change after rollout

## Rollout discipline
### Before moving to ENFORCE
- SHADOW only
- compare allow/block counterfactuals
- inspect by symbol and by scenario
- verify no config / no model failure path
- confirm rollback switch exists

### Rollback
- force mode to SHADOW or OFF
- freeze challenger
- revert p_min / enforce share config
- preserve metrics for postmortem

## Linked notes
- [[Pipeline Overview]]
- [[pre-publish-gates]]
- [[signal-dispatch]]
