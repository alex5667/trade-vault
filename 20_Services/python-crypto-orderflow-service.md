---
type: service
title: python-crypto-orderflow-service
service: python-crypto-orderflow-service
language: python
criticality: high
inputs: [stream:tick_<symbol>, stream:book_<symbol>, candles:data]
outputs: [signals:of:inputs, signals:of:confirm, stream:signals:diagnostics]
source_paths:
  - python-worker/services/crypto_orderflow_service.py
tags: [python, orderflow, realtime, consumer-group, dq]
updated_at: 2026-04-18
---

# python-crypto-orderflow-service

## Purpose
Главный Python orchestration слой: читает Redis Streams, проверяет качество входа, поддерживает symbol runtimes и двигает pipeline к signal decisions.

## Responsibilities
- create / maintain consumer groups
- consume tick and book streams per symbol
- apply time hygiene
- dedupe
- unknown-side handling
- bootstrap calibration
- start / watch background loops
- maintain ML gate config
- restart crashed symbol tasks via supervisor

## Inputs
- `stream:tick_<symbol>`
- `stream:book_<symbol>`
- symbol / config state from Redis
- calibration data
- ML gate config

## Outputs
- updated runtime state
- diagnostics streams
- confirm / raw signal path downstream

## Background loops
- refresh loop
- ML gate maintenance loop
- burst flush loop
- supervisor loop

## Data-quality responsibilities
- reject stale ticks
- reject future ticks
- tolerate only bounded reorder
- dedupe identical messages
- quarantine bad-time streaks
- freeze symbol temporarily on repeated violations

## State
- per-symbol tasks
- per-symbol runtime
- seen tick dedupe window
- bootstrap semaphore
- restart history
- shutdown flag

## Failure modes
- consumer lag
- pending entries growth
- repeated symbol task crashes
- ML gate refresh slow / broken
- bootstrap timeout
- bad-time freeze storm
- high unknown-side ratio

## Observability
### Metrics
- ticks consumed total
- ticks dropped total by reason
- dedup drop total
- quarantine total
- symbol freezes active
- consumer lag ms
- loop latency / backlog
- task restarts total
- ml gate refresh latency

### Logs
- include symbol, stream, message id, reason code
- sample noisy hot-path logs
- never hide repeated critical failures without counters

## Rollout / rollback
### Rollout
- enable on a few symbols first
- verify consumer lag
- verify quarantine / drop metrics
- verify supervisor stability

### Rollback
- stop newly added symbols
- disable strict DQ changes first if false positives spike
- revert ML refresh or bootstrap changes if startup degrades

## Linked notes
- [[Time Model]]
- [[Data Quality Model]]
- [[detector-runtime]]
- [[ml-confirm-gate]]
- [[pre-publish-gates]]
