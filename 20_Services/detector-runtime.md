---
type: service
title: detector-runtime
service: detector-runtime
language: python
criticality: high
inputs: [runtime_state, tick_flow, book_flow, htf_levels]
outputs: [candidate_objects]
source_paths:
  - python-worker/handlers/crypto_orderflow/core/crypto_orderflow_detector.py
tags: [python, runtime, detector, cvd, obi]
updated_at: 2026-04-18
---

# detector-runtime

## Purpose
Преобразовать очищенный market flow в интерпретируемые candidate events с явными direction, kind, score и reasons.

## Runtime model
Для каждого symbol runtime должен держать:
- latest book snapshot
- spread / top depth
- delta / CVD rolling state
- z-scores
- OBI and stability indicators
- ATR / regime
- HTF levels / pivots

## Candidate kinds
- `obi_spike`
- `absorption`
- `breakout`
- `extreme`

## Detection logic
### OBI spike
Стакан устойчиво перекошен в одну сторону.

### Absorption
Сильный order-flow impulse без достаточного price follow-through.

### Breakout
Сильный impulse + пересечение или пробой значимого уровня.

### Extreme
Сверхсильное событие по z / impulse intensity.

## Candidate contract
Mandatory starter fields:
- `kind`
- `direction`
- `raw_score`
- `level_key`
- `reasons[]`
- `quality_flags{}` after validators

## First-layer validators
- spread validator
- breakout requires OBI confirmation
- mode / regime coherence

## Failure modes
- runtime missing required book state
- broken ATR / regime context
- z-score distortion from bad ticks
- false breakout under wide spread
- stale HTF levels
- candidate spam on same market idea

## Metrics
- candidates created total by kind
- candidates vetoed total by validator
- spread veto ratio
- breakout without OBI ratio
- runtime not-ready events
- candidate density per symbol / minute

## Operator questions
- Почему candidate rate вырос?
- Это реальные market changes или broken input quality?
- Почему breakout veto rate резко вырос?
- Нужен tuning thresholds или broken book health?

## Linked notes
- [[python-crypto-orderflow-service]]
- [[pre-publish-gates]]
- [[signal-dispatch]]
