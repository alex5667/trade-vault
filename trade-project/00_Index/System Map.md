---
type: index
title: System Map
project: trade
owners: [Alex]
tags: [index, architecture, pipeline]
updated_at: 2026-04-18
---

# System Map

## Purpose
Карта всей системы trade на одном листе: где рождаются данные, где принимаются решения, где ставится риск и где исполняются сделки.

## End-to-end pipeline
1. **Go ingestion**
   - WebSocket / REST backfill
   - trades / book / candles
   - публикация в Redis Streams

2. **Python preprocessing**
   - tick time policy
   - dedupe
   - unknown side policy
   - quarantine / freeze
   - supervisor / resilience

3. **Detector & runtime**
   - SymbolRuntime
   - CVD / delta z-score
   - OBI / book state
   - candidate generation

4. **Scoring & ML confirm**
   - LightGBM scorer
   - calibrated confidence
   - ML confirm gate
   - SHADOW / ENFORCE / A-B

5. **Pre-publish gates**
   - hard data quality
   - regime/session
   - feature drift
   - SMT coherence
   - edge cost
   - interval / anti-spam

6. **Dispatch**
   - stable signal_id
   - semantic dedup
   - publish to raw streams
   - execution queues
   - telegram notifications

7. **Execution**
   - MT5 bridge
   - Binance REST executor
   - paper simulator

8. **Post-trade**
   - monitor
   - break-even / trailing
   - SLQ / adaptive stop
   - slippage feedback

## Main streams
- `stream:tick_<symbol>`
- `stream:book_<symbol>`
- `candles:data`
- `signals:crypto:raw`
- `signals:of:inputs`
- `signals:of:confirm`
- `orders:queue`
- `orders:queue:mt5`
- `notify:telegram`
- `stream:signals:diagnostics`

## Core invariants
- time in **epoch ms** unless explicitly documented otherwise
- side / direction contracts must be explicit and typed
- no silent data correction without metrics and reason codes
- stale / future / duplicate / gap data must be detected
- execution path must be idempotent
- rollout changes must be reversible
- every veto / deny must produce a reason code
- replayability is mandatory for decision-critical logic

## Operator questions
- Где появился лаг: source, Redis, consumer, model, dispatch?
- Это data problem, model problem или execution problem?
- Сигнал заблокирован правилами, ML или DQ?
- Есть ли risk-on/risk-off mismatch между symbol и leaders?
- Можем ли мы воспроизвести решение оффлайн?

## Linked notes
- [[Pipeline Overview]]
- [[Time Model]]
- [[Data Quality Model]]
- [[go-worker-ingestion]]
- [[python-crypto-orderflow-service]]
- [[detector-runtime]]
- [[ml-confirm-gate]]
- [[pre-publish-gates]]
- [[signal-dispatch]]
- [[mt5-executor]]
