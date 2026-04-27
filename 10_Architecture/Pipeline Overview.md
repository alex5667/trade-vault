---
type: architecture
title: Pipeline Overview
project: trade
tags: [architecture, pipeline]
updated_at: 2026-04-18
---

# Pipeline Overview

## Purpose
Кратко и операционно описать, как данные проходят путь от биржи до сделки и аналитики.

## Step 1 — Source & ingestion
Источник данных — биржа. Go workers:
- поддерживают WebSocket
- фильтруют symbols
- делают reconnect
- используют REST backfill при разрывах
- пишут в Redis Streams

Артефакты:
- `stream:tick_<symbol>`
- `stream:book_<symbol>`
- `candles:data`

## Step 2 — Preprocessing
Python service читает Streams через consumer groups и приводит вход в детерминированный рабочий вид:
- stale / future tick detection
- dedupe
- unknown-side policy
- quarantine / freeze
- bootstrap calibration
- supervisor / anti-restart-storm

## Step 3 — Runtime & candidates
Для каждого symbol формируется runtime:
- current book snapshot
- z-delta / CVD state
- OBI
- ATR / regime
- HTF levels

На этом фоне возникают candidates:
- breakout
- absorption
- extreme
- obi_spike

## Step 4 — Confidence & ML
Rule signal получает confidence:
- primary score model
- calibrated probability
- optional ML confirm gate
- SHADOW / ENFORCE rollout discipline

## Step 5 — Gates
Каждый candidate должен пройти gates:
- data quality
- regime/session coherence
- feature drift
- SMT leader coherence
- edge cost / spread / expected slippage
- min interval

## Step 6 — Dispatch
Если candidate tradeable:
- собирается final signal payload
- создаётся stable signal_id
- включается semantic dedup
- signal route-ится в raw stream, execution queue и notify channel

## Step 7 — Execution
Исполнение может быть:
- MT5 bridge
- Binance REST
- paper simulation

## Step 8 — Post-trade loop
После открытия сделки система:
- мониторит позицию
- двигает stop в break-even / trailing
- пишет closes / fills / slippage
- обновляет cost-aware execution assumptions

## Design rules
- fail-open и fail-closed должны быть задокументированы отдельно
- каждая critical decision точка должна иметь reason code
- если есть silent fallback, он должен иметь метрики и alertability
- state transitions должны быть replayable

## Linked notes
- [[System Map]]
- [[Time Model]]
- [[Data Quality Model]]
- [[pre-publish-gates]]
- [[signal-dispatch]]
