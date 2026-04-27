---
type: index
title: Services Index
project: trade
owners: [Alex]
tags: [index, services]
updated_at: 2026-04-18
---

# Services Index

## Ingestion
- [[go-worker-ingestion]] — получает market data, держит WS, делает backfill, пишет в Redis.

## Python core
- [[python-crypto-orderflow-service]] — основной consumer и orchestration слой Python pipeline.
- [[detector-runtime]] — runtime state + detector + candidate generation.
- [[ml-confirm-gate]] — secondary ML gate поверх rule-based pipeline.
- [[pre-publish-gates]] — финальная цепочка hard business / market / DQ filters.
- [[signal-dispatch]] — payload assembly, dedup, publish / routing.

## Execution
- [[mt5-executor]] — bridge в MT5 / брокера.

## Architecture notes
- [[Pipeline Overview]]
- [[Time Model]]
- [[Data Quality Model]]

## For later extension
Рекомендуемые следующие заметки:
- `post-trade-monitor.md`
- `service-slos.md`
- `streams index.md`
- `execution model.md`
- `incident template.md`
- `rollout template.md`

## Owner checklist per service
Для каждого сервиса должны быть заполнены:
- Purpose
- Inputs / outputs
- State / caches
- Failure modes
- ENV
- Metrics
- Alerts
- Safe rollout / rollback
- Source paths
