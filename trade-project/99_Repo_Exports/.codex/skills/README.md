# Codex Skills Index

Use these local skills as scoped instruction packs. For `tr:` requests, load `trade-project-core` first, then only the smallest relevant subset.

## Core
- `trade-project-core`: global trade architecture, routing, response contract, production standards.
- `trade-quality-gates`: pass/fail gates, acceptance criteria, regression barriers.
- `trade-observability-rollout`: metrics, alerts, rollout, rollback, SLO/SLA.
- `trade-contract-regression`: Redis, WebSocket, REST, and storage contract compatibility.

## Subsystems
- `trade-go-redis-ingest`: Go ingest, exchange streams, Redis publish path.
- `trade-python-signal-engine`: Python detectors, gates, feature pipelines.
- `trade-api-ui-contracts`: NestJS, Next.js, DTOs, WebSocket/API payloads.
- `trade-timescale-postgres`: Postgres/Timescale schema, indexes, hypertables.

## Risk And Validation
- `trade-data-quality-time`: timestamp/data quality, sanitize, quarantine, metrics.
- `trade-latency-benchmarking`: p50/p95/p99, allocations, throughput, backpressure.
- `trade-execution-risk`: spread, slippage, fill quality, execution-cost gates.
- `trade-ml-replay-gating`: replay, ML confirmation, calibration, drift.
- `trade-backtest-validity`: anti-leakage, fill assumptions, replay validity.
- `trade-resilience-failure-drills`: failure injection, stale data, kill switches.

## Specialized Reviews
- `trade-exchange-adapter`: venue normalization, sequencing, metadata.
- `trade-storage-retention`: Redis stream length, Timescale retention/compression, archive policy.
- `trade-fast-*`: bounded low-risk workflows and boilerplate tasks.
- `trade-pro-*`: high-risk architecture, incident, ML, rollout, or schema reviews.

