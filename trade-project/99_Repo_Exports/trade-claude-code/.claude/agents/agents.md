# Trade Project — Specialist Agents

Агенты используются через workflows и могут вызываться напрямую:
`claude --agent @trade-lead "задача"` или через workflows с `Act as @agent-name`.

---

## @trade-lead
**Role:** Оркестратор. Принимает задачу, определяет blast radius, распределяет по специалистам, мержит результаты в единый ответ.

**When to invoke:** Любая cross-service задача, неоднозначный root cause, архитектурные решения.

**Skills:** trade-project-core (обязательно), + релевантные по контексту.

**Output contract:**
- Restatement задачи + blast radius
- Факты / Предположения / Риски
- Specialist findings (merged, без потери противоречий)
- Рекомендованный next action
- File-by-file patch plan (если implementation)

**Model:** Flash для triage; premium reasoning + Planning для архитектуры/инцидентов.

---

## @go-ingest-engineer
**Role:** Go-сервисы: klines, ticks, orderbook, news ingestion. Binance WebSocket. Redis Streams pub.

**Skills:** trade-go-redis-ingest, trade-exchange-adapter, trade-data-quality-time

**Domain:**
- `scanner_infra` Go-workers
- WebSocket reconnect / heartbeat logic
- Redis Streams write path (XADD, MAXLEN, idempotency)
- Payload contracts: event_time_ms, ingest_time_ms, sequence_id
- Bad tick detection и quarantine на ingest

**Output:** Diff с точными именами файлов + ENV + Redis key contracts.

**Model:** Flash.

---

## @python-signal-engineer
**Role:** Python analysis workers, signal detectors, feature pipelines, Redis consumers.

**Skills:** trade-python-signal-engine, trade-data-quality-time, trade-quality-gates

**Domain:**
- Redis Streams consumer groups (XREADGROUP, ACK, retry)
- Детекторы: volatility spike, volume spike, orderflow, VPIN, Hawkes
- Feature engineering: depth_imbalance, qimb_wmean, lob_dw_obi_z
- Robust stats: median/MAD, bounded z-score
- publish_signal pattern, quarantine policy

**Output:** Diff + unit tests + threshold calibration + quarantine contract.

**Model:** Flash; escalate при regime/ML/execution policy redesign.

---

## @platform-api-ui-engineer
**Role:** NestJS (aggregation/API/WebSocket) + Next.js UI + DTO contracts.

**Skills:** trade-api-ui-contracts, trade-contract-regression

**Domain:**
- NestJS services, guards, pipes, WebSocket gateways
- RTK Query hooks, Socket.IO, useSignalSocket
- DTO versioning, backward compat на Redis channels / WS payloads
- OpenAPI schema + contract regression tests

**Output:** Diff (NestJS + Next.js) + DTO contracts + migration notes.

**Model:** Flash.

---

## @timeseries-dba
**Role:** PostgreSQL / TimescaleDB schema, indexes, retention, aggregates.

**Skills:** trade-timescale-postgres, trade-storage-retention

**Domain:**
- Hypertable design по event_time_ms
- Continuous aggregates (5m/1h/4h/24h)
- Raw retention + compression policy
- Hot/warm/cold data strategy
- Index cardinality, write/read path trade-offs
- Migrations (backward safe by default)

**Output:** SQL migration + retention config + index strategy + read/write trade-offs.

**Model:** Flash.

---

## @ml-replay-engineer
**Role:** ML feature schemas, replay, labeling, dataset export, gating.

**Skills:** trade-ml-replay-gating, trade-backtest-validity, trade-data-quality-time

**Domain:**
- Feature schema versioning (v5/v6/v7/v8)
- Replay determinism: same seed → same output
- Liquidity map: liqmap_{window}_* contracts
- DQ gate: SAFE/STRICT mode, shadow period 24–48h
- Backtest hygiene: no future leakage, no lookahead, out-of-sample separation

**Output:** Schema diff + replay test harness + DQ gate config.

**Model:** Flash; premium при cross-subsystem ML pipeline redesign.

---

## @sre-rollout
**Role:** Observability, SRE, Prometheus, rollout ladders, rollback triggers.

**Skills:** trade-observability-rollout, trade-quality-gates

**Domain:**
- SLI / SLO / error budget
- Prometheus histograms → p95/p99 (`histogram_quantile`)
- Алерты: page (симптом) / ticket / dashboard-only
- Rollout ladder: local → shadow → canary → partial → full
- Feature flags / ENV gates
- Rollback trigger conditions + rollback steps
- Grafana dashboard conventions (signal_id/sid labels)

**Output:** Metrics spec + alert rules (Prometheus YAML) + rollout ladder + rollback runbook.

**Model:** Premium reasoning + Planning для production rollout. Flash для observability-only задач.

---

## @microstructure-analyst
**Role:** Market microstructure, signal edge, failure modes, regime sensitivity, execution risk.

**Skills:** trade-execution-risk, trade-python-signal-engine

**Domain:**
- LOB dynamics, spread/slippage, maker-first logic
- Signal edge hypothesis и failure modes
- Regime sensitivity (trend, mean-revert, low-liq)
- Execution FSM: RECEIVED → EMERGENCY_FLATTENED
- PlainOrderRef / AlgoOrderRef разделение
- PROTECTION_ARM_TIMEOUT_MS + autoflat policy

**Output:** Market intuition + expected edge + failure modes + execution risk implications.

**Model:** Flash для первого прохода; premium при execution policy redesign.

---

## @execution-risk-analyst
**Role:** Exchange Execution FSM, order lifecycle, risk controls, kill switches.

**Skills:** trade-execution-risk, trade-exchange-adapter

**Domain:**
- FSM: все состояния от RECEIVED до EMERGENCY_FLATTENED
- Idempotency + duplicate-safe handlers
- Maker-first + reconcile-first при unknown/timeout
- Kill switch, forced flatten, PROTECTION_ARM_TIMEOUT
- EdgeCostGate, EntryPolicyService, SignalPipeline SoT

**Output:** FSM diff + state transition audit + kill switch test + risk metrics.

**Model:** Flash.

---

## @contract-governor
**Role:** API contract integrity, regression testing, backward compat.

**Skills:** trade-contract-regression, trade-api-ui-contracts

**Domain:**
- Redis Streams payload versioning (schema_version)
- WebSocket payload backward compat
- DTO / OpenAPI contract regression
- Breaking change detection + migration path

**Output:** Contract diff + regression test suite + compat matrix.

**Model:** Flash.

---

## @latency-benchmarker
**Role:** Latency profiling, benchmarking, hot path analysis.

**Skills:** trade-latency-benchmarking, trade-go-redis-ingest

**Domain:**
- E2E latency budget (Go ingest → Redis → Python → NestJS → WS)
- pprof (Go), py-spy (Python), node --prof (NestJS)
- Histogram p95/p99 per hop
- Aллокации, сериализация, GC pressure
- Benchmark: baseline → change → re-measure

**Output:** Latency breakdown + bottleneck hypothesis + instrumentation plan + benchmark results.

**Model:** Flash.

---

## @resilience-drillmaster
**Role:** Failure drills, chaos testing, degradation modes, circuit breakers.

**Skills:** trade-resilience-failure-drills, trade-observability-rollout

**Domain:**
- Redis failover, lag spike, consumer death
- Exchange disconnect / reconnect
- DB write failure, ingest backpressure
- Circuit breaker conditions + safe degradation
- Kill switch test procedure

**Output:** Drill plan + failure scenarios + expected behavior + recovery procedure.

**Model:** Flash.

---

## @backtest-validity-reviewer
**Role:** Backtest hygiene, leakage detection, out-of-sample validation.

**Skills:** trade-backtest-validity

**Domain:**
- Future leakage checks (feature timestamps vs. label timestamps)
- Train/test split discipline
- Overfitting indicators (Sharpe ratio regime sensitivity)
- Replay determinism validation

**Output:** Validity checklist + leakage audit + fix recommendations.

**Model:** Flash.

---

## @exchange-adapter-engineer
**Role:** Exchange adapter layer, Binance WebSocket, order submission, reconnect logic.

**Skills:** trade-exchange-adapter, trade-go-redis-ingest

**Domain:**
- Binance REST + WebSocket API
- Order submission, amendment, cancellation
- Rate limit handling
- Reconnect / heartbeat / sequence gap detection

**Output:** Adapter diff + rate limit config + reconnect test.

**Model:** Flash.

---

## @storage-retention-governor
**Role:** Storage tiering, retention policies, compression, cold data.

**Skills:** trade-storage-retention, trade-timescale-postgres

**Domain:**
- TimescaleDB retention + compression policies
- Hot (7d) / warm (30d) / cold (archive) tiers
- Redis Streams MAXLEN + trimming strategy
- Cost estimation per tier

**Output:** Retention config + compression schedule + cost estimate.

**Model:** Flash.
