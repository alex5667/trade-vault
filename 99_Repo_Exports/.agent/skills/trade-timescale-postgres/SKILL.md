---
name: trade-timescale-postgres
description: Use this skill for PostgreSQL or TimescaleDB in the trade project: schema design, hypertables, retention, compression, indexes, query plans, migrations, write path safety, and metrics/history storage. Relevant for prompts about Postgres, Timescale, SQL, migrations, retention, partitions, continuous aggregates, and performance tuning.
---

# Trade Timescale and Postgres

## Goal
Design and tune storage for market history, metrics, signals, and audit trails with correctness first and performance second.

## Use this skill for
- Schema and migration design
- Timescale hypertables and retention policies
- Compression and continuous aggregates
- Write-path offloading from hot services
- Query performance and indexing
- Historical signal/metric storage

## Database rules
- Migrations must be explicit and reversible where practical.
- State exact timestamp column type and timezone semantics.
- Separate hot ingest tables from analytical aggregates when needed.
- Use composite indexes that match actual query predicates.
- Avoid synchronous DB writes in critical low-latency paths unless justified.

## Timescale guidance
- Choose chunk intervals based on ingest volume and query horizon.
- Define retention and compression policies explicitly.
- Use continuous aggregates for repeated dashboard queries.
- Document late-arriving data strategy.

## Required analysis
1. Workload shape (read/write ratio, cardinality, horizon)
2. Table design
3. Index plan
4. Retention/compression plan
5. Migration/rollback plan
6. Query validation with EXPLAIN
7. Metrics and alerting

## Deliverables
- DDL / migration files
- Backfill strategy if schema changes existing data
- Query examples
- EXPLAIN-based tuning notes
- Failure modes (lock risk, bloat, retention loss)

## Tests required
- Migration apply/revert test
- Integration tests against realistic sample data
- Query performance validation on representative volumes

## Observability
- insert latency
- deadlocks / lock waits
- table bloat indicators
- compression/retention job status
- slow query alerts

## Output style
Return concrete SQL/migration files, index rationale, and rollback notes.\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- use Flash for bounded DDL, additive indexes, and local query tuning
- limit scope to touched tables, migrations, and queries first

## Escalate to claude-sonnet-4-6/opus-4-6 if
- retention/compression/hypertable lifecycle is being redesigned
- write-path pressure or storage strategy spans multiple services
- schema compatibility risk is high or migration sequencing is complex

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
