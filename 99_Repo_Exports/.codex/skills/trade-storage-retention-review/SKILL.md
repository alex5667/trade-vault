---
name: trade-storage-retention-review
description: Review or design storage-retention and archival policy for the trade project across Redis, Postgres, Timescale, replay inputs, and operational metrics.
---

1. Act as **@trade-lead**. Restate the retention goal, datasets affected, storage layers involved, and operational constraints.
2. Load **trade-project-core**.
3. Load **trade-storage-retention**.
4. Load **trade-timescale-postgres**.
5. If Redis streams are affected, load **trade-go-redis-ingest**.
6. If replay or ML datasets are affected, load **trade-ml-replay-gating**.
7. Act as **@storage-retention-governor** and produce a retention matrix, archive policy, and rollback-safe migration plan.
8. Act as **@timeseries-dba** for schema, retention, compression, and EXPLAIN-oriented storage impact review.
9. Act as **@quality-gatekeeper** and define pass/fail release gates.
10. If the change is production-facing, act as **@sre-rollout** to define rollout, rollback, and monitoring.
11. Return one merged answer:
   - Goal
   - Facts
   - Assumptions
   - Risks
   - Retention matrix / exact changes
   - Tests / validation
   - Metrics / alerts
   - Rollout / rollback
