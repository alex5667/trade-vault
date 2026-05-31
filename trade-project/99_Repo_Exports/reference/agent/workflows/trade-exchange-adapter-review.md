---
description: Review or design exchange-adapter changes for the trade project, keeping venue-specific quirks isolated and contracts deterministic.
---

1. Act as **@trade-lead**. Restate the adapter goal, source venue, target scope, and success criteria.
2. Load **trade-project-core**.
3. Load **trade-exchange-adapter**.
4. If time or sequencing is touched, load **trade-data-quality-time**.
5. If Redis, NestJS, or WebSocket payloads are touched, load **trade-contract-regression** and **trade-api-ui-contracts**.
6. Act as **@exchange-adapter-engineer** and produce the venue-specific normalization rules, edge cases, and exact file changes.
7. If hot-path ingestion is affected, act as **@go-ingest-engineer** and validate performance and reconnect behavior.
8. Act as **@contract-governor** if downstream contracts may change.
9. Act as **@quality-gatekeeper** and convert the merged plan into pass/fail criteria.
10. Return one merged answer:
   - Goal
   - Facts
   - Assumptions
   - Risks
   - File changes / contracts
   - Tests
   - Metrics / alerts
   - Rollout / rollback
