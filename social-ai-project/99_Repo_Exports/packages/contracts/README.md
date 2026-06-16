# packages/contracts

Versioned contracts. JSON Schemas live in `/schemas`; this package re-exports + validates them.

Schemas: `social_event.v1`, `trend_candidate.v1`, `content_brief.v1`, `render_job.v1`, `publish_job.v1`, `outcome_event.v1`, `policy_decision.v1`.

Phase 1 (Epic 2): EventEnvelope validation, contract tests, schema-compatibility CI. `shared-types` generates TS types from these.
