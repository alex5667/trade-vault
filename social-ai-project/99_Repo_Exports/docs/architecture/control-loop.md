# Control Loop — implemented pipeline

The plan's core management contour (§20) is implemented and proven end-to-end on real
Redis + TimescaleDB:

```
collect → rank → brief (LLM) → policy gate → approve → publish (draft) → outcome → governor
```

## Components (all runnable + tested)

| Stage | Service / module | Input → Output | Schema |
|---|---|---|---|
| collect | `collector-manual-trends` (Py) | seed file → `social:trend:candidates` | trend_candidate.v1 |
| rank | `worker-trend-rank` | candidates → `social:trend:ranked` + `trend_observations` | trend_candidate.v1 |
| brief | `worker-content-plan` (LLM) | ranked → `social:brief:results` + `content_briefs` | content_brief.v1 |
| policy | `worker-policy-critic` | briefs → `social:policy:results` + `policy_decisions` | policy_decision.v1 |
| approve | NestJS `POST /review/:id/approve` or `/publish/:id/schedule` | → `social:publish:requests` | publish_job.v1 |
| publish | `worker-publish` (sandbox adapter) | requests → `social:publish:status` + `publish_jobs` | publish_job.v1 |
| outcome | `worker-outcome-attribution` | status → `social:outcome:24h` + `platform_metric_snapshots` + `experiment_outcomes` | outcome_event.v1 |
| governor | `worker-governors` | cohort outcomes → stage (off→shadow→canary→enforce) + `governor_decisions` | — |

## Shared libraries (`packages/*/python`)
- `social_streams` — EventEnvelope v1, producer, consumer groups, **PEL recovery, DLQ, quarantine**, replay.
- `social_db` — Postgres/Timescale repositories (statement-timeout guarded).
- `social_llm` — `LLMClient` (StubLLM / OllamaClient) + `generate_structured` (JSON validate + 1 repair → reject).
- `social_obs` — structured logger + Prometheus metrics; `social_config` — env config.

## Safety properties enforced
- **Draft-first**: `worker-publish` clamps visibility to draft/private/unlisted unless `AUTOPUBLISH_ENABLED=true` (proven: all E2E publishes were `private`).
- **LLM never publishes**: briefs are JSON validated against `content_brief.v1`; invalid output → repair → Drop → DLQ (`llm_schema_reject_total`).
- **Idempotency**: producer dedupe + sandbox publisher idempotent on `job_id` (`publish_duplicate_blocked_total`).
- **Poison isolation**: schema-invalid stream messages → `social:quarantine`, never crash the consumer.
- **Governed promotion**: governor compares admitted vs control cohorts, promotes on the lower confidence bound of lift, rolls back on negative lift, with a Redis TTL fail-safe stage key.

## Run it
```bash
make test           # 34 unit tests (social_streams + workers)
make e2e            # Phase 2-3 proof on a throwaway redis
make test-stack-up  # throwaway Redis :6397 + Timescale :5433 (migrated)
make e2e-full       # full loop with DB persistence + governor
make test-stack-down
```

A clean `e2e-full` run persists, per cycle: `trend_observations=3, content_briefs=3,
policy_decisions=3, publish_jobs=3, publish_status_events=3, platform_metric_snapshots=3,
governor_decisions=1`, and advances the governor to `canary` on a strong admitted cohort.

## Not yet implemented (later phases)
Real platform adapters (YouTube/TikTok OAuth uploads — Phase 8/9), media pipeline
(ASR/OCR/render — Phase 3+), Next.js operator screens (Phase 7), vLLM serving,
remaining NestJS modules (Auth/Outcome/Governor/Commerce/Replay), CI workflows.
