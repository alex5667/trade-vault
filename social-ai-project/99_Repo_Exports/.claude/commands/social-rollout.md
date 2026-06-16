---
name: social-rollout
description: Prepare a safe shadow→draft→canary→enforce rollout plan for a social-system change with metrics and rollback triggers. Autopublish stays gated until SLOs are stable.
---

When the user types `/social-rollout <change>` or asks how to safely deploy a production-affecting social change, orchestrate the rollout plan using `.claude/agents/agents.md` and `.claude/skills/`.

## Mission
Convert `<change>` into a concrete release plan with gates, metrics, and rollback conditions.

## Execution sequence
1. Act as **@social-lead** and restate the change, blast radius, and production risk.
2. Load **social-project-core** and **social-observability-rollout**.
3. Load other skills as needed for the changed subsystem.
4. Act as **@sre-rollout** and define the release ladder:
   - local / test validation
   - replay or golden/fixture validation
   - shadow mode (generate, do not publish)
   - draft / private / unlisted publish (human review)
   - canary by platform / account / traffic share
   - ramp policy
   - full enablement (autopublish only after stable SLOs)
5. Require specialist review from the changed subsystem owner:
   - **@go-collector-engineer** for collector / Redis ingest changes
   - **@python-worker-engineer** for enrichment / scoring / worker logic
   - **@llm-content-engineer** for LLM agent / prompt / schema changes
   - **@platform-adapter-engineer** for publishing adapter changes
   - **@control-plane-engineer** for NestJS / Next.js / DTO / UI changes
   - **@timeseries-dba** for schema / DB / retention changes
   - **@ml-replay-engineer** for ML or replay-sensitive changes
6. Return one merged release plan with: Goal · Facts/Assumptions/Risks · Preconditions · Stage-by-stage rollout · Metrics + alert thresholds · Automatic rollback triggers · Manual rollback procedure · Post-deploy validation checklist.

## Rollout rules
- A rollout plan without measurable thresholds is incomplete.
- Prefer config-gated rollback (drop to draft-only / pause autopublish) over emergency code rollback.
- Define degraded safe mode explicitly.
- If the user asks for direct autopublish, still provide the safer staged alternative first.

## Model lane
Default to **claude-opus + Planning** for this workflow.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
