---
name: trade-rollout
description: Prepare a safe shadow to canary to enforce rollout plan for a trade-system change with metrics and rollback triggers
---

When the user types `/trade-rollout <change>` or asks how to safely deploy a production-affecting trade change, orchestrate the rollout plan using `.claude/agents/agents.md` and `.claude/skills/`.

## Mission
Convert `<change>` into a concrete release plan with gates, metrics, and rollback conditions.

## Execution sequence
1. Act as **@trade-lead** and restate the change, blast radius, and production risk.
2. Load **trade-project-core** and **trade-observability-rollout**.
3. Load other skills as needed for the changed subsystem.
4. Act as **@sre-rollout** and define the release ladder:
   - local / test validation
   - replay or fixture validation
   - shadow mode
   - canary by symbol, traffic share, or host subset
   - ramp policy
   - full enablement
5. Require specialist review from the changed subsystem owner:
   - **@go-ingest-engineer** for exchange / Redis ingest changes
   - **@python-signal-engineer** for detectors / gates / worker logic
   - **@platform-api-ui-engineer** for NestJS / Next.js / DTO / UI changes
   - **@timeseries-dba** for schema / DB / retention changes
   - **@ml-replay-engineer** for ML or replay-sensitive changes
6. Return one merged release plan with:
   - Goal
   - Facts / Assumptions / Risks
   - Preconditions
   - Stage-by-stage rollout
   - Metrics and alert thresholds
   - Automatic rollback triggers
   - Manual rollback procedure
   - Post-deploy validation checklist

## Rollout rules
- A rollout plan without measurable thresholds is incomplete.
- Prefer config-gated rollback over emergency code rollback where feasible.
- Define degraded safe mode explicitly.
- If user asks for direct production enablement, still provide the safer staged alternative first.\n

## Model lane
Default to a **claude-opus-4-6 + Planning** mode for this workflow.

## Escalation guidance
- production-affecting rollout ladders should default to premium reasoning

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
