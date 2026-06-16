---
name: social-parallel-investigation
description: Investigate an ambiguous social-ai problem by fanning out parallel hypotheses across subsystems (ingest, streams, enrichment, LLM, control plane, adapters, DB, governors) and merging into one ranked root-cause answer.
---

When the user types `/social-parallel-investigation <problem>` or the root cause is unclear and may span subsystems, run a parallel investigation.

## Execution sequence
1. Act as **@social-lead**; restate the problem and observable symptoms.
2. Load **social-project-core** and **social-observability-rollout**.
3. Fan out independent hypotheses, each owned by a specialist:
   - **@go-collector-engineer** — ingest/quota/freshness
   - **@python-worker-engineer** — enrichment/scoring/stream consumers
   - **@llm-content-engineer** — LLM schema/timeout/cost
   - **@platform-adapter-engineer** — publish/status/quota
   - **@control-plane-engineer** — API/UI/state machine
   - **@timeseries-dba** — schema/aggregates/retention
   - **@ml-replay-engineer** — replay/governor decisions
4. Each returns: hypothesis · supporting/contradicting evidence · confidence · cheapest disproving test.
5. Merge into one ranked answer (keep contradictions visible) with the recommended next action.

## Rules
- Evidence-first; cite metrics/logs/streams/DLQ.
- Prefer the cheapest disproving experiment per hypothesis.
- Do not collapse to one cause prematurely.

## Model lane
**claude-sonnet/opus + Planning**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
