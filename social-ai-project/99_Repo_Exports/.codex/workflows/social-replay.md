---
name: social-replay
description: Build or review deterministic replay, regression baselines, and LLM/trend/outcome validation flows for the social-ai project. Covers raw trend replay, content-plan replay, publish dry-run replay, outcome attribution replay, governor decision replay.
---

When the user types `/social-replay <scope>` or asks for replay, baseline diffing, dataset export, regression checks, or offline validation, orchestrate the work using `.claude/agents/agents.md` and `.claude/skills/`.

## Mission
Produce a deterministic replay and regression-validation plan for `<scope>`.

## Mandatory replay scenarios
- raw trend replay
- content plan replay (LLM with pinned prompt/model via llama.cpp)
- publish dry-run replay
- outcome attribution replay
- governor decision replay

## Execution sequence
1. Act as **@social-lead** and define what must be replayed: raw social events, media enrichment, trend features, LLM plans, publish decisions, outcomes, or governor decisions.
2. Load **social-project-core**, **social-data-quality-time**, and **social-observability-rollout**.
3. Load **social-llm-content-planner** if LLM agent output is in scope (pin prompt+model; use llama.cpp for determinism).
4. Load **social-go-redis-ingest** if source event capture/normalization is in scope.
5. Load **social-timescale-postgres** if archive tables, retention, or replay datasets must be stored.
6. Load **social-governor** if governor decisions are replayed.
7. Act as **@ml-replay-engineer** and define: source streams + retention assumptions, canonical payload schema, ordering/timestamp normalization, baseline artifacts, comparison metrics + failure thresholds.
8. Act as **@sre-rollout** and define observability for replay jobs.
9. Return one merged answer: Goal · Facts/Assumptions/Risks · Replay scope + data contracts · Required files/scripts/ENV/storage · Validation metrics + pass/fail thresholds · Tests · Rollout/rollback for the replay path.

## Replay rules
- Define timestamp unit + ordering guarantees explicitly.
- Remove/isolate wall-clock and live-API dependencies inside replayed logic (use archives, llama.cpp).
- LLM pass/fail = schema-valid + required fields + valid reason codes + score-in-range (not exact text).
- Make pass/fail criteria measurable. If historical/archive data is incomplete, say so and propose a safe fallback.

## Model lane
Default to **claude-opus + Planning**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
