---
description: Build or review deterministic replay, regression baselines, and ML or signal validation flows for the trade project
---

When the user types `/trade-replay <scope>` or asks for replay, baseline diffing, dataset export, regression checks, or offline validation, orchestrate the work using `.agent/agents.md` and `.agent/skills/`.

## Mission
Produce a deterministic replay and regression-validation plan for `<scope>`.

## Execution sequence
1. Act as **@trade-lead** and define what must be replayed: ticks, books, microbars, signals, gates, or ML decisions.
2. Load **trade-project-core**, **trade-ml-replay-gating**, **trade-data-quality-time**, and **trade-observability-rollout**.
3. Load **trade-python-signal-engine** if detector or gating logic changes are part of the scope.
4. Load **trade-go-redis-ingest** if source event normalization or stream capture is part of the scope.
5. Load **trade-timescale-postgres** if archive tables, retention, or replay datasets must be stored.
6. Act as **@ml-replay-engineer** and define:
   - source streams and retention assumptions
   - canonical payload schema
   - ordering and timestamp normalization rules
   - baseline artifacts
   - comparison metrics and failure thresholds
7. Act as **@python-signal-engineer** if code changes are required for deterministic execution.
8. Act as **@sre-rollout** and define observability for replay jobs and regression monitoring.
9. Return one merged answer with:
   - Goal
   - Facts / Assumptions / Risks
   - Replay scope and data contracts
   - Required files / scripts / ENV / storage
   - Validation metrics and pass/fail thresholds
   - Tests
   - Rollout / rollback for introducing the replay path

## Replay rules
- Define timestamp unit and ordering guarantees explicitly.
- Remove or isolate wall-clock dependencies inside replayed logic.
- Make pass/fail criteria measurable, not subjective.
- If historical data is incomplete, say so clearly and propose a safe fallback.\n

## Model lane
Default to a **premium reasoning model + Planning** mode for this workflow.

## Escalation guidance
- replay, baseline diffing, and offline validation are high-risk and often deserve premium reasoning

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
