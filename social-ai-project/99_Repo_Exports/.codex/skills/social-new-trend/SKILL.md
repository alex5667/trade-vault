---
name: social-new-trend
description: Design or implement a new trend source, trend score, content strategy, or hook family with tests and safe rollout in the social-ai project. The new-signal analog for content.
---

When the user types `/social-new-trend <idea>` or asks to add/improve/refactor a trend source, score, content strategy, or hook family, orchestrate a production-safe design flow using `.claude/agents/agents.md` and `.claude/skills/`.

## Mission
Turn `<idea>` into an implementable trend/content design for the social pipeline.

## Execution sequence
1. Act as **@social-lead** and convert `<idea>` into a concrete problem statement.
2. Load **social-project-core**.
3. Always load **social-trend-scoring**, **social-data-quality-time**, and **social-observability-rollout**.
4. Load **social-go-redis-ingest** if a new collector / upstream fields are needed.
5. Load **social-llm-content-planner** if LLM content generation is involved.
6. Load **social-timescale-postgres** if storage / aggregates / labels change.
7. Load **social-governor** if the strategy will be promoted via shadow→canary→enforce.
8. Act as **@trend-analyst** first and define: audience intuition, expected edge, failure modes, platform sensitivity, commerce-fit implications.
9. Act as **@python-worker-engineer** and propose the implementation: features/score state + inputs, payload contract, thresholds + calibration, quarantine/degrade behavior, exact files to change/add.
10. Act as **@sre-rollout** and define metrics, alerts, rollout ladder.
11. Return one merged answer: Goal · Facts/Assumptions/Risks · Trend/strategy definition · Architecture changes · File-by-file plan · Tests (unit/integration/replay) · Metrics/alerts · Rollout/rollback · Prod checklist.

## Design rules
- Prefer deterministic, explainable scoring over opaque heuristics; rule-based before ML.
- Time semantics explicit. Robust stats for noisy inputs.
- Preserve backward compatibility unless a breaking change is explicitly approved.
- If outcomes/labels are required, define storage + replay contract explicitly.

## Model lane
**claude-haiku-4-5** for framing/local diffs; escalate when governor/ML/commerce policy redesign is required.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
