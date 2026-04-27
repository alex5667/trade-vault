---
name: trade-new-signal
description: Design or implement a new trade signal, detector, gate, or confirmation path with tests and safe rollout
---

When the user types `/trade-new-signal <idea>` or asks to add, improve, or refactor a detector/gate, orchestrate a production-safe signal design flow using `.claude/agents/agents.md` and `.claude/skills/`.

## Mission
Turn `<idea>` into an implementable signal or gate design for the trade pipeline.

## Execution sequence
1. Act as **@trade-lead** and convert `<idea>` into a concrete problem statement.
2. Load **trade-project-core**.
3. Always load **trade-python-signal-engine**, **trade-data-quality-time**, and **trade-observability-rollout**.
4. Load **trade-go-redis-ingest** if new upstream fields, sequencing, or exchange-side enrichment are needed.
5. Load **trade-api-ui-contracts** if downstream DTO / WS / UI changes are needed.
6. Load **trade-timescale-postgres** if history, metrics, or labeling storage changes are needed.
7. Load **trade-ml-replay-gating** if the signal interacts with ML confirmation, replay, calibration, or dataset export.
8. Act as **@microstructure-analyst** first and define:
   - market intuition
   - expected edge
   - failure modes
   - regime sensitivity
   - execution / spread / slippage risk implications
9. Act as **@python-signal-engineer** and propose the concrete implementation:
   - detector state and inputs
   - payload contract
   - thresholds and calibration approach
   - quarantine / degrade behavior
   - exact files to change or add
10. Act as **@sre-rollout** and define metrics, alerts, and rollout ladder.
11. Return one merged answer with:
   - Goal
   - Facts / Assumptions / Risks
   - Signal definition
   - Architecture changes
   - File-by-file implementation plan
   - Tests (unit / integration / replay / load if needed)
   - Metrics / alerts
   - Rollout / rollback
   - Prod checklist

## Signal design rules
- Prefer deterministic logic over opaque heuristics.
- Time semantics must be explicit.
- For noisy thresholds prefer robust stats (median/MAD or bounded robust z) when appropriate.
- Preserve backward compatibility unless the user explicitly approves a breaking change.
- If labels or outcomes are required, define the storage and replay contract explicitly.\n

## Model lane
Default to **claude-haiku-4-5** for the first pass. Escalate to claude-sonnet-4-6/opus-4-6 when the triggers below fire.

## Escalation guidance
- use Flash first for problem framing, local detector diffs, and test scaffolding
- escalate when regime / ML / execution policy redesign is required

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
