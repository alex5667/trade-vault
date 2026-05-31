---
name: trade-postmortem
description: Analyze a trade incident or regression, find root causes, and produce a corrective action and prevention plan
---

When the user types `/trade-postmortem <incident>` or asks to investigate a regression, outage, bad signals, data corruption, or unexpected trading behavior, orchestrate an incident analysis using `.claude/agents/agents.md` and `.claude/skills/`.

## Mission
Produce a root-cause analysis and corrective-action plan for `<incident>`.

## Execution sequence
1. Act as **@trade-lead** and restate the incident timeline and impact.
2. Load **trade-project-core** and **trade-observability-rollout**.
3. Load subsystem skills based on the incident surface:
   - **trade-data-quality-time** for timestamp, ordering, stale-data, source-consistency, or malformed payload incidents
   - **trade-go-redis-ingest** for exchange connectivity, parsing, reconnect, or publisher failures
   - **trade-python-signal-engine** for detector/gate/calibration regressions
   - **trade-api-ui-contracts** for DTO, API, WS, or rendering regressions
   - **trade-timescale-postgres** for DB pressure, retention, schema, or query incidents
   - **trade-ml-replay-gating** for ML config, model drift, replay mismatch, or promotion incidents
4. Reconstruct the event in this order:
   - what changed
   - when it changed
   - what was observed
   - what protections failed or were missing
   - user, system, and trading impact
5. Produce one merged postmortem with:
   - Incident summary
   - Facts
   - Assumptions
   - Risks
   - Timeline
   - Likely root causes
   - Contributing factors
   - Immediate mitigation
   - Permanent corrective actions
   - Required tests / monitors / alerts
   - Rollout / rollback changes to prevent recurrence

## Postmortem rules
- No blame language.
- Distinguish observed facts from inference.
- Identify missing guardrails, not just the triggering bug.
- Every root cause must map to at least one concrete preventive action.\n

## Model lane
Default to a **premium reasoning model + Planning** mode for this workflow.

## Escalation guidance
- ambiguous incidents and production regressions should default to premium reasoning

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
