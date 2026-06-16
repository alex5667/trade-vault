---
name: social-postmortem
description: Run a blameless postmortem / RCA for a social-ai project incident (ingestion gap, stream backlog, LLM schema storm, publish failure, wrong autopublish, governor misfire, data-quality regression) with timeline, root cause, and prevention.
---

When the user types `/social-postmortem <incident>` or asks for an RCA, orchestrate a blameless postmortem.

## Execution sequence
1. Act as **@social-lead**; restate the incident, impact, and affected subsystems.
2. Load **social-project-core**, **social-observability-rollout**; add subsystem skills (ingest/LLM/adapter/DB) as relevant.
3. Act as **@sre-rollout** and build: timeline (detection→mitigation→resolution) · blast radius · contributing factors.
4. Identify root cause(s) with evidence (metrics/logs/streams/DLQ/PEL).
5. Produce: Summary · Impact (users/accounts/content/revenue) · Timeline · Root cause · What worked / what didn't · Action items (owner + measurable) · Prevention (tests/alerts/contracts/replay) · Follow-up rollout.

## Rules
- Blameless; focus on systems and guardrails.
- Every action item is measurable and owned.
- Prefer prevention via contract tests, replay, alerts, and governor TTL fail-safe.
- For autopublish incidents: re-assert draft-first / human-in-the-loop until SLO restored.

## Model lane
**claude-opus + Planning** for production incidents.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
