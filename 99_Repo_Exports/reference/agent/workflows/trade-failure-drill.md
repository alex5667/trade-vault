---
description: Design and evaluate resilience drills, degraded modes, kill switches, and rollback safety for the trade project
---

When the user types `/trade-failure-drill <scenario>` or asks how to improve operational quality and resilience, run a failure-drill design workflow.

## Mission
Validate safe behavior for `<scenario>` under realistic failure.

## Execution sequence
1. Act as **@trade-lead** and define the scenario and blast radius.
2. Load **trade-project-core**, **trade-observability-rollout**, and **trade-resilience-failure-drills**.
3. Load subsystem skills for affected components.
4. Act as **@resilience-drillmaster** and produce:
   - trigger
   - expected subsystem behavior
   - fail-open/fail-closed policy
   - metrics/logs/alerts
   - operator actions
   - rollback or kill-switch path
   - success/failure criteria
5. End with:
   - drill script
   - evidence checklist
   - follow-up fixes if drill fails

## Rules
- Prefer limited blast radius.
- Make manual operator steps explicit.
- Do not declare a drill complete without measurable success criteria.\n

## Model lane
Default to **Gemini Flash + Fast** for the first pass. Escalate to premium when the triggers below fire.

## Escalation guidance
- use Flash first for a single bounded scenario
- escalate for cross-system drills or policy redesign

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
