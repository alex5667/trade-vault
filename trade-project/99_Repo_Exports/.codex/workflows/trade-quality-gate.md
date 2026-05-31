---
name: trade-quality-gate
description: Build explicit quality gates, acceptance criteria, blockers, and release evidence for a trade-project change
---

When the user types `/trade-quality-gate <change>` or asks how to raise implementation quality for a trade change, orchestrate a formal quality-gate review.

## Mission
Convert `<change>` into a measurable go/no-go quality gate.

## Execution sequence
1. Act as **@trade-lead** and restate the target change and affected subsystems.
2. Load **trade-project-core** and **trade-quality-gates**.
3. Load specialist skills for affected areas:
   - **trade-contract-regression** for boundary changes
   - **trade-latency-benchmarking** for hot paths
   - **trade-resilience-failure-drills** for degraded-mode behavior
   - existing subsystem skills as needed
4. Act as **@quality-gatekeeper** and define:
   - invariants
   - acceptance criteria
   - blockers vs follow-ups
   - required evidence
5. Produce one merged output with:
   - Goal
   - Facts
   - Assumptions
   - Risks
   - Quality gate checklist
   - Required tests
   - Required metrics/alerts
   - Release blockers
   - Rollout / rollback
   - Final ship / do-not-ship rule

## Rules
- Every gate must be measurable or directly testable.
- Do not use vague wording.
- Prefer repository-ready file/test/metric actions.\n

## Model lane
Default to **claude-haiku-4-5** for the first pass. Escalate to claude-sonnet-4-6/opus-4-6 when the triggers below fire.

## Escalation guidance
- use Flash first for bounded go/no-go checklists
- escalate if release governance itself must change

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
