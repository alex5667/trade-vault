---
name: trade-quality-gates
description: Use this skill when the task is about improving software quality, release quality, acceptance criteria, definition of done, invariants, regression barriers, test matrix design, or measurable pass/fail gates for the trade project. Relevant for prompts about качество, quality gates, acceptance criteria, regression prevention, readiness, definition of done.
---

# Trade Quality Gates

## Goal
Convert implementation work into explicit, measurable, and reviewable quality gates.

## Use this skill for
- definition of done for new features
- release readiness criteria
- acceptance criteria for detectors, gates, APIs, or storage changes
- regression prevention
- turning vague "add tests" into a concrete test matrix
- ranking must-have vs nice-to-have checks

## Required analysis steps
1. Restate the feature or change in one sentence.
2. Identify affected subsystems and interfaces.
3. Define invariants that must remain true.
4. Define acceptance criteria with pass/fail wording.
5. Define required evidence:
   - tests
   - metrics
   - dashboards/logs
   - replay or benchmark outputs
6. Define release blockers and non-blockers.
7. Define rollback criteria.

## Quality dimensions
- correctness
- deterministic time behavior
- boundary contract safety
- replayability
- latency budget compliance
- observability coverage
- operator usability
- rollback readiness

## Preferred output
Produce:
- a quality scorecard
- a release gate checklist
- a test matrix grouped by subsystem
- explicit "ship / do not ship" conditions

## Example release gate categories
- `gate.correctness.unit`
- `gate.contract.compat`
- `gate.replay.baseline`
- `gate.latency.p99`
- `gate.observability.coverage`
- `gate.rollback.ready`

## Rules
- Every gate must be measurable or directly testable.
- Avoid vague statements such as "enough tests" or "looks stable".
- Make non-functional requirements explicit.
- Separate blockers from follow-up items.\n

## Default lane
Assume **Gemini Flash + Fast mode** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- use Flash for bounded acceptance criteria, checklists, and release evidence
- prefer measurable local gates over broad process redesign

## Escalate to premium if
- quality gates require cross-service governance redesign
- release policy itself must change
- proof requires ambiguous multi-service reasoning

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
