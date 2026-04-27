---
name: trade-resilience-failure-drills
description: Use this skill when the task involves resilience, degraded modes, stale data, duplicate events, missing ticks, Redis lag/outage, kill switches, fail-open vs fail-closed, failure injection, incident drills, or rollback safety in the trade project. Relevant for prompts about resiliency, chaos, stale data, outages, drills, degraded mode, rollback safety.
---

# Trade Resilience and Failure Drills

## Goal
Prove that the system remains predictable and safe under realistic failure conditions.

## Use this skill for
- stale market data handling
- duplicate or missing event tolerance
- Redis outage/lag scenarios
- partial dependency failure
- kill switch design
- fail-open vs fail-closed review
- incident rehearsal and post-release safety checks

## Required analysis steps
1. Name the failure mode.
2. Define trigger and blast radius.
3. Define expected behavior by subsystem.
4. Define observability evidence:
   - metrics
   - logs
   - alerts
5. Define operator action:
   - feature flag
   - throttle
   - quarantine
   - rollback
6. Define exit condition and recovery validation.
7. Define regression tests or drills to preserve behavior.

## Example drill scenarios
- Redis unavailable for 30s
- exchange timestamps jump forward
- order book stream stalls while ticks continue
- duplicate klines for same close time
- consumer lag exceeds threshold
- ML config missing during rollout

## Rules
- Every drill must end with a success/failure criterion.
- Prefer small blast radius and reversible changes.
- Make kill switches and manual runbooks explicit.
- State whether the intended safety posture is fail-open or fail-closed.\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- design one drill/scenario at a time
- prefer limited blast radius and explicit evidence checklist

## Escalate to claude-sonnet-4-6/opus-4-6 if
- the drill spans multiple teams/subsystems with unclear recovery ownership
- fail-open/fail-closed policy requires architecture-level review
- incident history is ambiguous and needs deeper RCA first

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
