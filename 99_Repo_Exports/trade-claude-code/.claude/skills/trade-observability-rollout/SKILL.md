---
name: trade-observability-rollout
description: Use this skill for production rollout, rollback, SRE, Prometheus metrics, logs, alerts, canary/shadow deployments, latency budgets, failure modes, and operational safety in the trade project. Relevant for prompts about observability, rollout, rollback, dashboards, alerts, SLOs, shadow mode, canary, circuit breakers, and production readiness.
---

# Trade Observability and Rollout

## Goal
Ensure every significant trade-system change is measurable, alertable, and safely releasable.

## Use this skill for
- Production readiness review
- Metrics/logging/alert design
- Canary/shadow/enforce ramp plans
- Rollback strategy
- SLO/SLI design for trading services
- Failure-mode and degradation planning

## Mandatory deliverables
- RED metrics or equivalent for the service
- Domain metrics relevant to the specific change
- Structured log fields
- Alert rules with thresholds and rationale
- Rollout stages
- Rollback triggers

## Preferred rollout ladder
1. Local verification
2. Replay / backtest / fixture validation
3. Shadow mode
4. Canary with bounded share or bounded symbol set
5. Gradual ramp
6. Full enablement

## Rollback rules
- Define automatic and manual rollback conditions.
- Define the safe fallback mode.
- Preserve debuggability after rollback.
- Prefer config-gated rollback where feasible.

## Metrics guidance
Always include:
- throughput
- error rate
- latency p50/p95/p99
- backlog/lag
- domain success metric (e.g. emitted signals, accepted decisions, dropped messages)

## Alerts guidance
Include at least:
- hard failure alert
- latency regression alert
- data quality alert
- stale/no-data alert
- business KPI degradation alert if relevant

## Output requirements
When proposing a production change, include:
- Prometheus metric names
- Sample log fields
- Alert expressions or pseudo-expressions
- Rollout steps
- Rollback steps
- Post-deploy validation checklist

## Output style
Be operationally concrete. Avoid vague statements like "monitor closely" without metrics or thresholds.\n

## Default lane
Assume **Gemini Flash + Fast mode** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- use Flash for bounded metrics/alert/dashboard boilerplate and checklist generation
- keep rollout advice tied to the affected subsystem only

## Escalate to premium if
- rollout policy spans multiple services or customer-visible blast radius
- automatic rollback logic or SLO policy is being redesigned
- incident ambiguity requires deeper reasoning

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
