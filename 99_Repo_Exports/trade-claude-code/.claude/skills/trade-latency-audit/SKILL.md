---
name: trade-latency-audit
description: Build a latency and throughput validation plan with budgets, benchmarks, and pass fail thresholds for hot paths in the trade project
---

When the user types `/trade-latency-audit <scope>` or asks to improve performance quality, run a benchmark-oriented performance review.

## Mission
Prove whether `<scope>` meets explicit latency and throughput budgets.

## Execution sequence
1. Act as **@trade-lead** and define the hot path and traffic shape.
2. Load **trade-project-core** and **trade-latency-benchmarking**.
3. Load subsystem skills for the relevant services.
4. Act as **@latency-benchmarker** and produce:
   - benchmark target
   - budgets (p50/p95/p99, throughput, backlog, RAM)
   - measurement method
   - baseline collection plan
   - change plan
   - re-measure plan
5. Output:
   - Goal
   - Facts
   - Assumptions
   - Risks
   - Benchmark matrix
   - Required tooling/tests
   - Metrics/alerts
   - Pass/fail thresholds
   - Rollout guardrails

## Rules
- Use baseline -> change -> re-measure.
- Prefer deterministic/replayable inputs where possible.
- Tie recommendations to concrete code paths and services.\n

## Model lane
Default to **Gemini Flash + Fast** for the first pass. Escalate to premium when the triggers below fire.

## Escalation guidance
- use Flash first for one hot path and one benchmark target
- escalate if the regression spans multiple services or requires redesign

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
