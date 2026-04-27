---
name: trade-regression-pack
description: Build a deterministic regression pack for tests, golden payloads, replay, benchmarks, and release evidence in the trade project
---

When the user types `/trade-regression-pack <change>` or asks for a stronger regression safety net, assemble a cross-cutting regression package.

## Mission
Create the minimum high-value regression evidence for `<change>`.

## Execution sequence
1. Act as **@trade-lead** and identify affected boundaries and hot paths.
2. Load:
   - **trade-quality-gates**
   - **trade-contract-regression**
   - **trade-latency-benchmarking**
   - **trade-resilience-failure-drills**
   - plus subsystem skills as needed
3. Produce a regression pack with:
   - unit tests
   - integration/contract tests
   - replay or golden-data tests
   - latency benchmark cases
   - failure-drill cases
   - required metrics and alerts
4. Mark each item as:
   - required before merge
   - required before canary
   - follow-up hardening
5. End with:
   - exact files to add/change
   - fixtures to store
   - release evidence checklist

## Rules
- Optimize for maximum risk reduction per test artifact.
- Prefer deterministic fixtures over ad-hoc manual checks.
- Keep the pack small but high-signal.\n

## Model lane
Default to **claude-haiku-4-5** for the first pass. Escalate to claude-sonnet-4-6/opus-4-6 when the triggers below fire.

## Escalation guidance
- use Flash first to assemble the smallest high-value regression pack
- escalate if cross-system selection requires deeper redesign

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
