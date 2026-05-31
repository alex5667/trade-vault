---
name: trade-latency-benchmarking
description: Use this skill when the task involves latency, throughput, allocations, hot paths, p50/p95/p99 budgets, load testing, benchmark harnesses, backpressure, or performance regression in the trade project. Relevant for prompts about latency, benchmark, load, throughput, p99, budgets, allocations, backpressure, performance.
---

# Trade Latency Benchmarking

## Goal
Validate that latency-sensitive code meets explicit performance budgets.

## Use this skill for
- hot-path detector/gate changes
- Go ingestion performance
- Python feature extraction/gating performance
- NestJS WebSocket fan-out performance
- Redis backlog or backpressure concerns
- pre/post optimization validation

## Required analysis steps
1. Identify the hot path and traffic shape.
2. Define latency and throughput budgets.
3. Define memory/allocation expectations.
4. Define benchmark methodology:
   - synthetic load
   - replay load
   - burst profile
   - steady-state profile
5. Capture baseline.
6. Apply change.
7. Re-measure and compare deltas.

## Minimum metrics to report
- throughput
- p50 / p95 / p99 latency
- max latency or stall events
- queue/backlog growth
- CPU and RAM footprint
- allocation rate if relevant
- dropped/retried events if relevant

## Rules
- No performance claim without a named measurement method.
- Prefer reproducible benchmark inputs.
- Use the same data/profile before and after changes.
- Report both absolute values and percent deltas.
- Include pass/fail thresholds, not just raw numbers.\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- benchmark one hot path at a time
- keep the first pass focused on baseline, budgets, and method

## Escalate to claude-sonnet-4-6/opus-4-6 if
- latency regression spans multiple services
- trade-off requires architecture redesign rather than local tuning
- measurement is blocked by ambiguous system behavior

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
