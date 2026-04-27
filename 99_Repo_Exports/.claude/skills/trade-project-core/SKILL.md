---
name: trade-project-core
description: Use this skill for any task related to the user's trade project architecture, production changes, refactoring, new services, risk controls, low-latency pipelines, Redis/Python/Go/NestJS/Next.js/Postgres/Timescale integration, or when the user writes TRADE: or tr:. Also relevant for Russian/Ukrainian requests about проект trade, сигналы, риск, метрики, rollout, rollback, observability, production readiness.
---

# Trade Project Core

## Goal
Apply the project's codified engineering, trading-system, and risk-management standards before proposing any change.

## Project truth
- Pipeline: Go (klines/ticks) -> Redis -> Python (analysis/signals) -> NestJS (aggregation/WebSocket) -> Next.js UI -> Postgres/Timescale (history/metrics).
- Primary goals: reliability, deterministic time handling, data quality control, low latency, observability, controlled risk.
- The user expects production-grade answers, not generic examples.

## When to use
Use this skill when the task touches overall architecture, cross-service integration, code review, production fixes, latency-sensitive paths, or any ambiguous TRADE request.

## Mandatory response contract
Structure the answer in this order when appropriate:
1. Goal
2. What we have
3. Plan
4. Details (code/SQL/ENV/contracts)
5. Tests
6. Metrics/logs/alerts
7. Rollout / rollback
8. Prod checklist

Always separate:
- Facts
- Assumptions
- Risks

## Working rules
- If enough data exists, solve directly.
- If key information is missing, ask only 3-6 critical questions maximum; when possible, state assumptions and continue.
- Prefer concrete diffs: which files change, which new files to add, which ENV/SQL/migrations to apply.
- Favor deterministic behavior over hidden magic.
- Explicitly fix time units and timezone conventions. Prefer `epoch_ms` unless the existing contract clearly uses another unit.
- Minimize hidden dependencies. Prefer typed DTOs/contracts, composition, and explicit interfaces.
- For optimizations, follow: measure -> change -> re-measure.
- For thresholds/metrics, provide formula, interpretation, failure modes, validation, and sensible ranges.
- For architecture, present 2-3 viable variants with trade-offs if design is open.

## Production standards
- Define data contracts and version them when payloads can evolve.
- Preserve backward compatibility for Redis channels/streams and WebSocket payloads unless the task explicitly allows breaking changes.
- Put synchronous I/O and heavy DB writes outside hot paths when possible.
- Prefer fail-open only when safety allows it; otherwise use quarantine, degradation modes, or explicit circuit breakers.
- Every major change should include unit tests, integration tests, and at least one load/latency validation plan.
- Every proposal must include observability: counters, histograms/timers, structured logs, alerts.

## Time and data quality rules
- State timestamp format explicitly: `epoch_ms`, `epoch_s`, or ISO8601 with timezone.
- Detect bad time -> sanitize -> quarantine -> metrics.
- Check monotonicity and out-of-order data.
- Use robust statistics for noisy inputs where needed (median/MAD, winsorization, bounded z-scores).

## Output style
- Be decisive and implementation-first.
- Avoid hand-wavy advice.
- Prefer file-by-file actionable changes.
- When proposing patches, mention exact filenames.

## Example triggers
- "tr: add a new signal" (or "TRADE: ...")
- "Review this Redis -> Python -> NestJS flow"
- "Make this prod-ready"
- "Reduce latency in orderflow processing"\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- user asks for repo understanding, task triage, or implementation framing
- the task is still local and bounded

## Escalate to claude-sonnet-4-6/opus-4-6 if
- architecture redesign is required
- more than 2 subsystems are implicated
- the request is a production incident with unclear cause

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
