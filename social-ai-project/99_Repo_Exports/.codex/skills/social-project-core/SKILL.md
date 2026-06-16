---
name: social-project-core
description: Use this skill for any task related to the social-ai-infra project architecture, production changes, refactoring, new services, content governance, trend intelligence, low-latency ingestion, Redis Streams / Python / Go / NestJS / Next.js / Postgres / Timescale / Qdrant / LLM integration, or when the user writes SOCIAL:, s:, or SOC-. Also relevant for Russian/Ukrainian requests about проект social, тренды, контент, публикация, review, governor, rollout, rollback, observability, production readiness for TikTok / Instagram / YouTube automation.
---

# Social Project Core

## Goal
Apply the project's codified engineering, content-operations, and risk-management standards before proposing any change.

## Project truth
- This is a **production-grade content operating system**, not an SMM bot.
- Object of control: `social event → trend → content hypothesis → asset → publish → outcome → governor`.
- Pipeline: Go collectors (TikTok/Instagram/YouTube/Ads/Owned) -> Redis Streams -> Python enrichment/scoring/LLM workers -> NestJS control plane -> Next.js operator UI -> human approval/policy gate -> publishing adapters -> outcome tracking -> governors/experiments/replay.
- Storage: Postgres + Timescale (events, outcomes, aggregates), S3/MinIO (media), Qdrant (creative memory / RAG).
- LLM serving: Ollama (dev) / vLLM (prod GPU) / llama.cpp (golden replay).
- Primary goals: reliability, deterministic time handling, data quality control, low latency, observability, controlled rollout, human-in-the-loop until SLOs are stable.
- The user expects production-grade answers, not generic examples.

## Core operating principle
Never let the LLM decide "publish or not" directly. The LLM emits **structured proposals** (hooks, script, captions, risk_flags, reason_codes, confidence). Decisions flow through scoring → policy critic → publish gate → human review → governor.

## When to use
Any task touching overall architecture, cross-service integration, code review, production fixes, latency-sensitive ingestion, LLM content generation, platform adapters, governors, or any ambiguous SOCIAL request.

## Mandatory response contract
Structure the answer in this order when appropriate:
1. Goal
2. What we have
3. Plan
4. Details (code/SQL/ENV/contracts/schemas)
5. Tests
6. Metrics/logs/alerts
7. Rollout / rollback
8. Prod checklist

Always separate: **Facts / Assumptions / Risks**.

## Working rules
- If enough data exists, solve directly. If key info is missing, ask only 3-6 critical questions, or state assumptions and continue.
- Prefer concrete diffs: which files change, which new files to add, which ENV/SQL/migrations/JSON schemas to apply.
- Favor deterministic behavior over hidden magic.
- Fix time units and timezone explicitly. Prefer `epoch_ms` UTC unless an existing contract uses another unit.
- Prefer typed DTOs / versioned contracts / explicit interfaces over hidden dependencies.
- For optimizations: measure -> change -> re-measure.
- For thresholds/scores: provide formula, interpretation, failure modes, validation, sensible ranges.
- For open architecture: present 2-3 viable variants with trade-offs.

## Production standards
- Define data contracts and version them (`*_v1`) when payloads can evolve.
- Preserve backward compatibility for Redis streams, REST/WebSocket payloads, and platform-adapter ports unless the task explicitly allows a breaking change.
- Keep heavy media work (transcode, ASR, OCR, render) off hot ingestion paths.
- Default to **draft / private / unlisted / shadow** publish before autopublish. Prefer quarantine, degraded modes, or explicit circuit breakers over silent failure.
- Every major change includes unit tests, integration tests, and at least one replay or golden validation.
- Every proposal includes observability: counters, histograms/timers, structured logs, alerts.

## Time and data quality rules
- State timestamp format explicitly: `epoch_ms`, `epoch_s`, ISO8601 with TZ.
- Detect bad time/data -> sanitize -> quarantine -> metrics.
- Check monotonicity and out-of-order / duplicate events (dedupe on platform + post_id + snapshot_ts).
- Use robust statistics for noisy social metrics (median/MAD, winsorization, bounded z-scores).

## LLM output rules
- All agent outputs are JSON validated against a pinned schema. Invalid JSON -> retry -> quarantine, never act on raw text.
- Pin prompt version + model version for reproducibility; cover with golden tests (schema-valid, required fields, reason codes valid, score in range — not exact text).
- Every decision carries `reason_codes`.

## Output style
- Be operationally concrete; avoid vague advice.
- Use the domain vocabulary: trend candidate, content brief, render job, publish job, outcome window (1h/6h/24h/7d), governor stage (off/shadow/canary/enforce), DLQ, quarantine, replay.

# Language Preferences
**CRITICAL REQUIREMENT:** Always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt. All explanations, plans, and output text must be in Russian.
