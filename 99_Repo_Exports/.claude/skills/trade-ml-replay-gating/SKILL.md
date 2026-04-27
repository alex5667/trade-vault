---
name: trade-ml-replay-gating
description: Use this skill for ML confirmation, classifier gating, replay datasets, champion/challenger evaluation, calibration, precision/expectancy metrics, and safe ML rollout in the trade project. Relevant for prompts about ML gate, p_edge, calibration, ECE, Brier, champion challenger, golden replay, offline evaluation, online monitoring, and feature-label contracts.
---

# Trade ML Replay and Gating

## Goal
Make ML-assisted decisions measurable, replayable, calibrated, and safe to roll out in a live trading environment.

## Use this skill for
- ML confirm gate design
- Replay dataset generation
- Feature/label contracts
- Champion/challenger evaluation
- Calibration and threshold setting
- Live rollout guards for ML components

## Core rules
- Separate offline model quality from online operational quality.
- Require deterministic replay inputs wherever possible.
- Version feature schemas and model configs.
- Report both predictive metrics and trading utility metrics.
- Do not promote models without rollback and freeze conditions.

## Evaluation checklist
1. Define label clearly
2. Define horizon and leakage controls
3. Define feature schema/version
4. Offline metrics: PR, ROC, calibration, Brier, ECE
5. Trading metrics: expectancy, hit rate, MAE/MFE proxies if relevant
6. Slicing: symbol, regime, session, liquidity bucket
7. Online metrics: latency, error rate, no-config rate, fallback rate

## Rollout policy
- Start in SHADOW
- Compare champion vs challenger on the same replay/live shadow slices
- Use bounded canary before enforcement
- Freeze or revert on error spikes, latency regressions, calibration drift, or utility collapse

## Deliverables
- Feature contract
- Config contract
- Replay plan
- Evaluation plan
- Metrics/alerts
- Promotion and rollback rules

## Tests required
- Schema compatibility tests
- Deterministic replay tests
- Model loading/config validation tests
- Latency budget tests for hot path inference

## Observability
At minimum include:
- inference_requests_total
- inference_errors_total
- fallback_total
- no_config_total
- inference_latency_us/ms
- calibration drift indicators
- top-threshold precision proxy where applicable

## Output style
Provide exact files, config keys, stream names, dataset requirements, and promotion guards.\n

## Default lane
Assume a **claude-opus-4-6 + Planning mode** by default for this skill because shallow reasoning here can create expensive mistakes.

## Scope rules
- treat replay, ML gating, calibration, and promotion logic as high-risk by default
- prefer Planning mode and explicit baseline/failure thresholds

## Escalate to claude-sonnet-4-6/opus-4-6 if
- always stay premium unless the task is only file lookup or documentation

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
