---
name: trade-backtest-validity
description: Use this skill for replay and backtest trustworthiness in the trade project: leakage checks, event-time correctness, execution realism, train/test boundaries, and acceptance criteria for offline evaluation.
---

# Trade Backtest Validity

## Goal
Verify that replay and backtest results are realistic, leakage-free, and based on valid data-availability and execution assumptions.

## Default lane
Use Gemini Flash for bounded review of one metric, one replay job, or one local test harness. Escalate to a premium reasoning model for methodology redesign, label changes, or ambiguous offline-vs-online discrepancies.

## Use this skill for
- lookahead leakage review
- event-time vs processing-time correctness
- train/test split validation
- fill-model realism
- replay acceptance criteria
- offline metric interpretation for strategy changes

## Required output
1. Goal
2. Facts
3. Assumptions
4. Risks
5. Validity checks
6. Acceptance criteria
7. Tests and evidence needed

## Scope rules
- State exactly what data is available at decision time.
- Separate signal quality from fill/execution quality.
- Explicitly call out leakage risks, survivorship bias, and regime imbalance.
- Prefer replayable fixtures and deterministic evidence.

## Escalate to premium if
- labels or evaluation methodology change
- offline and online metrics disagree without clear cause
- the task affects ML features, outcomes, or execution assumptions
- trust in historical conclusions is at risk

## Token discipline
- Read only the relevant replay/backtest logic, nearest fixtures, and metric definitions first.
- Avoid broad historical analysis unless the validity trigger is cross-cutting.
