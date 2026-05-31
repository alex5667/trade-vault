---
name: trade-python-signal-engine
description: Use this skill for Python analysis workers in the trade project: real-time signal detection, Redis consumers, microstructure logic, robust statistics, rolling windows, orderflow features, low-latency async design, publish_signal patterns, and detector composition. Relevant for prompts about volatility spike, volume spike, orderflow, Python workers, Redis subscriptions, feature engineering, detector thresholds, MAD/median robustness.
---

# Trade Python Signal Engine

## Goal
Implement or improve Python signal workers that are modular, statistically robust, low-latency, and production-safe.

## Use this skill for
- New detector design
- Refactoring signal pipelines
- Redis consumer/producer logic
- Rolling statistics and thresholding
- Orderflow / volatility / volume analysis
- Python worker performance and observability

## Engineering rules
- Separate ingestion, feature extraction, decision logic, and publishing.
- Keep detectors composable and side-effect light.
- Prefer pure functions for feature computation where possible.
- Put config in explicit typed settings objects or validated env/config modules.
- Avoid hidden mutable global state in rolling calculations.
- Publish machine-readable reason codes with every signal.

## Statistical rules
- Favor robust estimators for noisy data: median, MAD, trimmed means when relevant.
- State warmup requirements explicitly.
- Distinguish sample insufficiency from a negative signal.
- Bound features and z-scores to avoid unstable tails.
- Document threshold rationale and failure modes.

## Signal contract
Each signal proposal should define:
- Trigger conditions
- Cooldown / dedup policy
- Required historical lookback
- Confidence or severity fields
- Reason codes / evidence fields
- False-positive controls

## Implementation checklist
1. Input schema
2. Rolling state structure
3. Feature math
4. Decision/gating logic
5. Publish payload
6. Tests
7. Metrics/logs
8. Backtest or replay validation plan

## Tests required
- Unit tests for feature math and thresholds
- Edge cases: warmup, NaN, missing values, spikes, outliers
- Integration tests with Redis input/output
- Replay tests against recorded streams
- Load/latency budget checks for hot paths

## Observability
At minimum include:
- signals_emitted_total
- signals_suppressed_total
- detector_errors_total
- processing_latency_ms/us
- input_lag_ms
- warmup_state gauge
- quarantine_reason counters when input is bad

## Output style
Return file-by-file changes, exact function names, ENV/config additions, and test cases.\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- use Flash for bounded detector changes, threshold tweaks, and test scaffolding
- limit scope to touched detectors, features, and fixtures first

## Escalate to claude-sonnet-4-6/opus-4-6 if
- regime logic, execution-risk policy, or detector architecture is being redesigned
- ML/replay coupling changes materially
- the change affects multiple detectors or system-wide state semantics

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
