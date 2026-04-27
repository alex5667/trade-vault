---
name: trade-data-quality-time
description: Use this skill when the task involves timestamps, event ordering, epoch seconds vs milliseconds vs microseconds, timezone handling, bad market data, out-of-order ticks, duplicate klines, sanitization, quarantine, or data quality metrics in the trade project. Relevant for Russian/Ukrainian prompts about время, таймштампы, качество данных, монотоничность, bad time, detect sanitize quarantine metrics.
---

# Trade Data Quality and Time

## Goal
Protect the trade pipeline from corrupted, ambiguous, or non-deterministic time/data inputs.

## Use this skill for
- Tick/kline timestamp normalization
- Redis payload validation
- Out-of-order or duplicate market data
- Timezone mistakes in backend or UI
- Data quality guards before signal generation
- Stream replay correctness and deterministic reprocessing

## Required analysis steps
1. Identify every timestamp field and its source.
2. State the canonical internal format.
3. Define normalization rules for incoming units (`s`, `ms`, `us`).
4. Define bad-data detection rules.
5. Define sanitize/quarantine behavior.
6. Define metrics, logs, and alerts.
7. Define replay/test cases.

## Canonical defaults
- Internal event time: `epoch_ms` UTC unless the task explicitly requires another unit.
- Human-readable serialization: ISO8601 with timezone offset or `Z`.
- Event ordering must be based on exchange event time first, ingest time second.

## Validation checklist
- Missing timestamp
- Non-numeric timestamp
- Timestamp outside sane bounds
- Unit mismatch (`s` mistakenly treated as `ms`)
- Backward jump beyond allowed threshold
- Future timestamp beyond drift budget
- Duplicate event id / duplicate `(symbol, ts, seq)`
- Negative prices/volumes or impossible OHLC relations

## Required behavior
- Detect -> sanitize -> quarantine -> metrics
- Never silently coerce suspicious data without logging the reason
- Preserve original raw value when sanitizing
- Emit reason codes suitable for dashboards and alerts

## Preferred techniques
- Use robust summaries for noisy streams: median, MAD, percentile bands
- Keep normalization deterministic and side-effect free
- Prefer explicit helper functions like `normalize_epoch_to_ms()`
- Separate validation from business logic

## Output requirements
When proposing code, include:
- Canonical DTO/schema
- Normalization function(s)
- Quarantine path or stream/key/table
- Metrics names
- Alert thresholds
- Replay tests for good/bad/out-of-order data

## Example reason codes
- `ts_missing`
- `ts_non_numeric`
- `ts_unit_mismatch`
- `ts_too_old`
- `ts_too_far_future`
- `event_out_of_order`
- `duplicate_event`
- `ohlc_invalid`\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- focus on the changed timestamp fields, units, and ordering assumptions
- prefer local sanitize/quarantine fixes over system-wide redesign

## Escalate to claude-sonnet-4-6/opus-4-6 if
- source-of-truth ambiguity remains unresolved
- multiple services disagree on time semantics
- a repo-wide time contract redesign is needed

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
