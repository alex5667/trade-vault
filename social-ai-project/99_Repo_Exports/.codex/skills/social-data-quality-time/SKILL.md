---
name: social-data-quality-time
description: Use this skill when the task involves timestamps, event ordering, epoch s/ms/us, timezone handling, bad/duplicate social metrics, out-of-order metric snapshots, sanitization, quarantine, or data quality metrics in the social-ai project. Relevant for Russian/Ukrainian prompts about время, таймштампы, качество данных, монотоничность, дубликаты, detect sanitize quarantine metrics для социальных метрик.
---

# Social Data Quality and Time

## Goal
Protect the social pipeline from corrupted, ambiguous, or non-deterministic time/data inputs (platform metrics, snapshots, webhook events).

## Use this skill for
- Metric-snapshot timestamp normalization
- Redis/stream payload validation
- Out-of-order or duplicate platform-metric snapshots
- Timezone mistakes in backend or UI
- Data quality guards before trend scoring / content planning
- Replay correctness and deterministic reprocessing

## Required analysis steps
1. Identify every timestamp field and its source (platform vs ingest vs render vs publish).
2. State the canonical internal format.
3. Define normalization rules for incoming units (`s`, `ms`, `us`).
4. Define bad-data detection rules.
5. Define sanitize/quarantine behavior.
6. Define metrics, logs, alerts.
7. Define replay/test cases.

## Canonical defaults
- Internal event time: `epoch_ms` UTC unless the task explicitly requires another unit.
- Human-readable serialization: ISO8601 with TZ offset or `Z`.
- Event ordering: platform/source event time first, ingest time second.

## Validation checklist
- Missing / non-numeric timestamp
- Timestamp outside sane bounds; unit mismatch (`s` treated as `ms`)
- Backward jump beyond threshold; future timestamp beyond drift budget
- Duplicate snapshot `(platform, object_id, snapshot_ts)`
- Impossible metric relations (views < likes? negative counts? CTR > 1, retention > 1)
- Counter resets / non-monotonic cumulative metrics (views/likes decreasing)

## Required behavior
- Detect -> sanitize -> quarantine -> metrics. Never silently coerce; log the reason.
- Preserve original raw value when sanitizing.
- Emit reason codes suitable for dashboards/alerts.
- For cumulative platform counters, compute deltas defensively (clamp negatives, flag resets).

## Preferred techniques
- Robust summaries for noisy engagement streams: median, MAD, percentile bands, winsorization.
- Deterministic, side-effect-free normalization (`normalize_epoch_to_ms()`).
- Separate validation from business logic.

## Example reason codes
`ts_missing`, `ts_non_numeric`, `ts_unit_mismatch`, `ts_too_old`, `ts_too_far_future`, `snapshot_out_of_order`, `duplicate_snapshot`, `metric_impossible`, `counter_reset`, `ratio_out_of_range`

## Output requirements
Canonical DTO/schema, normalization function(s), quarantine stream/table, metric names, alert thresholds, replay tests for good/bad/out-of-order/duplicate data.

## Default lane
**claude-haiku-4-5 (fast mode)** by default. Escalate if source-of-truth ambiguity persists, services disagree on time semantics, or a repo-wide time-contract redesign is needed.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
