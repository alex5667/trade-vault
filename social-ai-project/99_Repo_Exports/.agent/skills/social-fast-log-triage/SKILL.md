---
name: social-fast-log-triage
description: Fast lane to triage logs/metrics for a single social-ai symptom (stream lag, DLQ growth, LLM schema rejects, publish failures, freshness lag) and propose the next cheapest diagnostic.
---

When the user types `/social-fast-log-triage <symptom>`, do a quick bounded triage.

## Steps
1. Restate symptom + which signal shows it (metric/log/stream/DLQ/PEL).
2. List 2-4 likely causes ranked by probability.
3. Give the single cheapest next diagnostic (exact command/query/metric) to confirm/deny the top cause.
4. If it spans subsystems → escalate to **social-parallel-investigation**.

## Quick map
- `social_stream_pel_pending_total` ↑ → stuck consumer / poison row → check PEL, claim
- `social_stream_dlq_total` ↑ → schema/validation failure → inspect quarantine
- `llm_json_invalid_total`/`llm_schema_reject_total` ↑ → prompt/model drift → check pinned versions
- `publish_failure_total` ↑ → adapter/quota/policy → check reason label
- `social_source_freshness_lag_ms` ↑ → collector/quota stall

## Model lane
**claude-haiku-4-5 (fast mode)**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
