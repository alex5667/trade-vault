# Runbook: OFInputs DLQ / Quarantine (P96)

## Streams

- Main stream (default): `signals:of:inputs`
- Quarantine stream (default): `quarantine:signals:of:inputs`
- DLQ stream (default): `stream:dlq:of_inputs`

All stream names are configurable via `runtime.config`:
- `of_inputs_stream`
- `of_inputs_quarantine_stream`
- `of_inputs_dlq_stream`

## What is considered "bad" for OFInputs V3

V3 is only emitted when **book_state is healthy enough** and required LOB-derived fields are present.

If degraded, the system does:

1) **Deterministic downgrade** V3 → V2
2) **Quarantine (dedup)** a triage event with:
   - `dq_code`
   - `missing_fields`
   - `book_age_ms`
3) Still publishes a **valid V2** payload to the main stream

## dq_code meanings (low cardinality)

- `v3_class_missing`: contract not deployed (safe fallback to V2)
- `book_age_missing`: cannot compute book age (missing timestamps)
- `book_stale`: `book_age_ms > of_inputs_v3_max_book_age_ms` (default 1500ms)
- `missing_lob_fields`: required LOB features are absent in `indicators`

## Metrics

Emitted by the tick-processor process:

- `of_inputs_downgrade_total{symbol,from_version,to_version,reason}`
- `of_inputs_missing_lob_total{symbol,reason}`
- `of_inputs_quarantined_total{symbol,reason,attempt_version,published_version}`
- `of_inputs_publish_error_total{symbol,stage}`

Exporter (separate process):

- `of_inputs_dlq_len{stream}`
- `of_inputs_dlq_age_sec{stream}`

## Quick triage

1) Check if DLQ/quarantine is growing:

- `of_inputs_dlq_len{stream="stream:dlq:of_inputs"}`
- `of_inputs_dlq_len{stream="quarantine:signals:of:inputs"}`

2) Drilldown (tail samples):

```bash
export REDIS_URL=redis://localhost:6379/0
python -m orderflow_services.of_inputs_dlq_drilldown_p96 --dlq 200 --quarantine 200 --samples 5
```

## Remediation playbook

### A) `missing_lob_fields`

Cause: LOB feature pipeline not producing required keys (`qimb_wmean`, `mp_mid_bps`, `obi_dw`, `ofi_ml_norm`).

Actions:
- Verify book ingestion is running and indicators are updated.
- Check if the symbol has a live book stream.
- Inspect book_state health metrics (book age / reconnects) and WS stability.

### B) `book_stale`

Cause: book snapshot age too old vs tick time.

Actions:
- Investigate WS book lag / reconnect storms.
- Reduce compute pressure if event loop is blocked.
- Consider increasing `of_inputs_v3_max_book_age_ms` only if your latency budget allows.

### C) DLQ is non-empty (`of_inputs_publish_error_total` increases)

Cause: Redis errors / serialization failures.

Actions:
- Check Redis health, memory pressure, maxmemory policy.
- Validate stream maxlen settings.
- Inspect DLQ samples for `err_prefix`.

## Recommended config knobs

- `of_inputs_emit_v3` (0/1)
- `of_inputs_v3_max_book_age_ms` (default 1500)
- `of_inputs_quarantine_cooldown_ms` (default 6h)
- `of_inputs_stream_maxlen` (default 50k)
- `of_inputs_dlq_maxlen` (default 200k)


## Auto replay (P97)

When DLQ growth is caused by transient Redis issues, you can replay the stored payloads back into the main stream.

Tool:

```bash
export REDIS_URL=redis://localhost:6379/0
# Dry-run (counts only)
python -m orderflow_services.of_inputs_dlq_fixed_replay_p97

# Commit mode (replays + ACKs)
OF_INPUTS_DLQ_COMMIT=1 python -m orderflow_services.of_inputs_dlq_fixed_replay_p97
```

State (exported via `of_inputs_dlq_exporter_v1`):
- `of_inputs_dlq_replay_last_ok`
- `of_inputs_dlq_replay_last_ok_age_sec`
- `of_inputs_dlq_replay_last_replayed` / `..._skipped` / `..._failed`

Notes:
- Replay only ACKs DLQ entries when nested `payload` parses as JSON and has minimal keys (`v,symbol,ts_ms`).
- On publish failure it leaves the message pending (will be auto-claimed later).
