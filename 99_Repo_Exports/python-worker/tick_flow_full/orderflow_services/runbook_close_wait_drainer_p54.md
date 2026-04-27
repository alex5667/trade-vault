# P54 Close Wait Drainer Runbook

## Purpose
trades:close_wait stores POSITION_CLOSED events that arrived before decision:{sid} was written.
The drainer retries and completes the join once decision:{sid} appears, writing enriched payload into trades:closed
(and optionally ml_replay_inputs_v1).

## Processes
- Drainer: python -m tools.close_wait_drainer_v1
- Exporter: python -m tools.close_wait_drainer_exporter_v1

## Key Streams / Keys
- Input stream: trades:close_wait
- Dead-letter: trades:close_dead
- Output: trades:closed, ml_replay_inputs_v1
- Decision: decision:{sid}
- Dedup: join:closed:{sid} (TTL)

## Common Issues
### 1) Backlog grows
Symptoms:
- close_wait_pending_count rising
Actions:
- ensure drainer running (staleness)
- increase throughput: CLOSE_WAIT_BATCH=2000, CLOSE_WAIT_LOOP_S=0.1
- check decision writer TTL (DECISION_TTL_SEC) and error rate

### 2) Missing decision rate high
Likely:
- decision writer not writing in veto path
- decision TTL too small
Checks:
- GET decision:<sid> for recent sid

### 3) Dead-letter entries
Inspect:
- XREVRANGE trades:close_dead + - COUNT 10
Reason codes:
- decision_missing max_attempts / max_age_ms
- bad_payload_no_sid
- exception <Type>

## Rollback
Stop drainer; it does not affect trade execution. It only improves enrichment and KPI coverage.
