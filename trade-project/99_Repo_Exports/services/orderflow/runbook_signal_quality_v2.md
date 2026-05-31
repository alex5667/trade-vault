# Runbook: Signal quality (gap/seq + DQ hard share) v2

This runbook is designed to answer **"what do I do in 5 minutes"** when
`OF_SQ_*` alerts fire.

## 0) Immediate triage (60–90s)

1. Identify `symbol` from alert.
2. Check which family fired:
   - **Tick gap p95** (`tick_gap_p95_ms`, `tick_gap_n`) — latency/jitter.
   - **Tick missing seq EMA** (`tick_missing_seq_ema`) — continuity gaps.
   - **Book missing seq EMA** (`book_missing_seq_ema*`, `book_seq_last_gap_gauge`) — diff-depth chain broken.
   - **DQ hard share** (`dq_level==2 share`) — long-horizon integrity degradation.
3. If multiple symbols fire at once → suspect host/network-wide issue.

## 1) Diagnostics checklist

### A) Tick gap p95

Look at:
- `tick_gap_p95_ms{symbol=...}` and `tick_gap_n{symbol=...}`
- Host-level CPU throttling, GC pauses, container restarts.
- WS reconnect rate and network errors.

Interpretation:
- High `tick_gap_p95_ms` with healthy `tick_missing_seq_ema` often means *latency/jitter*, not lost data.

### B) Tick missing seq EMA

Look at:
- `tick_missing_seq_ema{symbol=...}`
- Reconnect/subscribe loops, dropped messages, parse errors.

Interpretation:
- Sustained EMA increase means real continuity gaps in tick stream.

### C) Book missing seq EMA (observe-only vs paging)

Look at:
- `book_missing_seq_ema_gauge{symbol=...}` (or legacy `book_missing_seq_ema`)
- `book_seq_last_gap_gauge{symbol=...}` (last detected `u/pu` discontinuity)
- Decision record fields (or logs):
  - `book_seq_last_reason`
  - `book_missing_seq_last_gap`

Then verify expected diff-depth frequency:
- ~10Hz (100ms) vs 4Hz (250ms) vs 2Hz (500ms)
- Ensure `dq_book_seq_ema_alpha` matches stream cadence.

**Observe-only window / gating**
- Verify `dq_book_veto_enabled` and `dq_observe_only_sec`.
- If you are within the first 24–48h after deploy/restart, `dq_level` may be 2 but `dq_veto` should be 0.

## 2) Actions

### A) SAFE mode (reduce blast radius)

If strict thresholds cause too many criticals (especially during warmup):
- Switch DQ mode to SAFE (your rollout policy).
- Keep capture/metrics enabled so you can see the problem clearly.

### B) Temporary threshold adjustments

If the stream is healthy but thresholds are mismatched to cadence:
- Increase `book_hard` and/or `gap_*` temporarily.
- Adjust `dq_book_seq_ema_alpha` to match real diff-depth frequency.

### C) Resync book stream

Book missing seq typically requires resync of snapshot:
- Trigger resync endpoint/command if available.
- Otherwise restart the book ingestion component (forces snapshot refresh).

### D) Host/network remediation

If multiple symbols fire simultaneously:
- Check host CPU steal/throttling, memory pressure, Docker health.
- Check network drops, DNS/resolution, upstream reachability.

## 3) Rollback

If paging on book seq is too aggressive or causing false positives:
- Disable `BOOK_VETO_ENABLED` (or `dq_book_veto_enabled=false`).
- Restore the previous Prometheus rules bundle (keep the new file for ticket-level visibility).

## 4) Notes for follow-up ticket

Include in the ticket:
- Time range, symbols affected
- `tick_gap_p95_ms`, `tick_missing_seq_ema`, `book_missing_seq_ema*`
- `book_seq_last_reason`, `book_missing_seq_last_gap`
- Effective DQ mode (SAFE/STRICT), `dq_book_veto_enabled`, and observe-only uptime
