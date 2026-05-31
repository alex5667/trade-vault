# Runbook: Signal quality (DQ / gaps / seq)

## What to look at first (5 minutes)
1) `dq_level` and `dq_veto_total{bucket}` per symbol  
2) `tick_gap_p95_ms` + `tick_gap_n` (make sure there are enough samples)  
3) `tick_missing_seq_ema` and `book_missing_seq_ema`  
4) Decision record fields: `dq_reasons`, `dq_reason_bucket`, `book_seq_last_reason`, `tick_seq_last_reason`

## Typical causes
- **tick_gap_p95_ms spikes**: websocket stall, redis lag, GC pauses, CPU saturation.
- **tick_missing_seq_ema**: upstream drops/reconnects, out-of-order delivery, consumer lag.
- **book_missing_seq_ema**: depth stream resync, missing depthUpdate events, snapshot overlap mismatch.
- **dq_level==2 share**: usually means thresholds too strict for current feed frequency, or real stream instability.

## SAFE vs STRICT operations
- Start rollouts in **SAFE** mode (observe-only for book veto, softer thresholds).
- Switch to **STRICT** only after you have stable baselines for:
  - tick_gap_p95_ms distribution
  - tick_missing_seq_ema distribution
  - book_missing_seq_ema distribution

## Mitigation playbook
- If `tick_gap_p95_ms` is spiking:
  - check consumer lag / redis latency / network drops
  - temporarily increase `gap_*` thresholds or reduce gating strictness
- If `book_missing_seq_ema` is elevated during warmup:
  - keep `dq_book_veto_enabled=false` until observe-only window passes
  - confirm depth stream frequency and EMA alpha match (10Hz vs 4Hz vs 2Hz)

## Rollback
- Disable book veto: `dq_book_veto_enabled=false`
- Revert alert bundle to previous version if flapping.
