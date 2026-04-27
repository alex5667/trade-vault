# Soak (24h) — DQ Level share + calibration helper

Goal
  Validate that SAFE thresholds do not produce false-reds on a healthy stream,
  and collect empirical distributions to calibrate STRICT.

Inputs
  - Capture NDJSON from B6/B7 (golden capture / decision records).
  - One JSON object per line.

Run
  ```bash
  python3 ml_analysis/tools/soak_dq_level_share_v1.py /path/to/capture.ndjson \
    --max-points 20000
  ```

What to look at
  - dq2 share per symbol
    - SAFE target: typically << 10% (ideally < 1–3% on stable symbols)
    - If dq2 share spikes on many symbols simultaneously:
        suspect upstream latency/clock skew, not thresholds
  - book_missing_seq_ema p99
    - If p99 is near your hard threshold, you will see flapping.
      Increase hard threshold or reduce alpha (more smoothing) for that stream.

Calibration knobs
  - dq_book_seq_ema_alpha: choose by book stream frequency
      10Hz -> 0.05..0.10
       4Hz -> 0.20
       2Hz -> 0.30
       1Hz -> 0.30..0.50
  - thresholds:
      book_hard / gap_soft_ms / gap_hard_ms / tick_soft / tick_hard

Operational notes
  - Keep observe-only enabled for book hard-veto for the first 24–48h after deploy.
  - Do NOT attach high-cardinality labels (stream_id, reason strings) to Prom counters.
