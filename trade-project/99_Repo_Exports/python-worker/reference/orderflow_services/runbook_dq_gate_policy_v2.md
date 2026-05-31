# Runbook: DQ Gate Policy (v2)

Purpose
- Make DQ (data-quality) degradation observable and operationally actionable.
- Keep production risk low by starting in SAFE+penalty, then tightening to STRICT+enforce based on measured distributions.

Scope
- Runtime metrics produced by tick_processor strict-DQ trackers:
  - tick_gap_p95_ms (gauge, ms)
  - tick_missing_seq_ema (gauge, 0..1)
  - book_missing_seq_ema (gauge, 0..1)
- DQ decision logic in tick_flow_full/core/dq_gate_v1.py.

Key concepts

1) What the metrics mean

- tick_gap_p95_ms
  - Per-symbol p95 of inter-tick gaps over the recent snapshot window.
  - Interpreting spikes:
    - Short spikes: feed hiccup / Redis / websocket jitter.
    - Sustained > soft: data gets stale → confirmation decisions become noisy.
    - Sustained > hard/extreme: treat as hard DQ event (potential veto in enforce mode).

- tick_missing_seq_ema / book_missing_seq_ema
  - EMA of missing-sequence events (0..1), where 0 means “no gaps observed recently”.
  - These are “risk multipliers” for DQ: a high gap p95 without missing-seq might be benign (quiet market / low prints), but gap p95 + missing-seq strongly indicates feed loss or ordering faults.

2) SAFE vs STRICT

DQ_MODE controls defaults in dq_gate_v1 (unless overridden by explicit DQ_* thresholds):

- SAFE
  - Intended for rollout and noisy environments.
  - Defaults (approx):
    - tick_gap_p95_ms_soft ≈ 5000
    - tick_gap_p95_ms_hard ≈ 8000
    - tick_gap_p95_ms_extreme ≈ 12000
    - tick_missing_seq_ema_hard ≈ 0.25 (soft ≈ 0.125)
    - book_missing_seq_ema_hard ≈ 0.25 (soft ≈ 0.125)

- STRICT
  - Intended once you have baseline distributions from replay/AB.
  - Defaults (approx):
    - tick_gap_p95_ms_soft ≈ 3000
    - tick_gap_p95_ms_hard ≈ 4500
    - tick_gap_p95_ms_extreme ≈ 9000
    - tick_missing_seq_ema_hard ≈ 0.15 (soft ≈ 0.05)
    - book_missing_seq_ema_hard ≈ 0.10 (soft ≈ 0.03)

Note
- These defaults are deliberately conservative; you should calibrate them on real replay inputs.

3) DQ gate modes

DQ_GATE_MODE controls the action:

- off
  - No penalty/veto; only metrics.
- penalty
  - Apply soft penalty to final score; do not hard-veto.
- enforce
  - Hard-veto when dq_gate_v1 returns veto.
- both
  - Apply penalty and allow hard-veto.

Recommended rollout

Stage 0 (observe-only)
- DQ_GATE_ENABLE=0
- Ensure gauges exist and are non-zero in Grafana.

Stage 1 (SAFE+penalty)
- DQ_GATE_ENABLE=1
- DQ_GATE_MODE=penalty
- DQ_MODE=safe
- Run for 24h; review distributions and alert noise.

Stage 2 (STRICT+penalty)
- Keep DQ_GATE_MODE=penalty
- Switch DQ_MODE=strict
- Run for 24h; confirm the “quarantine share” and veto candidates are acceptable.

Stage 3 (STRICT+enforce)
- DQ_GATE_MODE=enforce (or both)
- Apply canary first (BTCUSDT only) before broad rollout.

Environment variables

Enable/mode
- DQ_GATE_ENABLE=0|1
- DQ_GATE_MODE=off|penalty|enforce|both
- DQ_MODE=safe|strict

Threshold overrides (optional; if unset → dq_gate_v1 defaults by DQ_MODE)
- DQ_TICK_GAP_P95_MS_SOFT
- DQ_TICK_GAP_P95_MS_HARD
- DQ_TICK_GAP_P95_MS_EXTREME
- DQ_TICK_GAP_REQUIRES_SEQ (1 recommended)

- DQ_TICK_MISSING_SEQ_EMA_SOFT
- DQ_TICK_MISSING_SEQ_EMA_HARD
- DQ_BOOK_MISSING_SEQ_EMA_SOFT
- DQ_BOOK_MISSING_SEQ_EMA_HARD

Also used by dq_gate_v1
- DQ_TICK_GAP_MIN_SAMPLES (default 50)
- DQ_PEN_MAX (default 0.10)

Alerting

This repo ships SAFE-default alerts in:
- orderflow_services/prometheus_alerts_dq_gate_policy_v2.yml

If you run STRICT, consider tightening alert thresholds accordingly (or add a STRICT rule group).

How to calibrate thresholds on real data (recommended)

Prereq
- You already archive replay inputs (signals:of:inputs) via replay_inputs_archiver.

Option A: evaluate directly from archive directory

Example:
  python3 orderflow_services/dq_threshold_eval_harness_p112.py \
    --archive-dir /var/lib/trade/archives/ml_replay_inputs_v1 \
    --start-ts-ms 1700000000000 --end-ts-ms 1700086400000 \
    --out-json /tmp/dq_eval.json --out-md /tmp/dq_eval.md

Option B: evaluate from an exported NDJSON slice

1) Export inputs:
  python3 ml_analysis/tools/export_replay_inputs_ndjson_v1.py \
    --archive-dir /var/lib/trade/archives/ml_replay_inputs_v1 \
    --start-ts-ms ... --end-ts-ms ... \
    --out /tmp/inputs.ndjson

2) Run harness:
  python3 orderflow_services/dq_threshold_eval_harness_p112.py \
    --inputs /tmp/inputs.ndjson --out-json /tmp/dq_eval.json

Interpreting the harness output
- Focus on p95/p99 of tick_gap_p95_ms and p99 of missing_seq_ema.
- Recommended heuristic for initial thresholds:
  - soft ≈ max(default_soft, p99)
  - hard ≈ max(default_hard, p99 * 1.2)
  - extreme ≈ max(default_extreme, p99 * 1.5)
- Validate changes via replay/AB before enforce.

On-call triage checklist (when DQ alerts fire)

1) Confirm the problem is real
- Check if multiple symbols fire simultaneously (infra issue) or isolated (symbol feed issue).
- Correlate with:
  - tick_time quarantine alerts (bad time)
  - redis latency / CPU / packet loss

2) Identify whether it’s “gap” or “seq”
- tick_gap_p95_ms high but missing_seq_ema near 0:
  - likely low activity or transient jitter; consider SAFE-only action.
- missing_seq_ema rising:
  - sequence gaps or ordering issues; treat as production risk.

3) Action
- If widespread: switch DQ_GATE_MODE to penalty (or off) temporarily to avoid mass veto.
- If isolated symbol: canary-disable symbol or move to quarantine.
- Preserve evidence:
  - capture 5–10 minutes of inputs from the replay archive around incident.
