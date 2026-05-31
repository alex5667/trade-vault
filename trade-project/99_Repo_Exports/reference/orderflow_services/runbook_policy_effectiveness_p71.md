# P71 Runbook — Policy effectiveness report

## What it is
The **policy effectiveness report** quantifies the trade-off between *coverage* (how often the system is in ok/warn/block) and *quality* (expectancy, precision@top5%, ECE) for the last 24 hours.

It is designed to make threshold calibration (P68/P69) **observable and safe**.

## Data sources
- Input: Redis stream `trades:closed` (last 24h window).
- Field required on each trade: `policy_effective_mode` (expected values: `ok`, `warn`, `block`; everything else -> `unknown`).

## Where outputs go
### 1) Full report payloads (Redis strings)
- JSON: `reports:policy_effectiveness:p71:last_json`
- CSV: `reports:policy_effectiveness:p71:last_csv`

### 2) Prometheus-exported snapshot (cfg2 keys)
These keys are written to `settings:dynamic_cfg` and exported by `meta_cov_rollout_exporter_v1.py`:

- `policy_effectiveness_last_ts_ms`
- `policy_effectiveness_baseline_ok_present`
- `policy_effectiveness_share_24h_{ok|warn|block|unknown}`
- `policy_effectiveness_expectancy_r_delta_24h_{ok|warn|block|unknown}`
- `policy_effectiveness_precision_top5p_delta_24h_{ok|warn|block|unknown}`
- `policy_effectiveness_ece_delta_24h_{ok|warn|block|unknown}`

## How to interpret
### Coverage
- `policy_effectiveness_share_24h{mode="warn"}`: share of trades closed while policy was in warn.
- If `unknown` share is non-trivial, the report is not reliable (field propagation issue).

### Deltas vs ok baseline
All deltas are computed as **mode − ok**.
- Expectancy delta < 0 ⇒ mode is worse than ok.
- Precision delta < 0 ⇒ top-ranked signals are less precise than ok.
- ECE delta > 0 ⇒ calibration is worse than ok.

A typical safe calibration loop:
1) Ensure `unknown` share ~ 0.
2) Check that **warn** is not materially worse than ok (or accept it intentionally as a safety trade-off).
3) Check that **block** has very low share (unless intentionally in maintenance), and if it occurs, it should typically correspond to data-quality issues (verify DQ metrics).

## What to do when alerts fire
### PolicyEffectivenessReportStale
1) Run the worker manually:
   - `python3 tools/policy_effectiveness_report_worker_v1.py --once`
2) Check Redis connectivity and stream length:
   - `redis-cli XLEN trades:closed`
3) Check `POLICY_EFF_MAX_SCAN` and lookback window (too small max_scan can lead to no samples).

### PolicyEffectivenessUnknownShareHigh
1) Inspect a few recent `trades:closed` entries:
   - verify `policy_effective_mode` exists and is populated.
2) Trace propagation: decision → trade open → trade close.
3) If field is missing in upstream components, fix the trade record schema/serializer.

### PolicyEffectivenessWarnMuchWorseThanOk
1) Verify sample sizes (N ok/warn in last 24h).
2) Validate that warn is not a proxy for a specific bad regime (e.g., DQ flags / stale book).
3) Consider recalibrating thresholds:
   - move warn threshold to reduce false-warn
   - add hysteresis / cooldown to avoid oscillation
   - tighten data-quality gate so warn triggers only on real risk
4) Re-check trade-off after change.

## Suggested scheduling
Run every 5–15 minutes (worker reads last 24h; cost dominated by stream scan).
Tune:
- `POLICY_EFF_MAX_SCAN` (default 200k)
- `POLICY_EFF_LOOKBACK_H` (default 24)
