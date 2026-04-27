# Runbook: Meta AB v2 (P70)

## What it is

**Meta AB v2** is a nightly champion/challenger evaluator for the meta-model.
It reads the latest dataset, scores champion vs challenger, recommends the next
challenger share, and writes a JSON report.

This runbook covers:
1) Nightly job health
2) Prometheus exporter health
3) Common failures and safe recovery

---

## Key artifacts

- Nightly job module: `tools.meta_ab_v2_nightly_job_v1`
- Report JSON: `/var/lib/trade/of_reports/meta_ab_v2_report.json`
- Exporter module: `tools.meta_ab_v2_report_exporter_v1`

---

## SLO / What to alert on

- **Report freshness**: `meta_ab_v2_report_age_sec` should be < **30h**.
- **Report parsed OK**: `meta_ab_v2_report_parsed_ok` should be `1`.
- **Run OK**: `meta_ab_v2_run_ok` should be `1`.
- **Dataset health**:
  - `meta_ab_v2_n_total` should be > 0
  - `meta_ab_v2_n_eligible` should not be unexpectedly near 0 for long periods

---

## Quick triage

### 1) Check report age

- Look at `meta_ab_v2_report_age_sec` in Prometheus/Grafana.
- If it is increasing past ~30h, the nightly job likely stopped running.

### 2) Check job logs

Depending on how you run nightlies:

- **systemd timer**: `journalctl -u <your-nightly-unit> --since "36 hours ago"`
- **docker exec from scheduler**: `docker logs <your-timers-container> --tail 200`

### 3) Validate required inputs exist

- Dataset parquet exists and is fresh (mtime reasonable):
  - `/var/lib/trade/of_reports/datasets/meta_inputs_outcomes_v2.parquet`
- Both models exist (champion & challenger):
  - `/var/lib/trade/of_reports/models/meta_champion.joblib`
  - `/var/lib/trade/of_reports/models/meta_challenger.joblib`

---

## Common failure modes

### A) Report missing

Symptoms:
- `meta_ab_v2_report_parsed_ok = 0`

Actions:
1) Run nightly job manually once (dry-run):
   - `ENABLE_META_AB_V2_NIGHTLY=1 META_AB_V2_APPLY=0 python -m tools.meta_ab_v2_nightly_job_v1`
2) If it fails: inspect error text; typical causes are missing parquet/models.

### B) Report stale

Symptoms:
- `meta_ab_v2_report_age_sec` > 108000 (30h)

Actions:
1) Ensure the scheduler is firing.
2) Ensure the container/host has correct time.
3) If a timer was disabled by a flag, re-enable:
   - `ENABLE_META_AB_V2_NIGHTLY=1`

### C) Eligible collapses to ~0

Symptoms:
- `meta_ab_v2_n_eligible` drops near 0 for hours/days.

Actions:
1) Check `p_min` threshold used for eligibility:
   - `meta_ab_v2_p_min`
2) Validate upstream feature schema and input coverage.
3) Confirm the dataset builder isn’t emitting NaNs / missing columns.

---

## Safe recovery / Rollback

1) If the job output looks suspicious, keep apply off:
   - `META_AB_V2_APPLY=0`
2) If auto-apply exists downstream, disable the consumer (feature flag) first.
3) Remove/ignore the last report by rotating it (do not delete without backup).
