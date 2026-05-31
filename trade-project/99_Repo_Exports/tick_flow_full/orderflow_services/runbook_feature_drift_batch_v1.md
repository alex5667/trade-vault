# Runbook: Feature Drift Batch (PSI / KS)

## Goal
Explain *which* Tier-1 features drifted between a stable reference window and the
current 24h/7d window.

## Signals
- `feature_drift_batch_warn_n`
- `feature_drift_batch_crit_n`
- `feature_drift_batch_denylist_suggest_n`
- `feature_drift_batch_feature_psi{feature=...}`
- `feature_drift_batch_feature_ks_stat{feature=...}`

## Source of truth
Redis hash:
```bash
redis-cli HGETALL metrics:feature_drift_batch:last
```

The hash points to the JSON report via `report_json`.

## Triage
1. Open the report JSON/CSV and identify whether drift is:
   - true distribution shift (`psi`, `ks_stat`)
   - missing-rate shift
   - zero-inflation shift
   - clipping / outlier-rate shift
2. Cross-check the feature against:
   - feature selection loop
   - denylist proposal workflow
   - live EMA/z drift and DQ metrics
3. If drift is critical and recent, prefer **shadow-disable** before hard denylist.
4. If drift persists across several days and feature is not core-protected, run denylist AB workflow.

## Manual rerun
```bash
python -m services.nightly.feature_drift_report_v1 \
  --reference_path /path/to/reference.jsonl \
  --current_path /path/to/current.jsonl \
  --out_json /tmp/feature_drift_batch.json
```

## Suggested next action
If the report sets `denylist_suggested=1` for several features, feed the JSON to
`autogen_feature_denylist_proposal_v1 --drift_report_json ...`.
