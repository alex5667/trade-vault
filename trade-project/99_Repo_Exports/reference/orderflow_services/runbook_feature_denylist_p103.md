# Runbook: Feature Denylist Actionable Output (P103)

## Purpose
Close the loop for "feature selection -> actionable output":

1) Feature-selection loop produces `stability_table.csv`.
2) `autogen_feature_denylist_proposal_v1` generates a candidate denylist proposal (`pending_ab`).
3) Nightly AB runner evaluates the proposal on a deterministic replay dataset:
   - `feature_denylist_replay_ab_v1` transitions `pending_ab -> ab_done` or `ab_failed`.
4) Human approval is required to mark it `approved` (no auto-apply).

## Signals / Metrics
- `feature_denylist_proposals_total{status="pending_ab"}`
- `feature_denylist_oldest_pending_age_seconds`
- `feature_denylist_ab_runner_*` (age/pending/processed/fail)

## Alerts
### FeatureDenylistPendingABStale
Oldest pending AB proposal is older than 72 hours.

**Fast check**
```bash
ls -lt proposals/denylist_proposal_*.manifest.json | head
jq -r '.status,.proposal_hash,.created_utc' proposals/denylist_proposal_*.manifest.json | head -n 30
```

**Action**
1) Run AB runner manually (safe):
```bash
python -m ml_analysis.tools.nightly_feature_denylist_ab_runner_v1 \
  --proposals-dir ./proposals \
  --max-pending 1
```
2) If it fails, inspect stderr tail printed by runner and the AB run dir:
```bash
ls -lt proposals/ab_runs | head
```

### FeatureDenylistABRunnerStale
AB runner is not ticking (Redis metrics not refreshed).

### FeatureDenylistApprovedNotAppliedStale
Oldest approved proposal (status=approved) is older than 24 hours.

**Fast check**
```bash
jq -r 'select(.status=="approved") | .proposal_hash,.approved_utc' proposals/denylist_proposal_*.manifest.json | head -n 20
```

**Action**
Apply the approved proposal:
```bash
python -m ml_analysis.tools.apply_feature_denylist_proposal_v1 \
  --manifest <manifest> \
  --apply 1
```

**Action**
- Ensure timers worker is running and `ENABLE_FEATURE_DENYLIST_AB_RUNNER=1`.
- Ensure exporter is running (textfile collector):
  - `python -m ml_analysis.tools.feature_denylist_proposal_exporter_v1`

### FeatureDenylistABRunnerFailing
At least one AB run failed.

**Action**
- Inspect the manifest mentioned in runner stdout tail.
- Re-run AB with higher verbosity by calling replay tool directly:
```bash
python -m ml_analysis.tools.feature_denylist_replay_ab_v1 --manifest <path>
```

## Approval workflow (mandatory)
After `ab_done`:

1) Validate AB report path in manifest:
```bash
jq '.ab' <manifest>
```

2) Approve (status -> approved) only if guard passed:
```bash
python -m ml_analysis.tools.approve_feature_denylist_proposal_v1 \
  --manifest <manifest> \
  --approve 1
```

3) Apply (status -> applied) to update the active denylist json:

```bash
python -m ml_analysis.tools.apply_feature_denylist_proposal_v1 \
  --manifest <manifest> \
  --apply 1
```

If you prefer code-review workflow, you can still apply the generated patch in a separate PR.

## Rollback
If a denylist was applied and quality regressed:
- revert the denylist JSON changes (single file) and redeploy training.
- keep the proposal manifests for audit.
