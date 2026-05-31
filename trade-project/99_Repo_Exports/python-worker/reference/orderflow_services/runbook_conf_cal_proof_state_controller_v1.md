# Runbook: Confidence Calibration Proof-State Controller (v1)

## Purpose
Generates proof state JSON for `confidence_cal_gating_mode=cal_after_proof`.

## Paths / ENV
- `CONF_CAL_LIVE_REPORTS_DIR` points to live loop reports directory
- `CONF_CAL_PROOF_STATE_PATH` proof json (read by strategy + exporter)
- `CONF_CAL_PROOF_CONTROLLER_STATE_PATH` internal controller state (streaks/ramp)

## Quick checks
1) Live status exists and is fresh:
   - `confidence_calibration_live_status.json`
2) Controller runs periodically (cron/systemd timer).
3) Proof file updates:
   - fields: `valid`, `evidence_ts`, `canary_share`, `source.status_age_sec`

## Common failures
- Proof read fails: wrong path/permissions
- Proof stale: live loop stopped, or always skipped, or no good evidence runs
- Canary stuck at 0: never reached `min_good_runs`

## Recovery
- Fix live loop + controller schedule
- Reset controller state file if ramp/streak corrupted (safe)
