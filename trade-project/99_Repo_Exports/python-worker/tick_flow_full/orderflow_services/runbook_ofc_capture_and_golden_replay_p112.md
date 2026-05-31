# Runbook: OFC capture + Golden replay monitoring (P112)

## What exists

**Capture sidecar stats**
Writers persist JSON under:

- `<OFC_CAPTURE_DIR>/_state/ofc_capture_stats_<host>-<pid>.json`

Fields (schema `ofc_capture_stats_v1`): `written_total`, `bytes_total`, `errors_total`, `sampled_out_total`,
`last_write_ts_ms`, `last_error_ts_ms`, `last_error`, `last_path`.

**Nightly job state**
Nightly tool writes:

- `<GOLDEN_REPLAY_OUTDIR>/_state/gr_state_v1.json`

Fields (schema `gr_state_v1`): `day`, `policy_cnt`, `mismatches_total`, `last_ok_day`, `updated_ts_ms`.

**Exporter**
`orderflow_services/golden_replay_capture_exporter_p112.py` exposes Prometheus metrics from the files above.

## Alerts

- `OFCCaptureStuck`: no writes for >3m (warn)
- `OFCCaptureHighErrorRate`: >=5 errors / 5m (crit)
- `GoldenReplayNightlyStale`: no update >26h (warn)
- `GoldenReplayNightlyMismatch`: mismatches_total > 0 (crit)

## Triage checklist

1. Is exporter up?
   - `curl -sS localhost:$GR_CAPTURE_EXPORTER_PORT/metrics | grep ofc_capture_last_write_ts_ms`

2. Is capture enabled?
   - env: `OFC_CAPTURE_ENABLE=1`
   - optional: `OFC_CAPTURE_SAMPLE_PPM` (default 10000 = 1%)

3. Are state files updating?
   - `ls -lt <OFC_CAPTURE_DIR>/_state/ | head`
   - `cat <GOLDEN_REPLAY_OUTDIR>/_state/gr_state_v1.json`

4. If capture stuck:
   - check worker logs around last_error_ts_ms
   - check disk space, permissions, FS errors
   - confirm policy hash changing (expected) but still should write

5. If nightly stale:
   - check systemd timer for `scanner-golden-replay.timer`
   - run manually:
     - `python -m ml_analysis.tools.nightly_golden_replay_job_v1 --help`

## Rollback

- Disable capture: set `OFC_CAPTURE_ENABLE=0` (runtime safe).
- Disable exporter: stop service `scanner-gr-capture-exporter.service`.
- Disable nightly timer: stop/disable `scanner-golden-replay.timer`.
