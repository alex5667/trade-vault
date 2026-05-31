# OFC_CAPTURE + Nightly Golden Replay (B6)

## Goal
Make Train==Serve parity *operational*:

1) runtime captures deterministic inputs/outputs (NDJSON)
2) nightly job replays those decisions offline and fails if drift is detected

This closes the loop:
runtime -> captured decisions -> replay -> alert/CI gate.

## Enable capture (runtime)

### Minimal enable

Environment:

```
OFC_CAPTURE=1
OFC_CAPTURE_DIR=/var/lib/scanner/ofc_capture
```

Or cfg2:

```
ofc_capture_enable=1
ofc_capture_dir=/var/lib/scanner/ofc_capture
```

### Deterministic sampling

Sampling is *deterministic* per stable_key = `symbol|direction|ts_ms`.

Defaults:

* `OFC_CAPTURE_SAMPLE_PPM=1000` (0.1%)
* `OFC_CAPTURE_SEED=ofc_cap_v1`

Examples:

* 1%: `OFC_CAPTURE_SAMPLE_PPM=10000`
* 0.01%: `OFC_CAPTURE_SAMPLE_PPM=100`

### Storage layout

Capture writes:

```
<OFC_CAPTURE_DIR>/<YYYYMMDD>/policy_<dq_policy_hash>/decisions-<host>-<pid>-<seq>.ndjson
```

One file per process (no cross-process locks).

### Rotation

Defaults:

* `OFC_CAPTURE_MAX_BYTES=268435456` (256MB)
* `OFC_CAPTURE_ROTATE_SEC=3600` (hourly)

## Nightly replay job

Tool:

```
python -m ml_analysis.tools.nightly_golden_replay_job_v1 --fail-on-mismatch
```

Defaults:

* date = yesterday (UTC)
* outdir = `/var/lib/scanner/golden_replay_reports`
* retention = `OFC_CAPTURE_KEEP_DAYS` (default 10)

Outputs:

* `report_<YYYYMMDD>.json`
* per-policy parity outputs under `<outdir>/<YYYYMMDD>/<policy_hash>/...`

## systemd timer

Files:

* `scanner-infra/systemd/scanner-golden-replay.service`
* `scanner-infra/systemd/scanner-golden-replay.timer`

Install example:

```
sudo install -m 0644 scanner-infra/systemd/scanner-golden-replay.service /etc/systemd/system/
sudo install -m 0644 scanner-infra/systemd/scanner-golden-replay.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now scanner-golden-replay.timer
```

Adjust `WorkingDirectory` + `PYTHONPATH` to your repo path.

## Operational thresholds / guidance

* Start capture at 0.1% (1000 ppm) for 24h.
* If parity is clean, raise to 1% for higher confidence.
* Keep capture retention small (7-14 days) to limit disk usage.

## Failure modes

* `no_data`: capture not enabled, wrong dir, or zero sampling.
* `mixed_policy`: indicates policy hash changed during the day; split by policy dirs is expected.
* parity mismatch: code drift, non-deterministic inputs, or missing capture fields.

## Security / privacy

Captured evidence can include strategy internals. Restrict access and consider encrypting backups.
