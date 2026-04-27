
# Tick Quality Gated Command Wrapper

## Purpose
This tool (`tools.run_tick_quality_gated_command`) allows wrapping any shell command with a "Tick Quality Gate" check.
It first queries Prometheus metrics to verify data quality (latency, freshness, known side).
- If the gate **PASSES**, the target command is executed.
- If the gate **FAILS**, the target command is NOT executed, and the wrapper exits with code 20.
- If there is **INSUFFICIENT DATA**:
  - `fail_closed` (default): Exit 21, do not run command.
  - `fail_open`: Run command anyway (warns).

## Usage

```bash
python -m tools.run_tick_quality_gated_command \
  --metrics-url http://metrics-service:8000/metrics \
  --window-s 60 \
  --fail-mode fail_closed \
  -- \
  <YOUR_COMMAND> [ARGS...]
```

## Arguments

- `--metrics-url`: URL to scrape Prometheus metrics (default: `http://localhost:8000/metrics`)
- `--window-s`: Observation window in seconds (default: 60)
- `--fail-mode`: `fail_closed` (block on missing data) or `fail_open` (proceed)
- `--symbol`: Optional symbol filter
- `--`: Separator before the target command

## Exit Codes

- `0`: Success (Gate passed + Command succeeded)
- `10`: Gate passed, but Command failed
- `20`: Gate FAILED (Threshold breach)
- `21`: Gate INSUFFICIENT DATA (Fail Closed)
- `22`: Internal Error

## Redis Audit

If `TICK_GATE_PUBLISH_REDIS=1` is set, the wrapper publishes a JSON event to `TICK_GATE_REDIS_STREAM` (default `ops:tick_quality_gate`) capturing the decision and result.
