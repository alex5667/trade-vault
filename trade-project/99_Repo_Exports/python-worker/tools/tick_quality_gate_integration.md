# Tick Quality Gate (Step 18)

This document shows how to use `tools.tick_quality_gate_check` as a gate for
metric-gated ramp / deployment decisions.

## Quick run

```bash
python -m tools.tick_quality_gate_check --metrics-url http://localhost:8000/metrics --window-s 60
python -m tools.tick_quality_gate_check --metrics-url http://localhost:8000/metrics --window-s 60 --symbol BTCUSDT
```

Exit codes:
* 0 - PASS
* 2 - FAIL (threshold breached)
* 1 - INSUFFICIENT_DATA (missing metrics / no samples)

## Suggested ENV (conservative defaults)

```bash
TICK_GATE_WINDOW_S=60
TICK_GATE_PROCESS_P99_MS=25
TICK_GATE_E2E_P99_MS=5000
TICK_GATE_UNKNOWN_SIDE_EMA=0.10
TICK_GATE_TS_NOW_EMA=0.05
TICK_GATE_TS_STREAM_ID_EMA=0.20
TICK_GATE_SKEW_EMA_MS=30000
TICK_GATE_AGE_EMA_MS=30000
```

## Example: gate a ramp script

Pseudo-code:

```python
import subprocess

cmd = [
  'python', '-m', 'tools.tick_quality_gate_check',
  '--metrics-url', metrics_url,
  '--window-s', '60',
  '--json',
]
proc = subprocess.run(cmd, capture_output=True, text=True)
if proc.returncode == 2:
    # FAIL -> halt ramp
    raise SystemExit('tick-quality gate failed: ' + proc.stdout)
elif proc.returncode == 1:
    # insufficient -> fail-open OR fail-closed depending on your policy
    pass
```

## Notes

* The tool relies on histograms added in Step 17 and EMA gauges from Step 15.
* For high symbol counts, use Step 16 (`collapse` label mode) to keep
  cardinality manageable.
