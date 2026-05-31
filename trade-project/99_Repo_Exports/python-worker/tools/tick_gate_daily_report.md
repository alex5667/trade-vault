# Tick Gate Daily Report

This tool summarizes tick-quality gate outcomes written to a Redis Stream (typically `ops:tick_quality_gate`).

## Usage

```bash
export REDIS_URL=redis://redis-worker-1:6379/0
export TICK_GATE_REDIS_STREAM=ops:tick_quality_gate

# Text report (default window 24h)
python -m tools.tick_gate_daily_report --hours 24

# JSON (for dashboards)
python -m tools.tick_gate_daily_report --hours 24 --format json
```

## Expected payload

The stream entries may contain either:

1) A JSON field (`json` or `payload`) with structure similar to:

```json
{
  "ts_ms": 1730000000000,
  "status": "FAIL",
  "return_code": 20,
  "failures": [{"metric":"tick_ingest_process_ms.p99","value":40,"threshold":25}],
  "symbol": "BTCUSDT"
}
```

2) Or a flat set of fields (the tool merges them into a single dict).

The tool is intentionally tolerant: it normalizes status and will fallback to `return_code`
when status is missing.
