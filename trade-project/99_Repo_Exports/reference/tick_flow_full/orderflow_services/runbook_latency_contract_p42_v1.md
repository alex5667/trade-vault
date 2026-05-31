# Latency Contract Rollout Gate (P4.2)

This guard blocks sensitive rollout/apply paths when cross-service latency
coverage is incomplete or when latency budgets remain breached for too long.

## Block conditions

- missing external stage coverage:
  - `go_ingest / ingest_to_redis`
  - `nest_gateway / emit_to_ws`
  - `nest_gateway / end_to_end_event`
- `budget_breach_total > 0` for at least `LATENCY_CONTRACT_ROLLOUT_GATE_BUDGET_HOLD_S`

## Keys

- SLO summary: `metrics:latency_contract:slo:last`
- rollout gate state: `metrics:latency_contract:rollout_gate:last`
- active gate key: `cfg:orderflow:latency_contract:rollout_gate:v1`

## Preflight

```bash
python3 -m orderflow_services.latency_contract_rollout_preflight_v1 --purpose latency_contract_sensitive_apply
```

## Generic wrapper

```bash
orderflow_services/integrations/run_with_latency_contract_rollout_preflight_v1.sh <command...>
```

## Typical remediation

1. Confirm external writers are alive and writing hashes for Go/NestJS stages.
2. Inspect `metrics:latency_contract:slo:last` and `metrics:latency_contract:rollout_gate:last`.
3. If the block is due to sustained budget breach, resolve the latency issue first; do not bypass the gate.
