# ExecHealth Freeze Reconnect Chaos Runbook (P16)

## Goal

Validate end-to-end reconnect self-healing for trusted Redis clients:

- client returns with correct `CLIENT SETNAME`
- client returns with correct `CLIENT SETINFO LIB-NAME`
- recovery event appears in `ops:exec_health:freeze_events:v1`
- `recovery_total` increments in heal-state
- `wrong_user` stays in violation path and is not self-healed

## Repairable reconnect drift

```bash
python orderflow_services/exec_health_freeze_reconnect_chaos_harness_v1.py \
  --service exec_health_freeze_override_v1 \
  --mode reconnect-both
```

Expected:

- `ok=true`
- `recovered=true`
- `after_entry.name` equals expected service client name
- `after_entry.lib-name` equals expected service lib-name
- `state.recovery_total >= 1`
- `event_id` non-empty

## Wrong user path

```bash
python orderflow_services/exec_health_freeze_reconnect_chaos_harness_v1.py \
  --service exec_health_freeze_override_v1 \
  --mode wrong-user \
  --wrong-user-url redis://default:<pass>@redis-worker-1:6379/0
```

Expected:

- `ok=false`
- error contains `wrong_user`
- no recovery event
- service remains in violation path

## Recommended CI subset

```bash
pytest -q \
  services/orderflow/tests/test_exec_health_freeze_reconnect_chaos_v1.py \
  orderflow_services/tests/test_exec_health_freeze_reconnect_chaos_harness_v1.py \
  orderflow_services/tests/test_exec_health_freeze_client_name_exporter_v1.py
```
