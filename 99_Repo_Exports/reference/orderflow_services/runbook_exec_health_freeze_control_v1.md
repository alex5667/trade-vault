# ExecHealth Freeze Control / Manual Ack (v1)

## Purpose

P7 adds a latched freeze-control workflow so the system cannot be thawed by simply
deleting the raw key:

- raw key: `cfg:orderflow:exec_health:auto_freeze:v1`
- control hash: `cfg:orderflow:exec_health:freeze_control:v1`
- autoguard state fallback: `metrics:exec_health:slo:autoguard:state`

Consumers read the control/state hashes first. A raw key delete alone is no longer
enough to resume publish/entry.

## Operator commands

Status:

```bash
python orderflow_services/exec_health_freeze_override_v1.py status
```

Manual thaw with explicit ack:

```bash
python orderflow_services/exec_health_freeze_override_v1.py thaw \
  --operator alex \
  --reason "validated rollback, scope mismatch resolved" \
  --ticket INC-42
```

Manual force-freeze:

```bash
python orderflow_services/exec_health_freeze_override_v1.py freeze \
  --operator alex \
  --reason "maintenance window" \
  --ticket CHG-17 \
  --minutes 30
```

## Contract

A valid thaw requires all of the following to be stored:

- `manual_ack_ts_ms`
- `manual_ack_operator`
- `manual_ack_reason`
- `manual_ack_ticket`
- `manual_ack_required=0`
- `manual_override_action=thaw`

## Operational notes

- If autoguard fires again after a thaw, it will relatch freeze and require a new manual ack.
- Prefer `thaw` only after confirming `cross_scope_mode_distinct` and
  `rollout_drift_instances_total` are back to normal.
- Watch exporter metrics:
  - `exec_health_freeze_control_effective_active`
  - `exec_health_freeze_control_manual_ack_required`
  - `exec_health_freeze_control_manual_override_active`
