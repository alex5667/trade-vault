# ExecHealth rollout preflight wrappers (v1)

P19 moves rollout-gate enforcement one layer earlier: sensitive ops jobs call a small preflight guard **before** the containerized Python entrypoint starts.

## Guarded jobs

- ACL policy apply
- override commit-thaw

## Generic preflight

```bash
python -m orderflow_services.exec_health_freeze_rollout_preflight_v1 --purpose exec_health_freeze_acl_policy_apply
```

Exit codes:
- `0` — gate open
- `24` — rollout gate active
- non-zero other — Redis / identity / bootstrap failure

## systemd wrappers

```bash
/opt/scanner_infra/orderflow_services/deploy/systemd/run-exec-health-freeze-acl-policy-apply-v1.sh
/opt/scanner_infra/orderflow_services/deploy/systemd/run-exec-health-freeze-override-commit-thaw-v1.sh
```

Both wrappers first run:
- `exec_health_freeze_rollout_preflight_v1`
- then `docker compose run --rm ...` for the sensitive job

## Manual usage

ACL apply:

```bash
sudo systemctl start exec-health-freeze-acl-policy-apply.service
```

Commit thaw:

```bash
OPERATOR=alice REQUEST_ID=req-123 sudo systemctl start exec-health-freeze-override-commit-thaw.service
```

## Notes

- this does **not** replace the in-process Python guard from P18; it adds an earlier gate
- preflight uses the audit Redis identity contract (`exec_health_freeze_rollout_preflight_v1`)
- if the rollout gate is latched, the compose container is not started at all
