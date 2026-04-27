# ExecHealth Freeze Service Identity (v1)

## Goal
Ensure trusted ExecHealth Redis clients start only with the expected Redis user, CLIENT NAME and lib-name.

## Contract
- writer services -> `exec_health_freeze_writer`
- audit services -> `exec_health_freeze_audit`
- bootstrap services -> `exec_health_freeze_bootstrap`

Each trusted process must set:
- `CLIENT SETNAME <expected>`
- `CLIENT SETINFO LIB-NAME <expected>`

## Rollout blocker
```bash
python orderflow_services/exec_health_freeze_service_identity_blocker_v1.py
```

## Exporter
```bash
python orderflow_services/exec_health_freeze_service_identity_exporter_v1.py
```

## What to inspect
- `CLIENT LIST`
- service env with named-user DSNs
- alerts `OF_ExecHealth_FreezeServiceIdentityDrift_Crit`
