# Runbook: Promote Freeze (Edge Stack)

## Owner
- team: **trade**
- component: **edge_stack**

## What it means
Promotions are blocked (auto-promote and guarded promote) for a limited time window.
Default freeze window: 24h.

This is a safety mechanism to prevent promoting models when monitoring routes/links/health are broken.

## Where state lives
Redis hash key:
- `cfg:edge_stack:promote_freeze` (override with `EDGE_STACK_PROMOTE_FREEZE_KEY`)

Fields:
- `until_ts_ms`
- `set_ts_ms`
- `reason`
- `source`

## Immediate actions
1) Find reason:
   - `HGETALL cfg:edge_stack:promote_freeze`
   - `HGETALL metrics:monitoring_smoke:last`

2) CLI controls (recommended):
   - status:
     `python -m ml_analysis.tools.promote_freeze_ctl status`
   - set:
     `python -m ml_analysis.tools.promote_freeze_ctl set --duration_s 3600 --reason "manual investigation"`
   - clear:
     `python -m ml_analysis.tools.promote_freeze_ctl clear`

   Audit stream:
   - `XREAD COUNT 10 STREAMS ops:eventlog 0-0`
2) Fix root cause:
   - public-proxy routes (Caddy)
   - Grafana/runbooks-web/Alertmanager/Prometheus health
   - Telegram webhook base URLs
3) If fixed and you want to clear early:
   - delete freeze key: `DEL cfg:edge_stack:promote_freeze`
   - or set `EDGE_STACK_PROMOTE_FREEZE_CLEAR_ON_SUCCESS=1` and rerun smoke job

## Rollback
If you suspect false positive:
- Set `MONITORING_SMOKE_FAIL_MODE=fail_open` (not recommended for prod)
- Or disable smoke nightly: `ENABLE_MONITORING_SMOKE_NIGHTLY=0`

## Links
- Grafana: `/d/edge_stack_overview/edge-stack-overview?orgId=1`
- Runbook uptime: `/web_uptime.md`
