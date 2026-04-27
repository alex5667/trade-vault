# Nightly monitoring smoke tests

## What is checked
1) Public proxy routes health:
- `/grafana/api/health`
- `/runbooks/healthz`
- `/alertmanager/-/ready`
- `/alertmanager/api/v2/status`
- `/prometheus/-/ready`
- `/prometheus/api/v1/status/runtimeinfo`

1b) Internal service health (docker network):
- blackbox exporter `http://trade-blackbox-exporter:9115/-/healthy`

2) Monitoring contract (public-proxy routing must serve links we send in alerts):
- runbooks markdown:
  - `/runbooks/web_uptime.md`
  - `/runbooks/promote_freeze.md`
  - `/runbooks/chatops_security.md`
- grafana dashboards routes (final status should be 200; redirects to login are OK):
  - `/grafana/d/edge_stack_overview/...`
  - `/grafana/d/chatops_security/...`
  - `/grafana/d/monitoring_smoke/...`

2) Telegram webhook link building (dry_run per-request):
- ensures returned links start with RUNBOOKS_BASE_URL and GRAFANA_BASE_URL.

## Contract targets source of truth (recommended)
Targets can be auto-generated from alert annotations (`runbook_path`, `dashboard_path`) and stored in Redis:
- key: `cfg:monitoring_smoke:targets` (override: `MONITORING_SMOKE_TARGETS_KEY`)
  - `runbook_paths` (CSV)
  - `dashboard_paths` (CSV)

Refresh tool:
```bash
python -m ml_analysis.tools.build_smoke_contract_targets_from_alerts_v1
```

## Targets freshness
If Redis targets are enabled, nightly smoke checks updated_ts_ms in `cfg:monitoring_smoke:targets`.
If older than `MONITORING_SMOKE_TARGETS_MAX_AGE_S` (default 7d) it marks targets as stale.

Override:
```bash
export MONITORING_SMOKE_TARGETS_MAX_AGE_S=604800
```

## Run manually
```bash
export PUBLIC_BASE_URL="https://localhost"
export SMOKE_VERIFY_TLS=0
./scripts/smoke_public_proxy.sh
```

Python runner (writes Redis hash `metrics:monitoring_smoke:last`):
```bash
export PYTHONPATH=./tick_flow_full:./ml_analysis
export REDIS_URL="redis://redis-worker-1:6379/0"
python -m ml_analysis.tools.nightly_monitoring_smoke_tests_v1
```

Disable redis targets:
```bash
export SMOKE_TARGETS_FROM_REDIS=0
```

## Override targets
```bash
export SMOKE_RUNBOOK_PATHS="/runbooks/web_uptime.md,/runbooks/promote_freeze.md"
export SMOKE_DASHBOARD_PATHS="/grafana/d/chatops_security/chatops-security?orgId=1,/grafana/d/monitoring_smoke/monitoring-smoke-nightly-contract?orgId=1"
```

## Exporter
`monitoring-smoke-exporter` exposes:
- `monitoring_smoke_last_success`
- `monitoring_smoke_last_updated_ts_ms`
- `monitoring_smoke_age_seconds`
- `monitoring_smoke_runbooks_ok`
- `monitoring_smoke_dashboards_ok`
- `monitoring_smoke_failed_checks_total`
- `monitoring_smoke_alertmanager_api_ok`
- `monitoring_smoke_prometheus_api_ok`
- `monitoring_smoke_blackbox_exporter_ok`
- `monitoring_smoke_targets_stale`
- `monitoring_smoke_targets_age_seconds`
