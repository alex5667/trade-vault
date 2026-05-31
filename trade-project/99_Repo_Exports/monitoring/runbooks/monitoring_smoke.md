# Runbook: Monitoring Smoke (Nightly Contract)

## Owner
- team: **trade**
- component: **monitoring**

## Dashboard
- `/d/monitoring_smoke/monitoring-smoke-nightly-contract?orgId=1`

## What this is
Nightly smoke is a **contract check** that validates:
1) Public-proxy routes are alive (Grafana/Runbooks/Alertmanager/Prometheus).
2) Alert links contract: `runbook_path` and `dashboard_path` used in alert annotations
   are **actually reachable** via public-proxy.
3) Telegram webhook link-builder works (dry-run).
4) Internal critical dependency health (blackbox-exporter `/-/healthy`).
5) Targets freshness: the generated contract targets list is **not stale**.

## Where state is stored
- Last smoke result hash:
  - `metrics:monitoring_smoke:last`
  - fields include:
    - `success`, `runbooks_ok`, `dashboards_ok`, `failed_total`
    - `alertmanager_api_ok`, `prometheus_api_ok`, `blackbox_exporter_ok`
    - `targets_stale`, `targets_age_s`, `updated_ts_ms`
- Contract targets (generated from alert annotations):
  - `cfg:monitoring_smoke:targets` (override: `MONITORING_SMOKE_TARGETS_KEY`)
  - fields:
    - `runbook_paths` (CSV, public paths like `/runbooks/x.md`)
    - `dashboard_paths` (CSV, public paths like `/grafana/d/<uid>/...`)
    - `updated_ts_ms`

## Alerts
- `MonitoringSmokeRunbooksBroken`
- `MonitoringSmokeDashboardsBroken`
- `MonitoringSmokeAlertmanagerApiBroken`
- `MonitoringSmokePrometheusApiBroken`
- `MonitoringSmokeBlackboxExporterBroken`
- `MonitoringSmokeTargetsStale`

## Quick triage
1) Open dashboard: `/d/monitoring_smoke/monitoring-smoke-nightly-contract?orgId=1`
2) Identify which flag is 0:
   - runbooks_ok / dashboards_ok / am_api_ok / prom_api_ok / blackbox_ok / targets_stale
3) Inspect last result payload in Redis:
```bash
redis-cli HGETALL metrics:monitoring_smoke:last
```
4) If contract targets stale:
```bash
redis-cli HGETALL cfg:monitoring_smoke:targets
```

## Fix by symptom

### Runbooks broken
Typical causes:
- runbooks-web container down
- public-proxy route `/runbooks/*` misconfigured
- missing markdown file under `monitoring/runbooks/`

Actions:
1) Public check:
```bash
curl -I https://<domain>/runbooks/healthz
curl -I https://<domain>/runbooks/web_uptime.md
```
2) Internal check:
```bash
docker logs -n 200 trade-runbooks-web
```

### Dashboards broken (Grafana routes)
Typical causes:
- public-proxy route `/grafana/*` broken
- Grafana down
- wrong subpath config (root_url)

Actions:
```bash
curl -I https://<domain>/grafana/api/health
curl -I https://<domain>/grafana/d/monitoring_smoke/monitoring-smoke-nightly-contract?orgId=1
```

### Alertmanager / Prometheus API broken
Actions:
```bash
curl -I https://<domain>/alertmanager/api/v2/status
curl -I https://<domain>/prometheus/api/v1/status/runtimeinfo
```
If `/-/ready` is OK but API is not, check public-proxy path rewriting.

### Blackbox exporter unhealthy (internal)
Actions:
```bash
docker logs -n 200 trade-blackbox-exporter
docker exec -it trade-prometheus wget -qO- http://trade-blackbox-exporter:9115/-/healthy
```

### Targets stale
This means `cfg:monitoring_smoke:targets.updated_ts_ms` is missing or older than
`MONITORING_SMOKE_TARGETS_MAX_AGE_S` (default 7 days).

Actions:
1) Refresh targets from alert annotations:
```bash
export PYTHONPATH=./tick_flow_full:./ml_analysis
export REDIS_URL="redis://redis-worker-1:6379/0"
python -m ml_analysis.tools.build_smoke_contract_targets_from_alerts_v1
redis-cli HGETALL cfg:monitoring_smoke:targets
```
2) Ensure nightly refresh is enabled:
- `ENABLE_MONITORING_SMOKE_TARGETS_REFRESH=1`
- check `services/of_timers_worker.py` logs.
