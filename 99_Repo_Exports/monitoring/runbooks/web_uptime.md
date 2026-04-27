# Runbook: Web uptime (Grafana / Runbooks / Alertmanager / Prometheus)

## Owner
- team: **trade**
- component: **monitoring**

## Dashboard
- `/d/web_uptime/web-uptime-blackbox?orgId=1`

## Symptoms
- Alert: `BlackboxExporterScrapeDownCritical`
- Alert: `BlackboxProbeMetricsMissingInternal` / `BlackboxProbeMetricsMissingPublic`
- Alert: `WebServiceDownCritical` (internal)
- Alert: `WebServiceDownWarningPublic` (public routing/TLS)
- Alert: `WebTlsCertExpiringSoon` / `WebTlsCertExpiringCritical`
- Alert: `WebProbeLatencyHighPublic`

## Immediate checks (internal)
1) Prometheus Targets:
   - `trade-blackbox-exporter` is UP
   - jobs: `blackbox_http` and `blackbox_public`
   - check `min(up{job="blackbox_public"})` and `min(up{job="blackbox_http"})` (should be 1)
2) Identify failing instance from alert label `instance`:
   - `http://trade-grafana:3000/api/health`
   - `http://trade-runbooks-web:8082/healthz`
   - `http://trade-alertmanager:9093/-/ready`
   - `http://trade-prometheus:9090/-/ready`
3) Docker:
   - `docker ps | grep trade-`
   - `docker logs <container> -n 200`

## Blackbox exporter scrape DOWN / metrics missing
1) Docker:
   - `docker ps | grep blackbox`
   - `docker logs trade-blackbox-exporter -n 200`
2) Connectivity from Prometheus container:
   - `docker exec -it trade-prometheus wget -qO- http://trade-blackbox-exporter:9115/-/healthy`
3) Validate blackbox config (CI already checks):
   - `./scripts/ci_prometheus_lint.sh`

## Public routing/TLS checks
1) Confirm public-proxy running:
   - `docker logs trade-public-proxy -n 200`
2) Check base URLs env for Telegram webhook:
   - `RUNBOOKS_BASE_URL`, `GRAFANA_BASE_URL`, `ALERTMANAGER_BASE_URL`
3) Route-prefix/external-url:
   - Prometheus: `PROMETHEUS_ROUTE_PREFIX=/prometheus`, `PROMETHEUS_EXTERNAL_URL=https://<domain>/prometheus`
   - Alertmanager: `ALERTMANAGER_ROUTE_PREFIX=/alertmanager`, `ALERTMANAGER_EXTERNAL_URL=https://<domain>/alertmanager`
   - Grafana: `GF_SERVER_ROOT_URL=https://<domain>/grafana/` and `GF_SERVER_SERVE_FROM_SUB_PATH=true`

## Rollback (fast)
1) Temporarily point Telegram base URLs back to localhost (or internal):
   - set RUNBOOKS_BASE_URL/GRAFANA_BASE_URL/ALERTMANAGER_BASE_URL to reachable values
2) If public-proxy is broken, bypass it:
   - use direct ports 3000/8082/9093/9090

## Public DOWN (routing/TLS)
1) Check public-proxy container:
   - `docker logs -n 200 trade-public-proxy`
2) Check routing prefixes:
   - `/grafana/*`, `/runbooks/*`, `/alertmanager/*`, `/prometheus/*`
3) Check DNS and reachability from outside:
   - `curl -I https://<your-domain>/grafana/api/health`

## TLS certificate expiring
1) Inspect expiry in dashboard panel "TLS expiry min (days)".
2) If using ACME/Let's Encrypt:
   - ensure port 80/443 reachable from internet
   - check ACME storage volume permissions
   - verify domain points to correct IP
3) If manual certs:
   - renew and reload public-proxy

## Public probe latency high
1) Check probe_duration_seconds trend in dashboard.
2) Verify upstream latency:
   - `curl -w "%{time_total}\n" -o /dev/null -s https://<your-domain>/grafana/api/health`
3) If only some routes slow:
   - check upstream service health (Grafana/Prometheus/Alertmanager)
4) If all routes slow:
   - check host CPU/mem/disk and public-proxy saturation
