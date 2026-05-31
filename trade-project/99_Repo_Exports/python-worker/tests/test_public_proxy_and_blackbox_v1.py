"""
Tests for public proxy (Caddy) and blackbox exporter integration.

Validates:
1. docker-compose-crypto-orderflow.yml contains public-proxy and blackbox-exporter services
2. monitoring/public_proxy/Caddyfile exists and has required routes
3. monitoring/blackbox/blackbox.yml exists and has required modules
4. monitoring/prometheus/prometheus.yml has blackbox_http and blackbox_public jobs
5. prometheus_alerts_web_uptime_v1.yml contains required alerting rules
"""
import os

import pytest
import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

COMPOSE_FILE = os.path.join(ROOT, "docker-compose-crypto-orderflow.yml")
CADDYFILE = os.path.join(ROOT, "monitoring", "public_proxy", "Caddyfile")
BLACKBOX_YML = os.path.join(ROOT, "monitoring", "blackbox", "blackbox.yml")
PROMETHEUS_YML = os.path.join(ROOT, "monitoring", "prometheus", "prometheus.yml")
ALERTS_YML = os.path.join(ROOT, "prometheus_alerts_web_uptime_v1.yml")
RUNBOOK_MD = os.path.join(ROOT, "monitoring", "runbooks", "web_uptime.md")


def load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def read_text(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class TestComposeWiring:
    @pytest.fixture(scope="class")
    def compose(self) -> dict:
        return load_yaml(COMPOSE_FILE)

    def test_public_proxy_service_exists(self, compose: dict):
        assert "public-proxy" in compose.get("services", {})
        svc = compose["services"]["public-proxy"]
        assert "caddy" in svc.get("image", "")

    def test_blackbox_exporter_service_exists(self, compose: dict):
        assert "blackbox-exporter" in compose.get("services", {})
        svc = compose["services"]["blackbox-exporter"]
        assert "prom/blackbox-exporter" in svc.get("image", "")
        assert "--config.file=/etc/blackbox/blackbox.yml" in svc.get("command", [])

    def test_grafana_subpath_routing(self, compose: dict):
        svc = compose["services"].get("trade-grafana", {})
        env = svc.get("environment", [])
        assert any("GF_SERVER_ROOT_URL" in e for e in env)
        assert any("GF_SERVER_SERVE_FROM_SUB_PATH" in e for e in env)

    def test_prometheus_subpath_routing(self, compose: dict):
        svc = compose["services"].get("trade-prometheus", {})
        env = svc.get("environment", [])
        assert any("PROMETHEUS_EXTRA_ARGS" in e or "PUBLIC_BASE_URL" in e for e in env)

    def test_alertmanager_subpath_routing(self, compose: dict):
        svc = compose["services"].get("trade-alertmanager", {})
        cmd = svc.get("command", [])
        assert any("--web.external-url" in c for c in cmd)
        assert any("--web.route-prefix" in c for c in cmd)


class TestCaddyfile:
    def test_file_exists(self):
        assert os.path.isfile(CADDYFILE)

    def test_routes_exist(self):
        content = read_text(CADDYFILE)
        assert "handle_path /grafana/*" in content
        assert "handle_path /runbooks/*" in content
        assert "handle_path /alertmanager/*" in content
        assert "handle_path /prometheus/*" in content


class TestBlackboxConfig:
    def test_file_exists(self):
        assert os.path.isfile(BLACKBOX_YML)

    def test_modules_exist(self):
        cfg = load_yaml(BLACKBOX_YML)
        assert "modules" in cfg
        assert "http_2xx" in cfg["modules"]
        assert "http_2xx_insecure" in cfg["modules"]


class TestPrometheusScrapeJobs:
    def test_blackbox_jobs_exist(self):
        cfg = load_yaml(PROMETHEUS_YML)
        jobs = {j["job_name"]: j for j in cfg.get("scrape_configs", [])}
        assert "blackbox_http" in jobs
        assert "blackbox_public" in jobs

        # check that blackbox_http targets are correctly mapped
        job_http = jobs["blackbox_http"]
        assert job_http["metrics_path"] == "/probe"
        assert job_http["params"]["module"] == ["http_2xx"]

        job_public = jobs["blackbox_public"]
        assert job_public["metrics_path"] == "/probe"
        assert job_public["params"]["module"] == ["http_2xx_insecure"]


class TestAlertRules:
    def test_file_exists(self):
        assert os.path.isfile(ALERTS_YML)

    def test_rules_exist(self):
        cfg = load_yaml(ALERTS_YML)
        rules = cfg.get("groups", [])[0].get("rules", [])
        alert_names = [r.get("alert") for r in rules]
        assert "BlackboxExporterScrapeDownCritical" in alert_names
        assert "BlackboxProbeMetricsMissingInternal" in alert_names
        assert "BlackboxProbeMetricsMissingPublic" in alert_names
        assert "WebServiceDownCritical" in alert_names
        assert "WebServiceDownWarningPublic" in alert_names
        assert "WebTlsCertExpiringSoon" in alert_names
        assert "WebTlsCertExpiringCritical" in alert_names
        assert "WebProbeLatencyHighPublic" in alert_names


class TestRunbook:
    def test_file_exists(self):
        assert os.path.isfile(RUNBOOK_MD)
