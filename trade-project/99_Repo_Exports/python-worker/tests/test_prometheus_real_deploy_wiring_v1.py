"""
Tests for Prometheus real deploy wiring (patch_prometheus_real_deploy_wiring_v1).

Validates:
1. monitoring/prometheus/prometheus.yml is valid YAML with required fields
2. monitoring/alertmanager/alertmanager.yml is valid YAML with required fields
3. docker-compose-crypto-orderflow.yml contains prometheus + alertmanager services
   with correct image versions, ports, volumes, and network affiliation
4. Named volumes (trade_prometheus_data, trade_alertmanager_data) are declared
5. CI lint script contains the promtool check config step
"""
import os

import pytest
import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

PROMETHEUS_YML = os.path.join(ROOT, "monitoring", "prometheus", "prometheus.yml")
ALERTMANAGER_YML = os.path.join(ROOT, "monitoring", "alertmanager", "alertmanager.yml")
COMPOSE_FILE = os.path.join(ROOT, "docker-compose-crypto-orderflow.yml")
CI_LINT_SCRIPT = os.path.join(ROOT, "scripts", "ci_prometheus_lint.sh")


# ── Load helpers ────────────────────────────────────────────────────────────────

def load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def read_text(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ── Prometheus config tests ──────────────────────────────────────────────────────

class TestPrometheusConfig:
    """Validate monitoring/prometheus/prometheus.yml."""

    def test_file_exists(self):
        assert os.path.isfile(PROMETHEUS_YML), f"Missing: {PROMETHEUS_YML}"

    def test_valid_yaml(self):
        """File must be parseable YAML."""
        cfg = load_yaml(PROMETHEUS_YML)
        assert isinstance(cfg, dict), "prometheus.yml root must be a mapping"

    def test_global_section(self):
        cfg = load_yaml(PROMETHEUS_YML)
        assert "global" in cfg, "prometheus.yml must have 'global' section"
        assert "scrape_interval" in cfg["global"]
        assert "evaluation_interval" in cfg["global"]

    def test_scrape_configs_present(self):
        cfg = load_yaml(PROMETHEUS_YML)
        assert "scrape_configs" in cfg, "prometheus.yml must have 'scrape_configs'"
        jobs = [j["job_name"] for j in cfg["scrape_configs"]]
        assert "prometheus" in jobs, "'prometheus' self-scrape job must be present"

    def test_edge_stack_p59_scrape_job(self):
        """P59 train exporter must be scraped on port 9813."""
        cfg = load_yaml(PROMETHEUS_YML)
        jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}
        assert "edge_stack_train_p59" in jobs, "edge_stack_train_p59 scrape job required"
        targets = jobs["edge_stack_train_p59"]["static_configs"][0]["targets"]
        assert any("9813" in t for t in targets), "P59 must target port 9813"

    def test_edge_stack_p60_scrape_job(self):
        """P60 shadow exporter must be scraped on port 8012."""
        cfg = load_yaml(PROMETHEUS_YML)
        jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}
        assert "edge_stack_shadow_p60" in jobs, "edge_stack_shadow_p60 scrape job required"
        targets = jobs["edge_stack_shadow_p60"]["static_configs"][0]["targets"]
        assert any("8012" in t for t in targets), "P60 must target port 8012"

    def test_alerting_wired_to_alertmanager(self):
        """Prometheus must point to trade-alertmanager:9093."""
        cfg = load_yaml(PROMETHEUS_YML)
        assert "alerting" in cfg, "prometheus.yml must have 'alerting' section"
        targets = (
            cfg["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"]
        )
        assert any("9093" in t for t in targets), (
            "Prometheus must target alertmanager on port 9093"
        )

    def test_rule_files_present(self):
        """At least some rule_files must be listed."""
        cfg = load_yaml(PROMETHEUS_YML)
        assert "rule_files" in cfg, "prometheus.yml must declare rule_files"
        assert len(cfg["rule_files"]) > 0, "rule_files must be non-empty"


# ── Alertmanager config tests ────────────────────────────────────────────────────

class TestAlertmanagerConfig:
    """Validate monitoring/alertmanager/alertmanager.yml."""

    def test_file_exists(self):
        assert os.path.isfile(ALERTMANAGER_YML), f"Missing: {ALERTMANAGER_YML}"

    def test_valid_yaml(self):
        cfg = load_yaml(ALERTMANAGER_YML)
        assert isinstance(cfg, dict)

    def test_global_section(self):
        cfg = load_yaml(ALERTMANAGER_YML)
        assert "global" in cfg

    def test_route_section(self):
        cfg = load_yaml(ALERTMANAGER_YML)
        assert "route" in cfg
        assert "receiver" in cfg["route"]

    def test_receivers_section(self):
        cfg = load_yaml(ALERTMANAGER_YML)
        assert "receivers" in cfg
        assert len(cfg["receivers"]) > 0


# ── Docker Compose service tests ──────────────────────────────────────────────────

class TestComposeWiring:
    """Validate prometheus + alertmanager appear in docker-compose-crypto-orderflow.yml."""

    @pytest.fixture(scope="class")
    def compose_text(self) -> str:
        return read_text(COMPOSE_FILE)

    @pytest.fixture(scope="class")
    def compose(self) -> dict:
        return load_yaml(COMPOSE_FILE)

    def test_prometheus_service_exists(self, compose: dict):
        assert "prometheus" in compose.get("services", {}), (
            "docker-compose-crypto-orderflow.yml must declare 'prometheus' service"
        )

    def test_alertmanager_service_exists(self, compose: dict):
        assert "alertmanager" in compose.get("services", {}), (
            "docker-compose-crypto-orderflow.yml must declare 'alertmanager' service"
        )

    def test_prometheus_image_version(self, compose: dict):
        """Image must be pinned to a specific version (not :latest)."""
        image = compose["services"]["prometheus"]["image"]
        assert image == "prom/prometheus:v2.54.1", (
            f"Prometheus image must be prom/prometheus:v2.54.1, got: {image}"
        )

    def test_alertmanager_image_version(self, compose: dict):
        image = compose["services"]["alertmanager"]["image"]
        assert image == "prom/alertmanager:v0.27.0", (
            f"Alertmanager image must be prom/alertmanager:v0.27.0, got: {image}"
        )

    def test_prometheus_on_trade_network(self, compose: dict):
        networks = compose["services"]["prometheus"].get("networks", [])
        assert "trade-network" in networks, (
            "prometheus service must be on trade-network"
        )

    def test_alertmanager_on_trade_network(self, compose: dict):
        networks = compose["services"]["alertmanager"].get("networks", [])
        assert "trade-network" in networks, (
            "alertmanager service must be on trade-network"
        )

    def test_prometheus_container_name(self, compose: dict):
        name = compose["services"]["prometheus"].get("container_name")
        assert name == "trade-prometheus", (
            f"container_name must be 'trade-prometheus', got: {name}"
        )

    def test_alertmanager_container_name(self, compose: dict):
        name = compose["services"]["alertmanager"].get("container_name")
        assert name == "trade-alertmanager", (
            f"container_name must be 'trade-alertmanager', got: {name}"
        )

    def test_prometheus_mounts_config(self, compose_text: str):
        """prometheus.yml config must be mounted read-only."""
        assert "monitoring/prometheus/prometheus.yml" in compose_text, (
            "Prometheus config mount missing from compose"
        )

    def test_prometheus_mounts_orderflow_rules(self, compose_text: str):
        """orderflow_services/ must be mounted as alert rules directory."""
        assert "orderflow_services" in compose_text, (
            "orderflow_services alert rules mount missing from compose"
        )

    def test_alertmanager_mounts_config(self, compose_text: str):
        assert "monitoring/alertmanager/alertmanager.yml" in compose_text, (
            "Alertmanager config mount missing from compose"
        )

    def test_prometheus_data_volume_declared(self, compose: dict):
        volumes = compose.get("volumes", {})
        assert "trade_prometheus_data" in volumes, (
            "trade_prometheus_data named volume must be declared in compose"
        )

    def test_alertmanager_data_volume_declared(self, compose: dict):
        volumes = compose.get("volumes", {})
        assert "trade_alertmanager_data" in volumes, (
            "trade_alertmanager_data named volume must be declared in compose"
        )

    def test_prometheus_web_enable_lifecycle(self, compose_text: str):
        """Prometheus must be started with --web.enable-lifecycle for hot reload."""
        assert "--web.enable-lifecycle" in compose_text

    def test_prometheus_port_configurable(self, compose_text: str):
        """Port should use PROMETHEUS_PORT env var with default 9090."""
        assert "PROMETHEUS_PORT" in compose_text
        assert "9090" in compose_text

    def test_alertmanager_port_configurable(self, compose_text: str):
        assert "ALERTMANAGER_PORT" in compose_text
        assert "9093" in compose_text

    def test_prometheus_restart_policy(self, compose: dict):
        restart = compose["services"]["prometheus"].get("restart")
        assert restart == "unless-stopped", (
            f"prometheus restart must be 'unless-stopped', got: {restart}"
        )

    def test_alertmanager_restart_policy(self, compose: dict):
        restart = compose["services"]["alertmanager"].get("restart")
        assert restart == "unless-stopped"


# ── CI lint script tests ──────────────────────────────────────────────────────────

class TestCILintScript:
    """Validate ci_prometheus_lint.sh contains promtool check config step."""

    @pytest.fixture(scope="class")
    def script(self) -> str:
        return read_text(CI_LINT_SCRIPT)

    def test_script_exists(self):
        assert os.path.isfile(CI_LINT_SCRIPT)

    def test_promtool_check_config_step(self, script: str):
        """Script must run 'promtool check config' for the trade-prometheus config."""
        assert "promtool check config" in script, (
            "ci_prometheus_lint.sh must include 'promtool check config'"
        )

    def test_references_monitoring_prometheus_yml(self, script: str):
        assert "monitoring/prometheus/prometheus.yml" in script, (
            "CI lint must reference monitoring/prometheus/prometheus.yml"
        )

    def test_uses_correct_prometheus_image(self, script: str):
        assert "prom/prometheus:v2.54.1" in script, (
            "CI lint must use pinned prom/prometheus:v2.54.1"
        )

    def test_prometheus_compose_config_check(self, script: str):
        """docker compose config -q must be run for the main compose file."""
        assert "docker-compose-crypto-orderflow.yml" in script

    def test_summary_section(self, script: str):
        """Script must report PASS/FAIL summary and exit non-zero on failure."""
        assert "PASS=" in script
        assert "FAIL=" in script
        assert "exit 1" in script
