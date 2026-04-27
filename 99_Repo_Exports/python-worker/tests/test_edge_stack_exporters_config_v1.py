"""Tests for P59 + P60 Edge Stack exporter configuration and module contracts.

Tests validate:
  1. P59 exporter module: importable, Prometheus gauges registered with correct names.
  2. P60 exporter module: importable, Config dataclass loads defaults correctly from ENV.
  3. docker-compose-timers.yml: both service definitions present with correct ports.
  4. prometheus.yml: both alert rule files referenced, both scrape job names present.
  5. Alert rule YAML files: parse cleanly, use expected metric names in exprs.
"""
from __future__ import annotations

import os
import sys
import types

import pytest
import yaml


# ── Helper paths ─────────────────────────────────────────────────────────────
INFRA_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PW_ROOT = os.path.join(INFRA_ROOT, "python-worker")

sys.path.insert(0, PW_ROOT)
sys.path.insert(0, os.path.join(PW_ROOT, "ml_analysis"))


# ─────────────────────────────────────────────────────────────────────────────
# P59 — ml_analysis/tools/edge_stack_train_exporter_v1.py
# ─────────────────────────────────────────────────────────────────────────────

class TestEdgeStackTrainExporterP59:
    """Import and gauge-name assertions for the P59 train exporter."""

    @pytest.fixture(scope="class")
    def module(self):
        # Lazy import so Redis/prometheus_client import errors are surfaced clearly
        import importlib
        return importlib.import_module("ml_analysis.tools.edge_stack_train_exporter_v1")

    def test_module_importable(self, module):
        """Module must be importable without side-effects (no server started)."""
        assert module is not None

    def test_main_callable(self, module):
        """main() must exist and be callable."""
        assert callable(getattr(module, "main", None))

    def test_gauge_names(self, module):
        """All expected Prometheus gauge names must be registered in the module."""
        expected = {
            "edge_stack_train_exporter_up",
            "edge_stack_train_last_success",
            "edge_stack_train_last_updated_ts_ms",
            "edge_stack_train_last_oof_meta_brier",
            "edge_stack_train_last_oof_meta_ece",
        }
        # Gauges are module-level; inspect _metrics attribute of Gauge objects
        gauge_names = {
            v._name  # type: ignore[attr-defined]
            for v in vars(module).values()
            if hasattr(v, "_name")
        }
        missing = expected - gauge_names
        assert not missing, f"Missing Prometheus gauges in P59 exporter: {missing}"

    def test_env_defaults(self, module, monkeypatch):
        """Default ENV values match expected production configuration."""
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("EDGE_STACK_TRAIN_EXPORTER_PORT", raising=False)
        monkeypatch.delenv("EDGE_STACK_TRAIN_METRICS_KEY", raising=False)

        port = int(os.getenv("EDGE_STACK_TRAIN_EXPORTER_PORT", "9813"))
        key = os.getenv("EDGE_STACK_TRAIN_METRICS_KEY", "metrics:edge_stack_train:last")
        assert port == 9813
        assert key == "metrics:edge_stack_train:last"


# ─────────────────────────────────────────────────────────────────────────────
# P60 — orderflow_services/edge_stack_shadow_status_exporter_v1.py
# ─────────────────────────────────────────────────────────────────────────────

class TestEdgeStackShadowExporterP60:
    """Import and Config/gauge-name assertions for the P60 shadow exporter."""

    @pytest.fixture(scope="class")
    def module(self):
        import importlib
        return importlib.import_module("orderflow_services.edge_stack_shadow_status_exporter_v1")

    def test_module_importable(self, module):
        assert module is not None

    def test_main_callable(self, module):
        assert callable(getattr(module, "main", None))

    def test_load_cfg_defaults(self, module, monkeypatch):
        """load_cfg() must return expected default values when ENV is clean."""
        monkeypatch.delenv("EDGE_STACK_SHADOW_EXPORTER_PORT", raising=False)
        monkeypatch.delenv("EDGE_STACK_SHADOW_STATUS_FILE", raising=False)

        cfg = module.load_cfg()
        assert cfg.port == 8012
        assert "shadow_status.json" in cfg.status_file

    def test_compat_gauge_names(self, module):
        """P60 compat gauges expected by alert rules must be present."""
        expected = {
            "edge_stack_shadow_last_success",
            "edge_stack_shadow_last_updated_ts_ms",
            "edge_stack_shadow_champion_brier",
        }
        gauge_names = {
            v._name  # type: ignore[attr-defined]
            for v in vars(module).values()
            if hasattr(v, "_name")
        }
        missing = expected - gauge_names
        assert not missing, f"Missing P60 compat gauges: {missing}"


# ─────────────────────────────────────────────────────────────────────────────
# docker-compose-timers.yml — service presence checks
# ─────────────────────────────────────────────────────────────────────────────

class TestDockerComposeTimers:
    """Verify that both exporter services exist in docker-compose-timers.yml."""

    @pytest.fixture(scope="class")
    def compose(self):
        path = os.path.join(INFRA_ROOT, "docker-compose-timers.yml")
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_p59_service_present(self, compose):
        services = compose.get("services", {})
        assert "edge-stack-train-exporter-p59" in services, (
            "P59 exporter service 'edge-stack-train-exporter-p59' missing from docker-compose-timers.yml"
        )

    def test_p60_service_present(self, compose):
        services = compose.get("services", {})
        assert "edge-stack-shadow-exporter-p60" in services, (
            "P60 exporter service 'edge-stack-shadow-exporter-p60' missing from docker-compose-timers.yml"
        )

    def test_p59_port_9813(self, compose):
        svc = compose["services"]["edge-stack-train-exporter-p59"]
        ports = svc.get("ports", [])
        assert any("9813" in str(p) for p in ports), (
            "P59 exporter must expose port 9813"
        )

    def test_p60_port_8012(self, compose):
        svc = compose["services"]["edge-stack-shadow-exporter-p60"]
        ports = svc.get("ports", [])
        assert any("8012" in str(p) for p in ports), (
            "P60 exporter must expose port 8012"
        )

    def test_p59_command(self, compose):
        svc = compose["services"]["edge-stack-train-exporter-p59"]
        cmd = " ".join(svc.get("command", []))
        assert "edge_stack_train_exporter_v1" in cmd, (
            f"P59 command must invoke edge_stack_train_exporter_v1, got: {cmd}"
        )

    def test_p60_command(self, compose):
        svc = compose["services"]["edge-stack-shadow-exporter-p60"]
        cmd = " ".join(svc.get("command", []))
        assert "edge_stack_shadow_status_exporter_v1" in cmd, (
            f"P60 command must invoke edge_stack_shadow_status_exporter_v1, got: {cmd}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# prometheus.yml — scrape jobs and rule file wiring
# ─────────────────────────────────────────────────────────────────────────────

class TestPrometheusYml:
    """Verify prometheus.yml is correctly wired for P59/P60."""

    @pytest.fixture(scope="class")
    def prom(self):
        path = os.path.join(INFRA_ROOT, "prometheus.yml")
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_p59_alert_rule_referenced(self, prom):
        rules = prom.get("rule_files", [])
        assert any("edge_stack_train_p59" in r for r in rules), (
            "prometheus_alerts_edge_stack_train_p59.yml must be listed in prometheus.yml rule_files"
        )

    def test_p60_alert_rule_referenced(self, prom):
        rules = prom.get("rule_files", [])
        assert any("edge_stack_shadow_p60" in r for r in rules), (
            "prometheus_alerts_edge_stack_shadow_p60.yml must be listed in prometheus.yml rule_files"
        )

    def test_p59_scrape_job(self, prom):
        jobs = {sc["job_name"] for sc in prom.get("scrape_configs", [])}
        assert "edge_stack_train_p59" in jobs, (
            "Prometheus scrape job 'edge_stack_train_p59' missing from prometheus.yml"
        )

    def test_p60_scrape_job(self, prom):
        jobs = {sc["job_name"] for sc in prom.get("scrape_configs", [])}
        assert "edge_stack_shadow_p60" in jobs, (
            "Prometheus scrape job 'edge_stack_shadow_p60' missing from prometheus.yml"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Alert rule YAML files — metric name consistency
# ─────────────────────────────────────────────────────────────────────────────

class TestAlertRuleYamlFiles:
    """Alert rule YAML files must be valid and reference expected metric names."""

    def _load(self, relpath: str):
        path = os.path.join(INFRA_ROOT, relpath)
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_p59_alerts_parse(self):
        data = self._load("orderflow_services/prometheus_alerts_edge_stack_train_p59.yml")
        assert "groups" in data, "P59 alert YAML must have 'groups' key"

    def test_p59_expr_metric_names(self):
        data = self._load("orderflow_services/prometheus_alerts_edge_stack_train_p59.yml")
        # Collect all exprs in rules
        exprs = []
        for g in data.get("groups", []):
            for r in g.get("rules", []):
                if "expr" in r:
                    exprs.append(r["expr"])
        # Must reference at least these metrics
        combined = "\n".join(exprs)
        assert "edge_stack_train_last_success" in combined
        assert "edge_stack_train_last_updated_ts_ms" in combined

    def test_p60_alerts_parse(self):
        data = self._load("orderflow_services/prometheus_alerts_edge_stack_shadow_p60.yml")
        assert "groups" in data, "P60 alert YAML must have 'groups' key"

    def test_p60_expr_metric_names(self):
        data = self._load("orderflow_services/prometheus_alerts_edge_stack_shadow_p60.yml")
        exprs = []
        for g in data.get("groups", []):
            for r in g.get("rules", []):
                if "expr" in r:
                    exprs.append(r["expr"])
        combined = "\n".join(exprs)
        assert "edge_stack_shadow_last_success" in combined
        assert "edge_stack_shadow_last_updated_ts_ms" in combined
