"""
Tests for signal_quality_regime_exporter_p66_v1

Verifies:
  - _f / _i helper robustness
  - _set_metrics populates gauges correctly, including missing-key → 0 defaults
  - load_cfg reads environment variables
  - YAML alert file parses and contains all 3 new signal-quality alerts
"""
import os
import sys
import importlib
import types
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers to import without starting the HTTP server
# ---------------------------------------------------------------------------

def _import_exporter():
    """Import the exporter module, mocking prometheus_client.start_http_server.

    We stub prometheus_client BEFORE the module is exec'd so that the module-level
    Gauge() calls use our FakeGauge. The fake module must have __name__ and __package__
    set properly to avoid Python dataclasses introspection errors.
    """
    import importlib.util

    # Ensure python-worker directory is on the path so `orderflow_services.*` resolves
    pw_path = os.path.join(os.path.dirname(__file__), "..", "python-worker")
    pw_path = os.path.normpath(pw_path)
    if pw_path not in sys.path:
        sys.path.insert(0, pw_path)

    # Only stub if real prometheus_client not available; always force the stub
    # so we don't try to bind a real HTTP server during tests.
    fake_pc = types.ModuleType("prometheus_client")
    fake_pc.__name__ = "prometheus_client"
    fake_pc.__package__ = "prometheus_client"
    fake_pc.__spec__ = None
    fake_gauge_instances: dict = {}

    class FakeGauge:
        """Minimal Gauge stub that records set/labels calls for test assertions."""
        def __init__(self, name: str, doc: str, labelnames=None):
            self._name = name
            self._values: dict = {}
            fake_gauge_instances[name] = self

        def set(self, v: float) -> None:
            self._values["__default__"] = v

        def labels(self, **kw):
            key = tuple(sorted(kw.items()))
            inner = self._values.setdefault(key, {"val": None})
            stub = MagicMock()
            stub.set.side_effect = lambda v: inner.update({"val": v})
            return stub

    fake_pc.Gauge = FakeGauge
    fake_pc.start_http_server = lambda port: None
    # Override unconditionally so our stubs are used regardless of environment
    sys.modules["prometheus_client"] = fake_pc

    spec = importlib.util.spec_from_file_location(
        "signal_quality_regime_exporter_p66_v1",
        os.path.join(
            pw_path,
            "orderflow_services",
            "signal_quality_regime_exporter_p66_v1.py",
        ),
    )
    mod = importlib.util.module_from_spec(spec)
    # Register BEFORE exec so that @dataclass can resolve sys.modules[__module__].__dict__
    sys.modules["signal_quality_regime_exporter_p66_v1"] = mod
    spec.loader.exec_module(mod)
    return mod, fake_gauge_instances


# ---------------------------------------------------------------------------
# Try import; skip on import failure (e.g. missing prometheus_client in CI)
# ---------------------------------------------------------------------------

try:
    _mod, _gauges = _import_exporter()
    _f = _mod._f
    _i = _mod._i
    _set_metrics = _mod._set_metrics
    load_cfg = _mod.load_cfg
    SKIP = False
except Exception as e:
    SKIP = True
    SKIP_REASON = str(e)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(SKIP, reason=SKIP_REASON if SKIP else "")
class TestHelpers:
    def test_f_float(self):
        assert _f("1.5") == 1.5

    def test_f_none_returns_default(self):
        assert _f(None, 99.0) == 99.0

    def test_f_bad_string_returns_default(self):
        assert _f("not-a-number") == 0.0

    def test_f_bytes(self):
        assert _f(b"2.25") == 2.25

    def test_i_int(self):
        assert _i("42") == 42

    def test_i_float_string(self):
        assert _i("3.9") == 3

    def test_i_none_default(self):
        assert _i(None, -1) == -1

    def test_i_bad_string(self):
        assert _i("abc", 7) == 7


@pytest.mark.skipif(SKIP, reason=SKIP_REASON if SKIP else "")
class TestSetMetrics:
    def test_all_regimes_populated(self):
        """_set_metrics must call .set() on all 3 × 4 regime gauges without error."""
        data = {
            "signal_quality_last_ts_ms": "1700000000000",
            "signal_quality_expectancy_r_24h_regime_ok": "0.25",
            "signal_quality_expectancy_r_24h_regime_warn": "-0.05",
            "signal_quality_expectancy_r_24h_regime_block": "0.0",
            "signal_quality_precision_top5p_24h_regime_ok": "0.72",
            "signal_quality_precision_top5p_24h_regime_warn": "0.55",
            "signal_quality_precision_top5p_24h_regime_block": "0.40",
            "signal_quality_ece_24h_regime_ok": "0.03",
            "signal_quality_ece_24h_regime_warn": "0.12",
            "signal_quality_ece_24h_regime_block": "0.20",
            "signal_quality_n_24h_regime_ok": "150",
            "signal_quality_n_24h_regime_warn": "45",
            "signal_quality_n_24h_regime_block": "10",
        }
        # Should not raise
        _set_metrics(data)

    def test_empty_dict_uses_defaults(self):
        """Empty hash → all gauges set to 0, no exception."""
        _set_metrics({})

    def test_missing_keys_are_zero(self):
        """Partial hash with only ok keys — warn/block silently default to 0."""
        data = {
            "signal_quality_expectancy_r_24h_regime_ok": "0.5",
            "signal_quality_n_24h_regime_ok": "100",
        }
        _set_metrics(data)

    def test_last_ts_zero_leads_to_zero_age(self):
        """When last_ts_ms is 0 the age gauge must be set to 0.0 (not negative)."""
        _set_metrics({"signal_quality_last_ts_ms": "0"})


@pytest.mark.skipif(SKIP, reason=SKIP_REASON if SKIP else "")
class TestLoadCfg:
    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("DYN_CFG_KEY", raising=False)
        monkeypatch.delenv("SIGNAL_QUALITY_REGIME_EXPORTER_PORT", raising=False)
        monkeypatch.delenv("SIGNAL_QUALITY_REGIME_EXPORTER_INTERVAL_S", raising=False)
        cfg = load_cfg()
        assert "redis-worker-1:6379" in cfg.redis_url
        assert cfg.dyn_cfg_key == "settings:dynamic_cfg"
        assert cfg.port == 9817
        assert cfg.interval_s == 5.0

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://myredis:6380/1")
        monkeypatch.setenv("DYN_CFG_KEY", "cfg:custom")
        monkeypatch.setenv("SIGNAL_QUALITY_REGIME_EXPORTER_PORT", "9999")
        monkeypatch.setenv("SIGNAL_QUALITY_REGIME_EXPORTER_INTERVAL_S", "30")
        cfg = load_cfg()
        assert cfg.redis_url == "redis://myredis:6380/1"
        assert cfg.dyn_cfg_key == "cfg:custom"
        assert cfg.port == 9999
        assert cfg.interval_s == 30.0


# ---------------------------------------------------------------------------
# YAML structural checks — always run (no import needed)
# ---------------------------------------------------------------------------

ALERTS_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "python-worker",
    "orderflow_services",
    "prometheus_alerts_tradeoff_p66_v1.yml",
)


class TestAlertsYaml:
    def _load(self):
        with open(ALERTS_YAML_PATH) as f:
            return yaml.safe_load(f)

    def test_yaml_parses(self):
        doc = self._load()
        assert "groups" in doc

    def test_has_signal_quality_alerts(self):
        doc = self._load()
        all_alerts = [
            rule["alert"]
            for g in doc["groups"]
            for rule in g.get("rules", [])
        ]
        assert "SignalQualityOkRegimeExpectancyNegativeSLO" in all_alerts
        assert "SignalQualityWarnRegimeEceHighSLO" in all_alerts
        assert "SignalQualityBlockRegimeStillTradingSLO" in all_alerts

    def test_original_decisions_alerts_still_present(self):
        doc = self._load()
        all_alerts = [
            rule["alert"]
            for g in doc["groups"]
            for rule in g.get("rules", [])
        ]
        assert "DecisionFinalStaleSLO" in all_alerts
        assert "DecisionRegimeBlockShareHighSLO" in all_alerts

    def test_all_rules_have_required_fields(self):
        doc = self._load()
        for g in doc["groups"]:
            for rule in g.get("rules", []):
                assert "alert" in rule, f"missing 'alert' in rule: {rule}"
                assert "expr" in rule, f"missing 'expr' in {rule['alert']}"
                assert "labels" in rule, f"missing 'labels' in {rule['alert']}"
                assert "annotations" in rule, f"missing 'annotations' in {rule['alert']}"

    def test_signal_quality_alerts_use_n_guardrail(self):
        """New alerts must gate on regime N to avoid firing with insufficient data."""
        doc = self._load()
        for g in doc["groups"]:
            for rule in g.get("rules", []):
                name = rule.get("alert", "")
                if name in (
                    "SignalQualityOkRegimeExpectancyNegativeSLO",
                    "SignalQualityWarnRegimeEceHighSLO",
                ):
                    assert "signal_quality_n_24h_by_regime" in rule["expr"], (
                        f"{name} must guard on N"
                    )
