from __future__ import annotations

"""P6/P7 asset validation tests.

Verifies:
  - prometheus_rules_execution_p67_runtime.yml contains all required alert groups/names
  - execution_hardening_p67.env.example contains all required ENV knobs
"""

import os

import pytest
import yaml

# Paths relative to repo root
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

ENV_EXAMPLE_PATH = os.path.join(
    _REPO_ROOT, "deploy", "systemd", "execution_hardening_p67.env.example"
)
PROM_RULES_PATH = os.path.join(
    _REPO_ROOT, "monitoring", "prometheus_rules_execution_p67_runtime.yml"
)

REQUIRED_ENV_KNOBS = [
    "PROTECTION_ARM_TIMEOUT_MS",
    "TP_LIMIT_WATCHDOG_ENABLE",
    "EXEC_RECONCILE_ON_503_UNKNOWN",
    "EXEC_RECONCILE_PREFER_USER_STREAM",
    "RISK_ENGINE_V2_ENABLE",
    "EXEC_FEE_MAKER_BPS",
    "EXEC_FEE_TAKER_BPS",
]

REQUIRED_ALERT_NAMES = [
    "TradeExecutionProtectionArmTimeout",
    "TradeExecutionEmergencyFlatten",
    "TradeExecutionWatchdogFallbackSpike",
    "TradeExecutionMarkContractDivergence",
    "TradeExecutionUserStreamStale",
]


class TestPrometheusRules:
    def _load_rules(self):
        with open(PROM_RULES_PATH) as f:
            return yaml.safe_load(f)

    @pytest.mark.skipif(not os.path.exists(PROM_RULES_PATH), reason="rules file not found")
    def test_yaml_valid(self):
        doc = self._load_rules()
        assert "groups" in doc

    @pytest.mark.skipif(not os.path.exists(PROM_RULES_PATH), reason="rules file not found")
    def test_required_alert_names_present(self):
        doc = self._load_rules()
        all_alerts = []
        for group in doc.get("groups") or []:
            for rule in group.get("rules") or []:
                name = rule.get("alert") or rule.get("record", "")
                if name:
                    all_alerts.append(name)
        for alert_name in REQUIRED_ALERT_NAMES:
            assert alert_name in all_alerts, f"Missing alert: {alert_name}"


class TestEnvExampleFile:
    def _load_keys(self):
        keys = set()
        with open(ENV_EXAMPLE_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                k = line.split("=", 1)[0].strip()
                if k:
                    keys.add(k)
        return keys

    @pytest.mark.skipif(not os.path.exists(ENV_EXAMPLE_PATH), reason="env file not found")
    def test_required_knobs_present(self):
        keys = self._load_keys()
        for knob in REQUIRED_ENV_KNOBS:
            assert knob in keys, f"Missing ENV knob: {knob}"
