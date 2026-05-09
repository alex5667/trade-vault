"""P1.2.3 Bootstrap Supervisor asset tests.

Verify that the Prometheus alert rules YAML is well-formed, the Docker Compose
file contains the expected service definition and ENV keys, and the env example
contains the required bootstrap guard knobs.
"""
from pathlib import Path

import yaml

# Test file is at: scanner_infra/python-worker/services/tests/test_*.py
# parents[0] = tests/
# parents[1] = services/
# parents[2] = python-worker/
# parents[3] = scanner_infra/   ← repo root
_REPO = Path(__file__).parents[3]


def test_bootstrap_alert_rules_yaml_loads():
    """Prometheus alert rules must be valid YAML with expected group name."""
    path = _REPO / 'orderflow_services' / 'prometheus_alerts_execution_bootstrap_p123.yml'
    doc = yaml.safe_load(path.read_text())
    assert isinstance(doc, dict)
    assert doc['groups'][0]['name'] == 'trade-execution-bootstrap-p123'
    # Both alert names must be present
    rule_names = {r['alert'] for r in doc['groups'][0]['rules']}
    assert 'TradeExecutionBootstrapNotReady' in rule_names
    assert 'TradeExecutionBootstrapUserStreamNotReady' in rule_names


def test_compose_contains_bootstrap_health_service_and_envs():
    """docker-compose must declare the execution-bootstrap-health service with all required ENVs."""
    path = _REPO / 'docker-compose-crypto-orderflow.yml'
    text = path.read_text()
    assert 'execution-bootstrap-health:' in text
    assert 'command: python /app/services/execution_bootstrap_health_server.py' in text
    assert 'EXEC_BOOTSTRAP_REQUIRE_PROJECTION_READY=1' in text
    assert 'EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=1' in text


def test_env_example_contains_bootstrap_guards():
    """env.example must have all seven P1.2.3 bootstrap knobs."""
    path = _REPO / 'deploy' / 'execution_safe_defaults_p104.env.example'
    text = path.read_text()
    assert 'EXEC_BOOTSTRAP_REQUIRE_READY=1' in text
    assert 'EXEC_BOOTSTRAP_TIMEOUT_MS=120000' in text
    assert 'EXEC_BOOTSTRAP_HEALTH_PORT=8787' in text
    assert 'EXEC_BOOTSTRAP_REQUIRE_PROJECTION_READY=1' in text
    assert 'EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=1' in text
    assert 'EXEC_BOOTSTRAP_POLL_MS=500' in text
    assert 'EXEC_BOOTSTRAP_USER_STREAM_GRACE_MS=45000' in text
