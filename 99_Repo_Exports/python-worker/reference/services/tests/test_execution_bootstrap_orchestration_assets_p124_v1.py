"""P1.2.4 — orchestration asset validation tests.

Verifies that the compose file, alert rules, and env.example contain the
required P1.2.4 orchestration artifacts (healthcheck, supervised executor
new incident/runbook keys).
"""
from pathlib import Path
import yaml


# ---------------------------------------------------------------------------
# Alert rules
# ---------------------------------------------------------------------------

def test_orchestration_alert_rules_yaml_loads():
    """prometheus_alerts_execution_orchestration_p124.yml must be valid YAML."""
    path = (
        Path(__file__).parents[3]
        / 'orderflow_services'
        / 'prometheus_alerts_execution_orchestration_p124.yml'
    )
    doc = yaml.safe_load(path.read_text())
    assert isinstance(doc, dict), "YAML root must be a mapping"
    assert doc['groups'][0]['name'] == 'trade-execution-orchestration-p124'


def test_orchestration_alerts_contain_both_rules():
    """Alerts file must define TradeExecutionBootstrapBlocked and HealthServiceDown."""
    path = (
        Path(__file__).parents[3]
        / 'orderflow_services'
        / 'prometheus_alerts_execution_orchestration_p124.yml'
    )
    text = path.read_text()
    assert 'TradeExecutionBootstrapBlocked' in text
    assert 'TradeExecutionBootstrapHealthServiceDown' in text


# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------

def test_compose_contains_healthcheck_and_supervised_executor_gate():
    """docker-compose-crypto-orderflow.yml must have P1.2.4 healthcheck + supervised executor."""
    path = Path(__file__).parents[3] / 'docker-compose-crypto-orderflow.yml'
    text = path.read_text()
    assert 'execution-bootstrap-health:' in text, "execution-bootstrap-health service must exist"
    assert 'healthcheck:' in text, "healthcheck block must be present"
    assert 'http://127.0.0.1:8787/readyz' in text, "healthcheck must probe /readyz"
    assert 'binance-executor-supervised:' in text, "P1.2.4 supervised executor must exist"
    assert 'condition: service_healthy' in text, "ready-gate depends_on must use service_healthy"
    assert '--wait-until-ready' in text, "supervised executor must call --wait-until-ready"
    assert '--run-executor' in text, "supervised executor must call --run-executor"


def test_compose_bootstrap_health_has_incident_env_vars():
    """execution-bootstrap-health environment must include P1.2.4 Redis keys."""
    path = Path(__file__).parents[3] / 'docker-compose-crypto-orderflow.yml'
    text = path.read_text()
    assert 'EXEC_BOOTSTRAP_STATUS_KEY=orders:execution:bootstrap:status' in text
    assert 'EXEC_BOOTSTRAP_LAST_BLOCK_KEY=orders:execution:bootstrap:last_block' in text
    assert 'EXEC_BOOTSTRAP_BLOCK_TTL_SEC=86400' in text


# ---------------------------------------------------------------------------
# Env example
# ---------------------------------------------------------------------------

def test_env_example_contains_bootstrap_incident_keys():
    """execution_safe_defaults_p104.env.example must document the 3 new P1.2.4 vars."""
    path = (
        Path(__file__).parents[3]
        / 'deploy'
        / 'execution_safe_defaults_p104.env.example'
    )
    text = path.read_text()
    assert 'EXEC_BOOTSTRAP_STATUS_KEY=orders:execution:bootstrap:status' in text
    assert 'EXEC_BOOTSTRAP_LAST_BLOCK_KEY=orders:execution:bootstrap:last_block' in text
    assert 'EXEC_BOOTSTRAP_BLOCK_TTL_SEC=86400' in text
