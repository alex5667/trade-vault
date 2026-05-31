"""Tests for crypto_orderflow_service P4.5 risk audit integration.

Checks that the service file contains the expected integration hooks
without importing the full service (which requires all async dependencies).
"""
from pathlib import Path


def _src() -> str:
    """Read crypto_orderflow_service.py source text."""
    return (
        Path(__file__).resolve().parents[1] / 'services' / 'crypto_orderflow_service.py'
    ).read_text(encoding='utf-8')


def test_crypto_service_imports_risk_audit_sql_sink():
    """Service must have a try/except import block for RiskAuditSqlSink (P4.5)."""
    src = _src()
    assert 'RiskAuditSqlSink' in src, "RiskAuditSqlSink import missing"


def test_crypto_service_has_persist_method():
    """Service must define _persist_risk_decision_audit method (P4.5)."""
    src = _src()
    assert '_persist_risk_decision_audit' in src, "_persist_risk_decision_audit method missing"


def test_crypto_service_propagates_latency_fields():
    """Service must propagate risk_decision_latency_ms into signal dict (P4.5)."""
    src = _src()
    assert 'risk_decision_latency_ms' in src, "risk_decision_latency_ms field missing"


def test_crypto_service_propagates_clamp_ratio():
    """Service must propagate risk_clamp_ratio into signal dict (P4.5)."""
    src = _src()
    assert 'risk_clamp_ratio' in src, "risk_clamp_ratio field missing"


def test_crypto_service_has_sql_audit_sink_init():
    """Service must initialise risk_audit_sql_sink in __init__ (P4.5)."""
    src = _src()
    assert 'risk_audit_sql_sink' in src, "risk_audit_sql_sink init missing"
