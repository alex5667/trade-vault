"""P4.8 test: verify that the required SQL migration assets exist in db/migrations/."""
from pathlib import Path


def test_p48_sql_assets_exist():
    """Both SQL migration files for P4.8 must be present."""
    root = Path(__file__).resolve().parents[2]
    assert (root / 'db' / 'migrations' / '20260306_15_risk_mismatch_quarantine.sql').exists(), \
        'Missing migration: 20260306_15_risk_mismatch_quarantine.sql'
    assert (root / 'db' / 'migrations' / '20260306_16_risk_mismatch_quarantine_indexes.sql').exists(), \
        'Missing migration: 20260306_16_risk_mismatch_quarantine_indexes.sql'


def test_risk_drift_sql_sink_importable():
    """RiskDriftSqlSink must be importable from audit_code.risk."""
    from audit_code.risk.risk_drift_sql import RiskDriftSqlSink
    assert hasattr(RiskDriftSqlSink, 'from_env')
    assert hasattr(RiskDriftSqlSink, 'record_quarantine')


def test_risk_drift_sql_sink_disabled_on_empty_dsn():
    """RiskDriftSqlSink.from_env() must be disabled when RISK_AUDIT_SQL_DSN is unset."""
    import os
    old = os.environ.pop('RISK_AUDIT_SQL_DSN', None)
    old2 = os.environ.pop('EXECUTION_JOURNAL_DSN', None)
    try:
        from audit_code.risk.risk_drift_sql import RiskDriftSqlSink
        sink = RiskDriftSqlSink.from_env()
        assert not sink.enabled, 'Sink should be disabled when DSN is empty'
        assert sink.record_quarantine({'sid': 'test'}) is False
    finally:
        if old is not None:
            os.environ['RISK_AUDIT_SQL_DSN'] = old
        if old2 is not None:
            os.environ['EXECUTION_JOURNAL_DSN'] = old2
