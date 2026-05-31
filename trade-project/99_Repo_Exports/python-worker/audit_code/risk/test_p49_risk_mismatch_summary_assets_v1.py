from pathlib import Path


def test_p49_migrations_and_alerts_exist():
    """P4.9: verify that migration files and alert rules are present."""
    base = Path(__file__).resolve().parents[2]
    assert (base / 'db' / 'migrations' / '20260306_17_risk_mismatch_retention_partitioning.sql').exists()
    assert (base / 'db' / 'migrations' / '20260306_18_risk_mismatch_retention_partitioning_indexes.sql').exists()
    alert = (base / 'monitoring' / 'prometheus_rules_execution_p49_risk_mismatch.yml').read_text(encoding='utf-8')
    assert 'TradeRiskMismatchRateHigh24h' in alert
    assert 'TradeRiskMismatchSummaryStale' in alert
