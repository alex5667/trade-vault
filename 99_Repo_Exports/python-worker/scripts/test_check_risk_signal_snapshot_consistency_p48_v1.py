"""P4.8 test: verify check_risk_signal_snapshot_consistency.py contains P4.8 additions."""
from pathlib import Path


def test_risk_consistency_script_contains_drift_ledger():
    """The consistency checker must import RiskDriftSqlSink and use REPEATED_MISMATCH_QUARANTINED."""
    src = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'check_risk_signal_snapshot_consistency.py'
    ).read_text(encoding='utf-8')
    assert 'RiskDriftSqlSink' in src, 'check_risk_signal_snapshot_consistency.py must import RiskDriftSqlSink'
    assert 'REPEATED_MISMATCH_QUARANTINED' in src, 'quarantine_action REPEATED_MISMATCH_QUARANTINED must appear'


def test_risk_consistency_script_has_textfile_output():
    """The consistency checker must support --textfile-output for Prometheus metrics."""
    src = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'check_risk_signal_snapshot_consistency.py'
    ).read_text(encoding='utf-8')
    assert '--textfile-output' in src or 'textfile_output' in src, \
        '--textfile-output argument must be present'
    assert 'render_textfile' in src, 'render_textfile() helper must be present'


def test_risk_consistency_script_has_generated_at_ms():
    """The output dict must include generated_at_ms for traceability."""
    src = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'check_risk_signal_snapshot_consistency.py'
    ).read_text(encoding='utf-8')
    assert 'generated_at_ms' in src, 'generated_at_ms timestamp must be in output dict'
