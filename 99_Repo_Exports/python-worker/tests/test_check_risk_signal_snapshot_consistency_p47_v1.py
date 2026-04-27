"""P4.7: Verify that check_risk_signal_snapshot_consistency.py supports P4.7 flags."""
from pathlib import Path


def test_risk_consistency_checker_supports_repeated_quarantine_and_out_report():
    """P4.7 flags must be present in the consistency checker source."""
    src = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'check_risk_signal_snapshot_consistency.py'
    ).read_text(encoding='utf-8')
    # --quarantine-on-repeated CLI flag
    assert '--quarantine-on-repeated' in src
    # Default output path includes latest_risk_signal_consistency.json
    assert 'latest_risk_signal_consistency.json' in src
    # Quarantine reason tag for repeated mismatch
    assert 'repeated_risk_consistency_mismatch' in src
    # mismatch_rate must be emitted
    assert 'mismatch_rate' in src
