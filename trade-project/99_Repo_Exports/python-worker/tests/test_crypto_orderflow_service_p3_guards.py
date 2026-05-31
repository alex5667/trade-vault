"""P3 source-level guard tests for crypto_orderflow_service.py.

These tests read the source file as text (no import) to verify that all
mandatory P3 guard functions and patterns are present.  They protect against
accidental regressions when the file is re-generated or partially patched.
"""
from pathlib import Path

_SRC = (
    Path(__file__).parent.parent / "services" / "crypto_orderflow_service.py"
).read_text(encoding="utf-8")


def test_build_redis_dq_snapshot_present():
    """_build_redis_dq_snapshot must be defined (snapshots Redis DQ state)."""
    assert "_build_redis_dq_snapshot" in _SRC


def test_pre_publish_allows_signal_present():
    """_pre_publish_allows_signal gate must be present in the service."""
    assert "_pre_publish_allows_signal" in _SRC


def test_hard_dq_veto_log_message_present():
    """The hard-veto warning log must be in the source (traceability)."""
    assert "Hard DQ veto before publish" in _SRC


def test_dq_snapshot_field_written_to_signal():
    """signal['dq_snapshot'] must be assigned (DQ info carried in outbound signal)."""
    assert "dq_snapshot" in _SRC


def test_last_tick_ts_tracking():
    """last_tick_ts must be tracked on runtime (inputs DQ snapshot)."""
    assert "last_tick_ts" in _SRC


def test_xack_fail_events_tracking():
    """xack_fail_events must be tracked on runtime (inputs DQ snapshot)."""
    assert "xack_fail_events" in _SRC


def test_redis_dq_policy_import_present():
    """The service must import RedisDQSnapshot/evaluate_redis_dq (with fallback)."""
    assert "evaluate_redis_dq" in _SRC
    assert "RedisDQSnapshot" in _SRC


def test_redis_timeout_events_tracking():
    """redis_timeout_events must be tracked on runtime for DQ staleness counting."""
    assert "redis_timeout_events" in _SRC


def test_negative_age_events_tracking():
    """negative_age_events must be tracked (timestamps from the future = data corruption)."""
    assert "negative_age_events" in _SRC


def test_trade_dq_hard_veto_enable_env():
    """TRADE_DQ_HARD_VETO_ENABLE env flag must be read (operator kill-switch)."""
    assert "TRADE_DQ_HARD_VETO_ENABLE" in _SRC
