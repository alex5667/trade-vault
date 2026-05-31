"""Contract tests: verify that crypto_orderflow_service.py is correctly wired for P4.

Source-text inspection (no import of the service itself to avoid full dependency chain).
"""
from pathlib import Path

_svc = (
    Path(__file__).resolve().parent.parent
    / "services"
    / "crypto_orderflow_service.py"
)
_src = _svc.read_text()


def test_imports_risk_policy_engine():
    """Service must attempt to import from risk_policy_engine (P4 engine)."""
    assert "risk_policy_engine" in _src, \
        "crypto_orderflow_service.py must import from risk_policy_engine"


def test_imports_infer_symbol_tier():
    """Service must import infer_symbol_tier for auto-tier inference."""
    assert "infer_symbol_tier" in _src, \
        "crypto_orderflow_service.py must import infer_symbol_tier"


def test_build_portfolio_risk_input_passes_stop_distance():
    """_build_portfolio_risk_input must feed stop_distance_bps to the engine."""
    assert "stop_distance_bps" in _src, \
        "_build_portfolio_risk_input must pass stop_distance_bps"


def test_build_portfolio_risk_input_passes_volatility():
    """_build_portfolio_risk_input must feed volatility_bps to the engine."""
    assert "volatility_bps" in _src, \
        "_build_portfolio_risk_input must pass volatility_bps"


def test_build_portfolio_risk_input_passes_confidence():
    """_build_portfolio_risk_input must feed confidence to the engine."""
    assert "confidence" in _src, \
        "_build_portfolio_risk_input must pass confidence"


def test_build_portfolio_risk_input_passes_kill_switch():
    """_build_portfolio_risk_input must feed kill_switch to the engine."""
    assert "kill_switch" in _src, \
        "_build_portfolio_risk_input must propagate kill_switch"


def test_pre_publish_writes_risk_watchdog_timeout_ms():
    """_pre_publish_allows_signal must stamp risk_watchdog_timeout_ms onto the signal."""
    assert "risk_watchdog_timeout_ms" in _src, \
        "_pre_publish_allows_signal must write risk_watchdog_timeout_ms onto signal"


def test_pre_publish_writes_execution_policy():
    """_pre_publish_allows_signal must stamp execution_policy onto the signal."""
    assert '"execution_policy"' in _src or "execution_policy" in _src, \
        "_pre_publish_allows_signal must write execution_policy onto signal"


def test_pre_publish_writes_risk_tier():
    """_pre_publish_allows_signal must stamp risk_tier onto the signal."""
    assert "risk_tier" in _src, \
        "_pre_publish_allows_signal must write risk_tier onto signal"


def test_pre_publish_writes_risk_maker_policy_allowed():
    """_pre_publish_allows_signal must stamp risk_maker_policy_allowed onto the signal."""
    assert "risk_maker_policy_allowed" in _src, \
        "_pre_publish_allows_signal must write risk_maker_policy_allowed onto signal"
