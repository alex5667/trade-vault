
import pytest

from utils.time_utils import get_ny_time_millis


@pytest.fixture
def sample_position_closed_event():
    return {
        "event_type": "POSITION_CLOSED",
        "symbol": "BTCUSDT",
        "sid": "test_sid_123",
        "ts": get_ny_time_millis(),
        "pnl": 100.0,
        "risk_usd": 50.0,
        "r_mult": 2.0,
        "regime": "trend",
        "scenario": "continuation",
        "abs_lvl_tier": 1,
        "dn_tier": 2
    }

def test_closed_trade_fields_present(sample_position_closed_event: dict):
    # sample_position_closed_event is a fixture you can build from a real events:trades message
    ev = sample_position_closed_event
    assert ev.get("event_type") == "POSITION_CLOSED"
    # autopilot-required fields
    for k in ("symbol","sid","ts","pnl","risk_usd","r_mult","regime","scenario"):
        assert k in ev
