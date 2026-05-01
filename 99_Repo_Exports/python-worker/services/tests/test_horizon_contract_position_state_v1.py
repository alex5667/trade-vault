import pytest
from domain.models import PositionState
from services.horizon_contract import (
    stamp_position_from_signal_payload,
    hydrate_position_from_signal_payload,
)

@pytest.fixture
def base_payload():
    return {
        "sid": "test_sid",
        "symbol": "BTCUSDT",
        "meta": {
            "contract_ver": 2,
            "horizon": {
                "hold_target_ms": 60000,
                "risk_horizon_bucket": "short",
                "profile_source": "test_source",
            },
            "atr_profile": {
                "atr_tf_ms": 300000,
                "atr_value": 50.0,
                "atr_source": "test_atr_source",
            }
        }
    }

def create_test_pos(pid="p1", sid="test_sid", symbol="BTCUSDT"):
    return PositionState(
        id=pid, sid=sid, strategy="test_strat", source="test_src",
        symbol=symbol, tf="1m", direction="LONG",
        entry_price=50000.0, entry_ts_ms=1000,
        lot=1.0, qty=1.0, quantity=1.0, remaining_qty=1.0,
        sl=49000.0, tp_levels=[51000.0, 52000.0, 53000.0]
    )

def test_stamp_position_success(base_payload):
    pos = create_test_pos("p1", "test_sid", "BTCUSDT")
    # Attach
    ok = stamp_position_from_signal_payload(pos, base_payload)
    
    assert ok is True
    assert pos.hold_target_ms == 60000
    assert pos.risk_horizon_bucket == "short"
    assert pos.atr_tf_ms == 300000
    assert pos.horizon_profile_source == "test_source"
    assert pos.atr_source == "test_atr_source"
    
    # Check that canonical contract is preserved in signal_payload
    assert pos.signal_payload["meta"]["contract_ver"] == 2
    assert pos.signal_payload["meta"]["horizon"]["hold_target_ms"] == 60000

def test_stamp_position_flat_legacy_back_compat():
    # Legacy flat payload (simulating old signals or persistence)
    flat_payload = {
        "hold_target_ms": 120000,
        "risk_horizon_bucket": "medium",
        "atr_tf_ms": 60000,
        "atr_source": "legacy_flat",
    }
    pos = create_test_pos("p2", "test_sid_2", "ETHUSDT")
    
    ok = stamp_position_from_signal_payload(pos, flat_payload)
    
    assert ok is True
    assert pos.hold_target_ms == 120000
    assert pos.risk_horizon_bucket == "medium"
    assert pos.atr_tf_ms == 60000
    assert pos.atr_source == "legacy_flat"
    # Verify migration to nested contract in signal_payload
    assert pos.signal_payload["meta"]["contract_ver"] > 0
    assert pos.signal_payload["meta"]["horizon"]["hold_target_ms"] == 120000

def test_hydrate_position_from_signal_payload(base_payload):
    # Simulate fresh PositionState recovered from Redis hash (which only has signal_payload)
    pos = create_test_pos("p3", "test_sid", "BTCUSDT")
    pos.signal_payload = base_payload # in real TM, it's already updated from hash
    
    # Hydrate convenience attrs
    ok = hydrate_position_from_signal_payload(pos)
    
    assert ok is True
    assert pos.hold_target_ms == 60000
    assert pos.risk_horizon_bucket == "short"
    assert pos.atr_tf_ms == 300000

def test_fail_open_on_missing_contract():
    pos = create_test_pos("p4", "test_sid", "BTCUSDT")
    # Signal without contract
    empty_payload = {"some": "data"}
    
    ok = stamp_position_from_signal_payload(pos, empty_payload)
    # Should be False or handle gracefully without crashing
    assert ok is False
    assert pos.hold_target_ms == 0 # default
    assert pos.risk_horizon_bucket == "unknown"
