import pytest
from services.position_leg_policy import PositionLeg

def test_position_leg_serialization() -> None:
    leg = PositionLeg(entry=10000.0, qty=0.5, side="LONG", signal_id="sig_1", ts_ms=12345678, seq=1)
    
    d = leg.to_dict()
    assert d["entry"] == 10000.0
    assert d["qty"] == 0.5
    assert d["side"] == "LONG"
    assert d["signal_id"] == "sig_1"
    assert d["ts_ms"] == 12345678
    assert d["seq"] == 1
    
    leg2 = PositionLeg.from_dict(d)
    assert leg2.entry == 10000.0
    assert leg2.qty == 0.5
    assert leg2.side == "LONG"
    assert leg.signal_id == leg2.signal_id
    assert leg.ts_ms == leg2.ts_ms
    assert leg.seq == leg2.seq

def test_position_leg_serialization_defaults() -> None:
    leg2 = PositionLeg.from_dict({"entry": "100", "qty": "1.5"})
    assert leg2.entry == 100.0
    assert leg2.qty == 1.5
    assert leg2.side == "LONG" # defaults to LONG uppercase
    assert leg2.signal_id == ""
    assert leg2.ts_ms == 0
    assert leg2.seq == 0
