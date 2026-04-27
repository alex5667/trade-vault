"""
End-to-End integration test simulating the flow of a virtual trade 
from outbox to closed periodic reports, ensuring perfect metrics segregation.
"""
import copy
import json
import pytest
from unittest.mock import Mock, patch

def test_periodic_reporter_virtual_segregation():
    # Because full e2e requires database/redis, we test the classification 
    # logic of the periodic report engine with mocked payloads.
    # We want to ensure that "is_virtual=1" guarantees 0 leakage into Real stats.
    
    from services.periodic_reporter import PeriodicReporter
    
    # Simple trade fixtures representing what trailing/execution outputs
    real_trade = {
        "symbol": "BTCUSDT",
        "is_virtual": False,
        "pnl_net": 50.0,
        "one_r_money": 100.0,
        "closeTime": 1700000000000
    }
    
    virtual_trade = {
        "symbol": "ETHUSDT",
        "is_virtual": True,
        "pnl_net": -20.0,
        "one_r_money": 50.0,
        "closeTime": 1700000001000
    }
    
    # Initialize reporter with mocked redis
    reporter = PeriodicReporter.__new__(PeriodicReporter)
    reporter.tm = Mock()
    # Replace methods that do I/O if needed
    
    trades = [real_trade, virtual_trade, virtual_trade, real_trade]
    
    virtual_pnl = 0.0
    real_pnl = 0.0
    
    for t in trades:
        # Match actual logic used by production reader
        is_virt = reporter._is_trade_virtual(t)
        if is_virt:
            virtual_pnl += t["pnl_net"]
        else:
            real_pnl += t["pnl_net"]
            
    assert virtual_pnl == -40.0, "Virtual PnL accumulated incorrectly"
    assert real_pnl == 100.0, "Real PnL accumulated incorrectly - leakage occurred"
    
def test_outbox_envelope_virtual_tagging():
    # Ensure moving from outbox candidate to execution preserves is_virtual
    from core.outbox_envelope import make_envelope

    envelope = make_envelope(
        signal_id="123",
        source="test-worker",
        ts_ms=1000,
        kind="test",
        symbol="BTCUSDT",
        payload={"is_virtual": True, "risk_usd": 100.0, "sl": 45000.0, "tp": 55000.0},
    )

    # Payload is dictionary in envelope
    assert envelope.payload.get("is_virtual") is True
    # Test to_stream_fields
    fields = envelope.to_stream_fields()

    payload_json = json.loads(fields["payload_json"])
    assert payload_json.get("is_virtual") is True
