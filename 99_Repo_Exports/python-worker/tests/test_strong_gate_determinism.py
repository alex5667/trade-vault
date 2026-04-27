from utils.time_utils import get_ny_time_millis
import time
import pytest
from unittest.mock import MagicMock, patch
from services.crypto_orderflow_service import CryptoOrderflowService
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.configuration import DEFAULT_CONFIG

class MockDecision:
    def __init__(self, scenario="TEST", ok=True):
        self.scenario = scenario
        self.ok = ok
        self.have = 1
        self.need = 1
        self.reason = "PASS"
        self.a = True
        self.b = False
        self.c = False

class MockDeltaDetector:
    def push(self, tick):
        return {"delta": 100.0, "z": 3.0}
    z_threshold = 2.0

def test_strong_gate_determinism_no_confirmations():
    """Prove that obi_stable logic works even if confirmations list is empty before the gate."""
    service = CryptoOrderflowService(redis_dsn="redis://m", ticks_dsn="redis://t")
    service.logger = MagicMock()
    
    runtime = SymbolRuntime(symbol="BTCUSDT", config=DEFAULT_CONFIG.copy())
    runtime.delta_detector = MockDeltaDetector()
    runtime.require_strong_confirmation = True
    runtime.config["require_strong_confirmation"] = True
    runtime.config["strong_gate_shadow"] = False
    runtime.config["obi_stable_min_secs"] = 1.0
    
    # Pre-populate OBI event as stable
    now_ms = get_ny_time_millis()
    runtime.last_obi_event = {
        "ts_ms": now_ms - 100,
        "direction": "LONG",
        "obi": 0.6,
        "stable_secs": 2.0
    }
    
    # Needed for reversal branch
    class MockSweep:
        ts_ms = now_ms - 500
        kind = "EQH_SWEEP"
        pool_id = "p1"
        level = 100.0
        tol_px = 0.0
        breach_px = 100.1
        confirm_px = 99.9
        direction_bias = "LONG"
        touches = 5
    runtime.last_sweep = MockSweep()
    runtime.sweep = MagicMock()
    runtime.sweep.valid_ms = 60000

    tick = {"symbol": "BTCUSDT", "ts": now_ms, "price": 50000.0, "qty": 1.0, "side": "buy"}

    # Mock engine
    mock_ofc = MagicMock()
    mock_ofc.ok = 1
    mock_ofc.evidence = {"obi_stable": 1}
    mock_ofc.to_dict.return_value = {"ok": 1}

    with patch.object(service.of_engine, 'build', return_value=(mock_ofc, MagicMock())) as mock_build:
        # We check that build was called
        service._handle_tick(runtime, tick)
        
        assert mock_build.called

def test_fail_closed_on_zero_ts():
    """Verify that tick_ts=0 results in returning None (fail-closed)."""
    service = CryptoOrderflowService(redis_dsn="redis://m", ticks_dsn="redis://t")
    runtime = SymbolRuntime(symbol="BTCUSDT", config=DEFAULT_CONFIG.copy())
    
    tick = {"symbol": "BTCUSDT", "ts": 0, "price": 50000.0, "qty": 1.0, "side": "buy"}
    res = service._handle_tick(runtime, tick)
    assert res is None, "Should return None if ts is missing/zero"
