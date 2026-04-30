import pytest
from unittest.mock import AsyncMock, MagicMock
from ..strategy import OrderFlowStrategy
from ..runtime import SymbolRuntime

@pytest.fixture
def service():
    # Bypass __init__ to avoid remote redis connection hangs in test environment
    srv = OrderFlowStrategy.__new__(OrderFlowStrategy)
    srv._emit_payload = AsyncMock(return_value=None)
    srv.redis = AsyncMock()
    srv.ticks = AsyncMock()
    srv.logger = MagicMock()
    srv.adverse_continuation_counters = {}
    return srv
@pytest.fixture
def runtime():
    rt = MagicMock()
    rt.symbol = "BTCUSDT"
    rt.config = {"adverse_check_enable": 1}
    rt.pending_adverse_payload = None
    rt.pending_adverse_ts_ms = 0
    rt.atr_range_agg = MagicMock()
    snap = MagicMock()
    snap.tf_ms = 1000
    snap.n = 10
    snap.p50 = 1.0
    snap.p95 = 2.0
    rt.atr_range_agg.snapshot.return_value = snap
    rt.dynamic_cfg = {}
    return rt

@pytest.mark.asyncio
async def test_reversal_veto_no_evidence(service, runtime):
    """Reversal signal should be vetoed if no evidence is present."""
    payload = {
        "direction": "LONG"
        "indicators": {
            "strong_gate_scn": "reversal"
            "cvd_reclaim_ok": 0
            "absorption_volume": 0
            "obi_stable": 0
            "ofi_stable": 0
        }
    }
    res = service._eval_g10_adverse_gate(runtime, payload, 1000)
    assert res == "veto_reversal", "Should be vetoed"

@pytest.mark.asyncio
async def test_reversal_pass_with_evidence(service, runtime):
    """Reversal signal should pass if evidence is present."""
    payload = {
        "direction": "LONG"
        "indicators": {
            "strong_gate_scn": "reversal"
            "cvd_reclaim_ok": 1, # Evidence!
            "absorption_volume": 0
            "obi_stable": 0
            "ofi_stable": 0
        }
    }
    res = service._eval_g10_adverse_gate(runtime, payload, 1000)
    assert res == "pass", "Should pass"

@pytest.mark.asyncio
async def test_continuation_wait_for_bar(service, runtime):
    """Continuation signal should be buffered for the next bar."""
    payload = {
        "direction": "LONG"
        "indicators": {
            "strong_gate_scn": "continuation"
        }
    }
    res = service._eval_g10_adverse_gate(runtime, payload, 1000)
    assert res == "wait_continuation", "Should return wait_continuation and buffer"
    assert runtime.pending_adverse_payload == payload
    assert runtime.pending_adverse_ts_ms == 1000

@pytest.mark.asyncio
async def test_continuation_verified_by_bar_closed(service, runtime):
    """Continuation signal buffered, then verified by a bar closing in the correct direction."""
    # 1. Provide signal and buffer it manually as strategy would
    payload = {
        "direction": "LONG"
        "indicators": {
            "strong_gate_scn": "continuation"
        }
    }
    runtime.pending_adverse_payload = payload
    runtime.pending_adverse_ts_ms = 1000
    
    # 2. Close bar
    bar = MagicMock()
    bar.open = 100.0
    bar.high = 105.0
    bar.low = 100.0
    bar.close = 105.0 # c > o -> LONG favorable
    bar.vol = 10.0
    bar.cvd_close = 0.0
    bar.end_ts_ms = 1500
    bar.fp_evictions = 0
    
    await service._on_microbar_closed(runtime, bar)
    
    # 3. Verify it was emitted
    service._emit_payload.assert_called_once()
    assert runtime.pending_adverse_payload is None, "Buffer should be cleared"

@pytest.mark.asyncio
async def test_continuation_discarded_by_bar_closed(service, runtime):
    """Continuation signal buffered, then discarded if bar closes in wrong direction."""
    # 1. Provide signal
    payload = {
        "direction": "LONG"
        "indicators": {
            "strong_gate_scn": "continuation"
        }
    }
    runtime.pending_adverse_payload = payload
    runtime.pending_adverse_ts_ms = 1000
    
    # 2. Close bar
    bar = MagicMock()
    bar.open = 105.0
    bar.high = 105.0
    bar.low = 100.0
    bar.close = 100.0 # c < o -> NOT favorable for LONG
    bar.vol = 10.0
    bar.cvd_close = 0.0
    bar.end_ts_ms = 1500
    bar.fp_evictions = 0
    
    await service._on_microbar_closed(runtime, bar)
    
    # 3. Verify it was NOT emitted
    service._emit_payload.assert_not_called()
    assert runtime.pending_adverse_payload is None, "Buffer should be cleared"

