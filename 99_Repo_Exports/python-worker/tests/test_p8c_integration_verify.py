import pytest
from types import SimpleNamespace
from core.of_confirm_engine import OFConfirmEngine

def test_p8c_integration_structure():
    engine = OFConfirmEngine()
    
    # Mock runtime with book_state missing to test fallback
    runtime = SimpleNamespace(
        book_state=SimpleNamespace(snap=None, prev_snap=None),
        last_book={"bids": [[100, 1]], "asks": [[101, 1]]},
        prev_book={"bids": [[100, 1]], "asks": [[101, 1]]},
        last_regime="range",
        dynamic_cfg={},
        # other required attrs
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,
        last_wp=None,
        last_fp_edge=None,
        last_bar=None,
        pressure=SimpleNamespace(is_pressure_hi=lambda t, c: False)
    )
    
    cfg = {"liq_pressure_gate_mode": "both"}
    indicators = {}
    
    # Run build
    ofc, dec = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1000,
        price=100.0,
        delta_z=0.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    
    # Check 1: Fallback worked (evidence has qimb keys)
    # If fallback is missing, these keys won't be in evidence because compute_queue_imbalance_topn gets empty input
    # Actually if input is None, it returns {}, so strict check is if logic tried to use last_book
    
    # In current implementation (read from file), checking calls:
    # snap_t0 = getattr(runtime.book_state, "snap", None) if hasattr(runtime, "book_state") else None
    # if snap_t0 is None: snap_t0 = indicators.get("book_snapshot")
    
    # It does NOT check runtime.last_book. So if we pass runtime.last_book but not indicators['book_snapshot'] 
    # and not runtime.book_state.snap, it will fail to compute qimb.
    
    # Check 2: Evidence has liq_* keys
    # Current implementation does not add them to evidence dict, only indicators.
    
    print("Evidence keys:", ofc.evidence.keys())
    
    assert "qimb_wmean" in ofc.evidence, "qimb_wmean not found in evidence (fallback failed)"
    assert "liq_pressure_boost" in ofc.evidence, "liq_pressure_boost not found in evidence"
    assert "liq_pressure_pen" in ofc.evidence, "liq_pressure_pen not found in evidence"
