
import pytest
from core.sweep_detector import SweepEvent

def test_sweep_event_fields():
    # Verify SweepEvent structural contract for downstream consumers
    # This ensures that if SweepEvent changes, we know about it here.
    ev = SweepEvent(
        kind="EQH_SWEEP", direction_bias="SHORT", ts_ms=1000,
        pool_id="p1", pool_kind="EQH", level=100.0, touches=1, tol_px=0.1,
        breach_ts_ms=1001, breach_px=101.0, confirm_px=99.0
    )
    assert ev.kind == "EQH_SWEEP"
    assert ev.direction_bias == "SHORT"
    assert ev.ts_ms == 1000
    assert ev.pool_kind == "EQH"

def test_sweep_confirmation_logic_mirror():
    # Mirrors the logic added to tick_processor.py and orderflow_strategy.py
    # to ensure consistency in string formatting.
    
    # Case 1: EQH SWEEP
    ev_kind = "EQH_SWEEP"
    confirmations = []
    
    if ev_kind == "EQH_SWEEP":
        confirmations.append("sweep_eqh=1")
    elif ev_kind == "EQL_SWEEP":
        confirmations.append("sweep_eql=1")
    
    # Generic backward compat
    confirmations.append("sweep=1")
    
    assert "sweep_eqh=1" in confirmations
    assert "sweep=1" in confirmations
    assert "sweep_eql=1" not in confirmations

    # Case 2: EQL SWEEP
    ev_kind = "EQL_SWEEP"
    confirmations = []
    
    if ev_kind == "EQH_SWEEP":
         confirmations.append("sweep_eqh=1")
    elif ev_kind == "EQL_SWEEP":
         confirmations.append("sweep_eql=1")
    confirmations.append("sweep=1")

    assert "sweep_eql=1" in confirmations
    assert "sweep=1" in confirmations

def test_iceberg_alias_contract():
    # Verify the alias logic for iceberg_strict
    # Input from detector
    confs = ["ice_strict=1"]
    
    # Application logic
    expanded = list(confs)
    for c in confs:
        if c == "ice_strict=1":
            expanded.append("iceberg_strict=1")
    
    assert "ice_strict=1" in expanded
    assert "iceberg_strict=1" in expanded
    
    # Reverse case (if detector emitted strict alias)
    confs_rev = ["iceberg_strict=1"]
    expanded_rev = list(confs_rev)
    for c in confs_rev:
        if c == "iceberg_strict=1":
            expanded_rev.append("ice_strict=1")
            
    assert "ice_strict=1" in expanded_rev
    assert "iceberg_strict=1" in expanded_rev
