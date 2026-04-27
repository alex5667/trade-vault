import pytest
from types import SimpleNamespace
from core.meta_features_v2 import build_meta_features_v2, META_FEAT_V2_COLS, META_FEAT_V2_NEW_COLS

def test_build_meta_features_v2_structure():
    evidence = {}
    indicators = {}
    feat, missing = build_meta_features_v2(evidence, indicators)
    
    # Check all columns present
    for col in META_FEAT_V2_COLS:
        assert col in feat
    
    # Check new columns are zero by default (no runtime)
    for col in META_FEAT_V2_NEW_COLS:
        assert feat[col] == 0.0

def test_build_meta_features_v2_qimb():
    evidence = {}
    indicators = {}
    
    # Mock runtime with book state
    # L1: Bid 100 @ 10, Ask 101 @ 30 -> (30-10)/(30+10) = 20/40 = 0.5?
    # qimb = (bid - ask) / (bid + ask)
    # L1: Bid 10 qty, Ask 30 qty -> (10 - 30) / (40) = -0.5
    
    class MockSnap:
        bids = [(100, 10), (99, 20), (98, 30), (97, 40), (96, 50)]
        asks = [(101, 30), (102, 20), (103, 10), (104, 5), (105, 5)]
    
    runtime = SimpleNamespace(
        book_state=SimpleNamespace(
            snap=MockSnap(),
            prev_snap=None
        )
    )
    
    feat, missing = build_meta_features_v2(evidence, indicators, runtime=runtime)
    
    # L1: 10 vs 30 -> (10-30)/40 = -0.5
    assert feat["qimb_l1"] == -0.5
    
    # L2: 20 vs 20 -> 0.0
    assert feat["qimb_l2"] == 0.0
    
    # L3: 30 vs 10 -> (30-10)/40 = 0.5
    assert feat["qimb_l3"] == 0.5
    
    # wmean:
    # l1: -0.5 * 1 = -0.5
    # l2: 0 * 0.5 = 0
    # l3: 0.5 * 0.333 = 0.1666
    # l4: (40-5)/45 = 0.777 * 0.25 = 0.194
    # l5: (50-5)/55 = 0.818 * 0.2 = 0.163
    # sum weights = 1 + 0.5 + 0.333 + 0.25 + 0.2 = 2.283
    # Check roughly
    assert -1.0 <= feat["qimb_wmean"] <= 1.0

def test_build_meta_features_v2_ofi():
    evidence = {}
    indicators = {}
    
    # Prev: Bid 100@10, Ask 101@10
    # Curr: Bid 100@15 (Inc), Ask 101@5 (Dec)
    # OFI L1:
    # Bid: same px, qty inc by 5 -> +5
    # Ask: same px, qty dec by 5 -> (ask qty decreased = improvement? No, ask qty decrease means less liquidity? )
    # Logic: 
    # if ask_px == prev_ask_px: ea = ask_qty - prev_ask_qty
    # ea = 5 - 10 = -5
    # ofi = eb - ea = 5 - (-5) = 10
    
    class MockSnapCurr:
        bids = [(100, 15)]
        asks = [(101, 5)]
    
    class MockSnapPrev:
        bids = [(100, 10)]
        asks = [(101, 10)]
        
    runtime = SimpleNamespace(
        book_state=SimpleNamespace(
            snap=MockSnapCurr(),
            prev_snap=MockSnapPrev()
        )
    )

    feat, missing = build_meta_features_v2(evidence, indicators, runtime=runtime)
    
    # L1 OFI should be 10.0
    # Computed as sum of OFI levels.
    # L2..L5 are 0 assumed? _parse_snap slices up to 5. 
    # If list short, effectively 0?
    # Logic: "cb_px = c_bids[i][0] if i < len else 0.0"
    # If i >= len, px=0, qty=0. pre_px=0, prev_qty=0.
    # ofi_level -> diff 0.
    
    # So ofi_ml should be 10.0
    assert feat["ofi_ml"] == 10.0
    
    # ofi_ml_wsum: 10.0 * 1 = 10.0
    assert feat["ofi_ml_wsum"] == 10.0
    
    # ofi_ml_norm: 10.0 / (15+5) = 10/20 = 0.5
    assert feat["ofi_ml_norm"] == 0.5

def test_missing_snapshots():
    evidence = {}
    indicators = {}
    # Runtime but no snapshots
    runtime = SimpleNamespace(book_state=None, last_book=None, prev_book=None)
    
    feat, missing = build_meta_features_v2(evidence, indicators, runtime=runtime)
    
    assert feat["qimb_l1"] == 0.0
    assert "ofi_ml" in missing
    assert feat["ofi_ml"] == 0.0

