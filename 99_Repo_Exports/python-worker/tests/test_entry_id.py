"""
Test for entry_id determinism

Expert review:
  - Senior Python: Validates stable hashing for decision chain tracking
  - Financial Analysts: Ensures entry_id uniquely identifies decision context
"""
from services.smt_entry_policy_service import _entry_id


def test_entry_id_stable():
    """entry_id should be deterministic for same inputs"""
    cand = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "bundle": "b1",
        "setup_ts_ms": 123,
        "ab_key": "k",
        "zone_id": "Z"
    }
    snap = {"zone_id": "Z"}
    bundle = {"decision": "reversal"}
    
    a = _entry_id(cand, snap, bundle)
    b = _entry_id(cand, snap, bundle)
    
    assert a == b, "entry_id should be deterministic"
    assert len(a) == 40, "entry_id should be SHA1 hex (40 chars)"


def test_entry_id_changes_with_inputs():
    """entry_id should change when decision context changes"""
    cand1 = {"symbol": "BTCUSDT", "side": "LONG", "bundle": "b1", "setup_ts_ms": 123}
    cand2 = {"symbol": "BTCUSDT", "side": "SHORT", "bundle": "b1", "setup_ts_ms": 123}
    snap = {}
    bundle = {"decision": "reversal"}
    
    id1 = _entry_id(cand1, snap, bundle)
    id2 = _entry_id(cand2, snap, bundle)
    
    assert id1 != id2, "Different side should produce different entry_id"
