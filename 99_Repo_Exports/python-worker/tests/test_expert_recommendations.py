import pytest
from unittest.mock import MagicMock
from services.entry_policy_ab_gate import regime_group
from services.ab_winner_eval_store import ABWinnerEvalStore

def test_regime_group_logic():
    """Verify expert recommendation: group thin/news/illiquid as 'thin'."""
    # Thin/Risk regimes
    assert regime_group("thin") == "thin"
    assert regime_group("news") == "thin"
    assert regime_group("illiquid") == "thin"
    
    # Default regimes
    assert regime_group("default") == "default"
    assert regime_group("trending") == "default"
    assert regime_group("choppy") == "default"
    assert regime_group("") == "default"
    assert regime_group(None) == "default"

def test_store_extract_ignores_na_scenario():
    """Verify expert recommendation: ignore 'na' scenarios to keep signal clean."""
    store = ABWinnerEvalStore(r=MagicMock(), stream="test_stream")
    
    # 1. Valid scenario -> Extract
    raw_valid = {
        "symbol": "BTCUSDT", "regime": "thin", "ab_group": "default", 
        "scenario": "continuation", "ab_arm": "A", "r_mult": "1.5", "ts": "1000",
        "type": "POSITION_CLOSED"
    }
    res = store._extract(raw_valid)
    assert res is not None
    assert res[0] == "BTCUSDT"
    assert res[3] == "continuation"

    # 2. 'na' scenario -> None (Skip)
    raw_na = {
        "symbol": "BTCUSDT", "regime": "thin", "ab_group": "default", 
        "scenario": "na", "ab_arm": "A", "r_mult": "1.5", "ts": "1000",
        "type": "POSITION_CLOSED"
    }
    res = store._extract(raw_na)
    assert res is None

    # 3. Missing scenario -> defaults to 'na' -> None
    raw_missing = {
        "symbol": "BTCUSDT", "regime": "thin", "ab_group": "default", 
        # no scenario
        "ab_arm": "A", "r_mult": "1.5", "ts": "1000",
        "type": "POSITION_CLOSED"
    }
    res = store._extract(raw_missing)
    assert res is None

    # 4. 'reversal' scenario -> Extract
    raw_rev = {
        "symbol": "BTCUSDT", "regime": "thin", "ab_group": "default", 
        "scenario": "reversal", "ab_arm": "B", "r_mult": "1.5", "ts": "1000",
        "type": "POSITION_CLOSED"
    }
    res = store._extract(raw_rev)
    assert res is not None
    assert res[3] == "reversal"
