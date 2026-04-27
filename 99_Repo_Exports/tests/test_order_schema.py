import sys
from pathlib import Path

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from infra.order_schema import (
    normalize_side,
    extract_tp_levels,
    extract_profile,
    extract_tp_fills,
)


def test_normalize_side():
    assert normalize_side("long") == "LONG"
    assert normalize_side("SHORT") == "SHORT"
    assert normalize_side("buy") == "LONG"
    assert normalize_side("sell") == "SHORT"
    assert normalize_side(None) == "LONG"


def test_extract_tp_levels_prefers_json_then_fallback():
    h = {"tp_levels": "[100, 90, 80]"}
    assert extract_tp_levels(h) == [100.0, 90.0, 80.0]
    h2 = {"tp1": "100", "tp2": "90", "tp3": "0"}
    assert extract_tp_levels(h2) == [100.0, 90.0]


def test_extract_profile_reads_both_keys():
    assert extract_profile({"trail_profile": "p1"}) == "p1"
    assert extract_profile({"trailing_profile": "p2"}) == "p2"
    assert extract_profile({}) == ""


def test_extract_tp_fills_reconstructs_dicts():
    h = {"tp1_fill_price": "101.5", "tp1_fill_ts": "170", "tp3_fill_price": "99.0"}
    prices, times = extract_tp_fills(h)
    assert prices[1] == 101.5
    assert prices[3] == 99.0
    assert times[1] == 170
