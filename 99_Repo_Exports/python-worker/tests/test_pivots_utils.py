from handlers.data_processor import nearest_pivot, normalize_pivots_input


def test_normalize_pivots_bundle():
    pivots = {
        "ts_ms": "123",
        "date": "2026-01-04",
        "pivots": {"R1": 100, "S1": "90.5", "bad": -1, "nan": "x"},
    }
    d, ts, date = normalize_pivots_input(pivots)
    assert ts == 123
    assert date == "2026-01-04"
    assert d["R1"] == 100.0
    assert d["S1"] == 90.5
    assert "bad" not in d


def test_normalize_pivots_raw_dict():
    d, ts, date = normalize_pivots_input({"P": 10, "X": "11"})
    assert ts == 0
    assert date == ""
    assert d["P"] == 10.0
    assert d["X"] == 11.0


def test_nearest_pivot():
    d = {"R1": 100.0, "S1": 90.5}
    k, p = nearest_pivot(95.0, d)
    assert k == "S1"
    assert p == 90.5


def test_nearest_pivot_empty():
    k, p = nearest_pivot(100.0, {})
    assert k == ""
    assert p == 0.0
