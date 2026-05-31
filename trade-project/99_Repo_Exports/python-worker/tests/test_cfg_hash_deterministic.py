from signals.level_enricher import _cfg_hash


def test_cfg_hash_is_order_invariant():
    a = {"TP_RR": 2.0, "STOP_MODE": "atr", "X": 1}
    b = {"X": 1, "STOP_MODE": "atr", "TP_RR": 2.0}
    assert _cfg_hash(a) == _cfg_hash(b)
