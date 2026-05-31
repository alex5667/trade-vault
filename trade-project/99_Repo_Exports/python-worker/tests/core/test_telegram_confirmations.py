from core.telegram_confirmations import build_compact_confirmations


def test_compact_confirmations_order_and_presence():
    indicators = {
        "reclaim": 1,
        "obi_stable_secs": 2.25,
        "obi_stability_score": 0.92,
        "iceberg_strict": 1,
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.4,
        "weak_range_atr": 0.27,
        "weak_recent_cnt": 3,
        "weak_recent_window": 5,
    }
    s = build_compact_confirmations(indicators=indicators, confirmations=[])
    assert "reclaim" in s
    assert "obi=2.2s q=0.92" in s
    assert "ice" in s
    assert "fp=1.40x" in s
    assert "weakP=0.27" in s
    assert "weak5=3/5" in s
