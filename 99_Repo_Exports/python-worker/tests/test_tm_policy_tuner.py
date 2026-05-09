

from tools.tm_policy_tuner import group_rows_by_context, pick_winners


def test_pick_winner_lcb_basic() -> None:
    # A: mean 0.0, B: mean 0.2 -> B should win if edge >= 0.05
    rows = []
    for i in range(40):
        rows.append({"ts_ms": 1700000000000 + i, "symbol": "BTCUSDT", "regime": "range", "scenario": "continuation", "ab_group": "default", "ab_arm": "A", "r_mult": 0.0})
    for i in range(40):
        rows.append({"ts_ms": 1700000000000 + 1000 + i, "symbol": "BTCUSDT", "regime": "range", "scenario": "continuation", "ab_group": "default", "ab_arm": "B", "r_mult": 0.2})

    # We need to ensure LCBEvaluatorPerRegime is available for this test to work
    # Since we are in the same environment, it should be fine.

    grouped = group_rows_by_context(rows, window_days=9999)
    winners = pick_winners(grouped, min_samples_default=30, min_edge_r=0.05, min_samples_by_regime={})

    assert winners
    w = winners[0]
    assert w["winner_arm"] == "B"
