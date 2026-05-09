import pandas as pd

from tools.trade_diagnostics.report_top_loss_reasons import (
    classify,
    fees_bps_roundtrip,
)


class TestMetrics:
    def test_fees_bps_roundtrip_normal(self):
        # 2000 notional, 1.0 fees => 1/2000 * 10000 = 5 bps
        row = {"notional_usd": 2000.0, "fees": 1.0}
        assert fees_bps_roundtrip(row) == 5.0

    def test_fees_bps_roundtrip_fallback(self):
        # missing notional => use lot * entry_px
        row = {"notional_usd": 0.0, "fees": 1.0, "lot": 1.0, "entry_px": 2000.0}
        assert fees_bps_roundtrip(row) == 5.0

    def test_fees_bps_roundtrip_zero(self):
        row = {"notional_usd": 0.0, "fees": 1.0, "lot": 0.0}
        assert fees_bps_roundtrip(row) == 0.0

class TestClassification:
    def test_classify_win(self):
        row = {"pnl_net": 10.0}
        assert classify(row) == "WIN_OR_BE"

    def test_classify_cost_dominates(self):
        # fees_bps >= 8.0 (default)
        row = {"pnl_net": -5.0, "fees_bps": 9.0}
        assert classify(row) == "COST_DOMINATES"

    def test_classify_giveback_trail(self):
        # giveback > 0, mfe > 0, giveback >= 0.5 * mfe
        row = {
            "pnl_net": -5.0,
            "cost_bps": 2.0,
            "giveback": 10.0,
            "mfe_pnl": 15.0, # 10 >= 7.5
        }
        assert classify(row) == "GIVEBACK_TRAIL"

    def test_classify_l2_stale(self):
        # l2_age > 250 or stale_ratio > 0.2
        row = {
            "pnl_net": -10.0,
            "health_avg_l2_age_ms": 300.0,
        }
        assert classify(row) == "L2_STALE"

        row2 = {
            "pnl_net": -10.0,
            "health_l2_stale_ratio_now": 0.3,
        }
        assert classify(row2) == "L2_STALE"

    def test_classify_early_stop(self):
        # mfe_pnl < abs(pnl_net) * 0.5
        # loss = -100 (abs 100). mfe < 50.
        row = {
            "pnl_net": -100.0,
            "mfe_pnl": 20.0,
            "fees_bps": 2.0,
        }
        assert classify(row) == "EARLY_STOP"

    def test_classify_other(self):
        # Just a normal loss
        row = {
            "pnl_net": -100.0,
            "mfe_pnl": 200.0, # mfe huge (not early stop)
            "giveback": 0.0,  # not giveback
            "fees_bps": 2.0,  # low fees
            "health_avg_l2_age_ms": 10.0
        }
        assert classify(row) == "OTHER"

class TestAggregation:
    def test_end_to_end_aggregation(self):
        # Create a sample DataFrame corresponding to v1 schema usage
        data = [
            # Trade 1: Cost dominated (-50 loss, high fees)
            {"order_id": "1", "pnl_net": -50.0, "bucket": "COST_DOMINATES", "fees_bps": 20.0, "mfe_pnl": 10.0, "health_avg_l2_age_ms": 10.0},
            # Trade 2: Cost dominated (-50 loss, high fees)
            {"order_id": "2", "pnl_net": -50.0, "bucket": "COST_DOMINATES", "fees_bps": 25.0, "mfe_pnl": 10.0, "health_avg_l2_age_ms": 10.0},
            # Trade 3: L2 Stale (-20 loss, stale book)
            {"order_id": "3", "pnl_net": -20.0, "bucket": "L2_STALE",       "fees_bps": 2.0,  "mfe_pnl": 5.0,  "health_avg_l2_age_ms": 500.0},
            # Trade 4: Win (ignored in top loss buckets)
            {"order_id": "4", "pnl_net": 100.0, "bucket": "WIN_OR_BE",      "fees_bps": 2.0,  "mfe_pnl": 50.0, "health_avg_l2_age_ms": 10.0},
        ]
        df = pd.DataFrame(data)

        # 1) Top buckets by negative pnl contribution
        neg = df[df["pnl_net"] < 0].copy()

        buckets = (neg.groupby("bucket")
                     .agg(trades=("order_id","count"),
                          pnl_sum=("pnl_net","sum"),
                          pnl_avg=("pnl_net","mean"),
                          fees_med=("fees_bps","median"),
                          l2_age_med=("health_avg_l2_age_ms", "median"))
                     .sort_values("pnl_sum", ascending=True))

        # COST_DOMINATES: sum = -100, trades = 2
        # L2_STALE: sum = -20, trades = 1
        # Order: COST_DOMINATES (-100) then L2_STALE (-20)

        assert len(buckets) == 2
        assert buckets.index[0] == "COST_DOMINATES"
        assert buckets.iloc[0]["trades"] == 2
        assert buckets.iloc[0]["pnl_sum"] == -100.0

        assert buckets.index[1] == "L2_STALE"
        assert buckets.iloc[1]["pnl_sum"] == -20.0
