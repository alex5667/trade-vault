
import unittest

# Add parent directory to path to import services
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from services.trade_metrics_service import TradeMetricsService


class TestTradeMetricsUnits(unittest.TestCase):
    def setUp(self):
        self.tm = TradeMetricsService()

    def test_mfe_unit_conversion(self):
        """Test that MFE (Price) is converted to USD using Lot."""
        m = self.tm.new_metrics()
        t = {
            "pnl_gross": "10.0",
            "pnl_net": "9.0",
            "fees": "-1.0",
            "mfe": "1000.0",  # Price
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)

        # MFE USD = 1000 * 0.01 = 10.0
        # Exit Eff = 10.0 / 10.0 = 1.0
        self.assertAlmostEqual(m["sum_exit_eff_win"], 1.0)
        self.assertEqual(m["cnt_exit_eff_win"], 1)

    def test_giveback_usd_semantics(self):
        """Giveback field is stored as USD in Redis (closed.giveback = pos.mfe_pnl - pnl_gross).
        Reader must NOT multiply by lot again — that was a double-multiplication bug
        that clamped reported giveback_ratio at 1.5 for any lot != 1.
        """
        m = self.tm.new_metrics()
        # MFE USD = 20, PnL gross = 10, gave back 10 USD. Lot = 0.01 (irrelevant for USD fields).
        t = {
            "pnl_gross": "10.0",
            "pnl_net": "9.0",
            "fees": "-1.0",
            "mfe_pnl": "20.0",   # USD
            "giveback": "10.0",  # USD (NOT price-delta)
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)
        # Ratio = 10/20 = 0.5 (NOT 0.005 from over-multiplication, NOT 1.5 clamp)
        self.assertAlmostEqual(m["sum_giveback_ratio_win"], 0.5)

    def test_missed_profit_usd_semantics(self):
        """Missed_profit is stored as USD; reader must not multiply by lot."""
        m = self.tm.new_metrics()
        t = {
            "pnl_gross": "0.0",
            "pnl_net": "-1.0",
            "fees": "-1.0",
            "mfe_pnl": "20.0",        # USD
            "missed_profit": "20.0",  # USD (NOT price-delta)
            "lot": "0.01",
            "close_reason": "SL_AFTER_TP"
        }
        self.tm.accumulate_trade(m, t)
        # Ratio = 20/20 = 1.0
        self.assertAlmostEqual(m["sum_missed_profit_ratio"], 1.0)

    def test_explicit_usd_priority(self):
        """Test that explicit USD fields take precedence over raw fields."""
        m = self.tm.new_metrics()
        t = {
            "pnl_gross": "10.0",
            "mfe": "5000.0",       # Garbage Price
            "mfe_usd": "20.0",     # Correct USD
            "lot": "0.01",
            "close_reason": "TP"
        }
        self.tm.accumulate_trade(m, t)

        # Eff = 10 / 20 = 0.5
        self.assertAlmostEqual(m["sum_exit_eff_win"], 0.5)

class TestSnzAtrFix(unittest.TestCase):
    """Regression tests for the truthy-zero-string ATR bug.

    Redis stores floats as strings. "0.0" is a non-empty string → truthy in
    Python `or`-chains → stops the chain before reaching a real fallback.
    _snz() must return 0.0 for "0.0" so the chain continues.
    """

    def setUp(self):
        from services.trade_metrics_service import _snz
        self.snz = _snz
        self.tm = TradeMetricsService()

    def test_snz_zero_string_returns_zero(self):
        self.assertEqual(self.snz("0.0"), 0.0)
        self.assertEqual(self.snz("0"), 0.0)

    def test_snz_nonzero_string_returns_float(self):
        self.assertAlmostEqual(self.snz("0.0150"), 0.015)
        self.assertAlmostEqual(self.snz("1.23"), 1.23)

    def test_snz_none_returns_zero(self):
        self.assertEqual(self.snz(None), 0.0)

    def test_snz_real_zero_float_returns_zero(self):
        self.assertEqual(self.snz(0.0), 0.0)

    def test_atr_zero_string_falls_through_to_signal_payload(self):
        """When top-level atr="0.0" (truthy string but zero value),
        sl_atr/tp_atr must be computed from signal_payload ATR, not 0.
        """
        import json
        tm = TradeMetricsService()
        m = tm.new_metrics()
        # atr stored as "0.0" (what Redis returns when pos.atr was 0 at save)
        # signal_payload carries the real ATR from signal_pipeline.py
        signal_payload = json.dumps({
            "atr": 0.015,
            "indicators": {"atr_used_for_levels": 0.015},
        })
        t = {
            "atr": "0.0",           # truthy string, but zero value
            "entry_price": "1.0",
            "sl_price": "0.985",    # 0.015 from entry = 1.0 ATR distance
            "tp1_price": "1.030",   # 0.030 from entry = 2.0 ATR distance
            "signal_payload": signal_payload,
            "close_reason": "TIMEOUT",
        }
        tm.accumulate_trade(m, t)
        tm.finalize(m)
        # Should have computed sl_atr ≈ 1.0 and tp_atr ≈ 2.0
        self.assertGreater(m["avg_sl_atr"], 0, "sl_atr must be non-zero when ATR recoverable from signal_payload")
        self.assertGreater(m["avg_tp_atr"], 0, "tp_atr must be non-zero when ATR recoverable from signal_payload")
        self.assertAlmostEqual(m["avg_sl_atr"], 1.0, places=2)
        self.assertAlmostEqual(m["avg_tp_atr"], 2.0, places=2)

    def test_atr_zero_string_with_no_signal_payload_stays_zero(self):
        """When atr="0.0" and no signal_payload → sl_atr/tp_atr remain 0 (fail-open)."""
        tm = TradeMetricsService()
        m = tm.new_metrics()
        t = {
            "atr": "0.0",
            "entry_price": "1.0",
            "sl_price": "0.985",
            "tp1_price": "1.030",
            "close_reason": "TIMEOUT",
        }
        tm.accumulate_trade(m, t)
        tm.finalize(m)
        self.assertEqual(m["avg_sl_atr"], 0.0)
        self.assertEqual(m["avg_tp_atr"], 0.0)

    def test_generic_atr_in_signal_payload_indicators_is_ignored(self):
        """Regression: indicators.atr (live feature-time ATR, often 1m fallback)
        must NOT be used to normalize SL/TP that were placed on a higher-TF ATR.
        Otherwise we get 20-45 ATR readings (e.g. ETH SL=21.77 / atr=0.67 = 32 ATR).
        """
        import json
        tm = TradeMetricsService()
        m = tm.new_metrics()
        # Reproduces the prod payload: indicators carries a generic `atr` (and even
        # explicitly flags it bad via atr_bad=1), but no labeled variant. Must skip.
        signal_payload = json.dumps({
            "indicators": {
                "atr": 0.674,
                "atr_bad": 1,
                "atr_bad_reason": "jump_rel>1.200:3.801:tf=1m",
            },
        })
        t = {
            "atr": "0.0",
            "entry_price": "2175.94",
            "sl": "2197.71",
            "tp1_price": "2154.17",
            "signal_payload": signal_payload,
            "close_reason": "TIMEOUT",
        }
        tm.accumulate_trade(m, t)
        tm.finalize(m)
        self.assertEqual(m["cnt_sl_atr"], 0,
                         "must NOT contribute: only labeled atr_used_for_levels/atr_at_entry is allowed")
        self.assertEqual(m["cnt_tp_atr"], 0)
        self.assertEqual(m["avg_sl_atr"], 0.0)
        self.assertEqual(m["avg_tp_atr"], 0.0)

    def test_labeled_atr_in_signal_payload_indicators_is_used(self):
        """When indicators carries atr_used_for_levels (labeled), it is the
        canonical level-time ATR and MUST be used."""
        import json
        tm = TradeMetricsService()
        m = tm.new_metrics()
        signal_payload = json.dumps({
            "indicators": {
                "atr_used_for_levels": 0.015,
                "atr": 0.001,  # generic — should be ignored
            },
        })
        t = {
            "atr": "0.0",
            "entry_price": "1.0",
            "sl_price": "0.985",   # 0.015 = 1.0 ATR
            "tp1_price": "1.030",  # 0.030 = 2.0 ATR
            "signal_payload": signal_payload,
            "close_reason": "TIMEOUT",
        }
        tm.accumulate_trade(m, t)
        tm.finalize(m)
        self.assertEqual(m["cnt_sl_atr"], 1)
        self.assertAlmostEqual(m["avg_sl_atr"], 1.0, places=2)
        self.assertAlmostEqual(m["avg_tp_atr"], 2.0, places=2)


if __name__ == "__main__":
    unittest.main()
