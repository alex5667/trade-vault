import unittest
from unittest.mock import Mock

from domain.handlers import create_position, finalize_trade, process_tick
from domain.models import PositionState, SignalNorm, Tick, TradeClosed


class TestPnLAttribution(unittest.TestCase):
    def setUp(self):
        self.spec_mock = Mock()
        self.spec_mock.contract_size = 1.0
        self.spec_mock.max_time_back_ms = 0
        self.spec_mock.pnl_money = Mock(side_effect=lambda e, p, l, d, symbol=None: (p - e) * l if d == "LONG" else (e - p) * l)
        self.spec_mock.calculate_fees = Mock(return_value=1.5) # Mock fees
        self.spec_mock.risk_money = Mock(return_value=10.0) # Mock risk_money
        self.spec_mock.report_min_risk_usd = 1.0
        self.spec_mock.report_fees_risk_mult = 3.0

    def test_process_tick_updates_extremes_with_ts(self):
        pos = PositionState(
            id="test-pos", sid="sig-1", strategy="strat", source="src",
            symbol="BTCUSDT", tf="1m", direction="LONG",
            entry_price=100.0, entry_ts_ms=1000, lot=1.0,
            qty=1.0, quantity=1.0, remaining_qty=1.0,
            sl=90.0, tp_levels=[110.0],
            # Ensure these are None or 0 to test defensive init in process_tick
            max_price_seen=0.0, min_price_seen=0.0, p0_entry_px=0.0
        )

        # Ticks: 101@1010, 99@1020, 102@1030
        tick1 = Tick(symbol="BTCUSDT", ts_ms=1010, mid=101.0)
        process_tick(pos, tick1, self.spec_mock, [1.0])
        self.assertEqual(pos.max_price_seen, 101.0)
        self.assertEqual(pos.max_price_seen_ts_ms, 1010)
        self.assertEqual(pos.min_price_seen, 101.0) # Defensive init sets both

        tick2 = Tick(symbol="BTCUSDT", ts_ms=1020, mid=99.0)
        process_tick(pos, tick2, self.spec_mock, [1.0])
        self.assertEqual(pos.min_price_seen, 99.0)
        self.assertEqual(pos.min_price_seen_ts_ms, 1020)

        tick3 = Tick(symbol="BTCUSDT", ts_ms=1030, mid=102.0)
        process_tick(pos, tick3, self.spec_mock, [1.0])
        self.assertEqual(pos.max_price_seen, 102.0)
        self.assertEqual(pos.max_price_seen_ts_ms, 1030)

    def test_finalize_trade_computes_mae_mfe_bps_long(self):
        pos = PositionState(
            id="test-pos", sid="sig-1", strategy="strat", source="src",
            symbol="BTCUSDT", tf="1m", direction="LONG",
            entry_price=100.0, entry_ts_ms=1000, lot=1.0,
            qty=1.0, quantity=1.0, remaining_qty=1.0,
            sl=90.0, tp_levels=[110.0]
        )
        pos.max_price_seen = 102.0
        pos.max_favorable_price = 102.0
        pos.max_favorable_ts = 1030
        pos.min_price_seen = 99.0
        pos.max_adverse_price = 99.0
        pos.realized_pnl_gross = 2.0

        closed = finalize_trade(pos, self.spec_mock, exit_price=102.0, exit_ts_ms=2000, close_reason_raw="TP1", tp_ratios=[1.0])

        # MFE bps = (102-100)/100 * 10000 = 200 bps
        # MAE bps = (100-99)/100 * 10000 = 100 bps
        self.assertAlmostEqual(closed.mfe_bps, 200.0)
        self.assertAlmostEqual(closed.mae_bps, 100.0)
        self.assertEqual(closed.time_to_mfe_ms, 1030 - 1000)
        self.assertEqual(closed.hold_ms, 1000)

    def test_tradeclosed_backward_compat(self):
        # Verify it can be instantiated without new fields
        tc = TradeClosed(order_id="123", pnl_net=10.0)
        self.assertEqual(tc.schema_version, 2)
        self.assertEqual(tc.pnl_net, 10.0)
        self.assertEqual(tc.order_id, "123")

    def test_metadata_persistence(self):
        signal = SignalNorm(
            sid="sig-123", strategy="strat", source="src", symbol="ETHUSDT",
            tf="1m", direction="LONG", entry_price=2000.0, entry_ts_ms=10000,
            lot=1.0, qty=1.0, quantity=1.0, sl=1900.0, tp_levels=[2100.0],
            payload={
                "regime": "trending",
                "scenario": "breakout",
                "session": "london",
                "spread_bps": 2.5,
                "features": {"delta_z": 1.5, "unseen_feat": 99}
            }
        )
        pos = create_position(signal, self.spec_mock)

        self.assertEqual(pos.p0_signal_id, "sig-123")
        self.assertEqual(pos.p0_regime, "trending")
        self.assertEqual(pos.p0_scenario, "breakout")
        self.assertEqual(pos.p0_spread_bps_at_entry, 2.5)
        self.assertIn("delta_z", pos.p0_features_snapshot)
        self.assertNotIn("unseen_feat", pos.p0_features_snapshot) # Whitelist check

        closed = finalize_trade(pos, self.spec_mock, exit_price=2050.0, exit_ts_ms=15000, close_reason_raw="MANUAL", tp_ratios=[1.0])
        self.assertEqual(closed.signal_id, "sig-123")
        self.assertEqual(closed.regime, "trending")
        self.assertEqual(closed.features["delta_z"], 1.5)


class TestFinalizeTradeBaselineSelectedFill(unittest.TestCase):
    """Regression: BLOCKER 3 (2026-05-28) — baseline_*_price was 0 in 100% of
    trades_closed rows. stamp_closed_meta only fires for orphan closures, so
    non-orphan TP/SL closes (the majority) bypassed the baseline fill entirely,
    breaking the path-tp / bounded-sl / trailing-autocal A/B control group.
    finalize_trade must populate baseline_*_price for every close.
    """

    def setUp(self):
        self.spec_mock = Mock()
        self.spec_mock.contract_size = 1.0
        self.spec_mock.max_time_back_ms = 0
        self.spec_mock.pnl_money = Mock(side_effect=lambda e, p, l, d, symbol=None: (p - e) * l if d == "LONG" else (e - p) * l)
        self.spec_mock.calculate_fees = Mock(return_value=1.5)
        self.spec_mock.risk_money = Mock(return_value=10.0)
        self.spec_mock.report_min_risk_usd = 1.0
        self.spec_mock.report_fees_risk_mult = 3.0

    def _make_pos(self, signal_payload=None):
        pos = PositionState(
            id="pos-baseline", sid="sig-baseline", strategy="strat", source="src",
            symbol="BTCUSDT", tf="1m", direction="LONG",
            entry_price=70000.0, entry_ts_ms=1000, lot=1.0,
            qty=1.0, quantity=1.0, remaining_qty=1.0,
            sl=69000.0, tp_levels=[71000.0],
        )
        if signal_payload is not None:
            pos.signal_payload = signal_payload
        return pos

    def test_baseline_falls_back_to_selected_when_meta_empty(self):
        """Shadow-only / pre-canary signals carry no meta.live_surface_baseline.
        baseline_* must mirror selected_* so A/B aggregations have a control row.
        """
        pos = self._make_pos(signal_payload={})

        closed = finalize_trade(
            pos, self.spec_mock, exit_price=69000.0, exit_ts_ms=2000,
            close_reason_raw="SL", tp_ratios=[1.0],
        )

        self.assertEqual(closed.selected_sl_price, 69000.0)
        self.assertEqual(closed.selected_tp1_price, 71000.0)
        self.assertEqual(closed.baseline_sl_price, 69000.0)
        self.assertEqual(closed.baseline_tp1_price, 71000.0)

    def test_baseline_prefers_meta_snapshot_when_present(self):
        """When signal_preprocess captured a non-zero baseline before live-surface
        override, baseline_* must keep that pre-override snapshot — NOT the
        post-override selected_* — so A/B can quantify the override's effect.
        """
        pos = self._make_pos(signal_payload={
            "meta": {
                "live_surface_baseline": {"sl_price": 68500.0, "tp1_price": 71500.0},
                "live_surface_applied": {"applied": True, "reason_code": "LIVE_SURFACE_CANARY_APPLY"},
            }
        })

        closed = finalize_trade(
            pos, self.spec_mock, exit_price=71000.0, exit_ts_ms=2000,
            close_reason_raw="TP1", tp_ratios=[1.0],
        )

        # selected_* still reflect what trading used (from pos.sl/tp_levels).
        self.assertEqual(closed.selected_sl_price, 69000.0)
        self.assertEqual(closed.selected_tp1_price, 71000.0)
        # baseline_* must preserve the pre-override snapshot.
        self.assertEqual(closed.baseline_sl_price, 68500.0)
        self.assertEqual(closed.baseline_tp1_price, 71500.0)

    def test_baseline_falls_back_when_meta_snapshot_is_zero(self):
        """signal_preprocess writes baseline=0 when called before sl_price is
        computed (current upstream bug). Fallback must still produce non-zero
        baseline so the A/B view isn't empty.
        """
        pos = self._make_pos(signal_payload={
            "meta": {
                "live_surface_baseline": {"sl_price": 0.0, "tp1_price": 0.0},
                "live_surface_applied": {"applied": False, "reason_code": "ATR_POLICY_MISS"},
            }
        })

        closed = finalize_trade(
            pos, self.spec_mock, exit_price=69000.0, exit_ts_ms=2000,
            close_reason_raw="SL", tp_ratios=[1.0],
        )

        self.assertEqual(closed.baseline_sl_price, 69000.0)
        self.assertEqual(closed.baseline_tp1_price, 71000.0)

    def test_baseline_reads_meta_from_config_snapshot_too(self):
        """Some publishers stash meta under config_snapshot.meta rather than top
        level — same fallback chain as stamp_closed_meta.
        """
        pos = self._make_pos(signal_payload={
            "config_snapshot": {
                "meta": {
                    "live_surface_baseline": {"sl_price": 68000.0, "tp1_price": 72000.0},
                }
            }
        })

        closed = finalize_trade(
            pos, self.spec_mock, exit_price=71000.0, exit_ts_ms=2000,
            close_reason_raw="TP1", tp_ratios=[1.0],
        )

        self.assertEqual(closed.baseline_sl_price, 68000.0)
        self.assertEqual(closed.baseline_tp1_price, 72000.0)


if __name__ == "__main__":
    unittest.main()
