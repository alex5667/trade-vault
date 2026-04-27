import unittest
import json
from unittest.mock import Mock
from domain.models import SignalNorm, PositionState, TradeClosed, Tick
from domain.handlers import (
    create_position, process_tick, finalize_trade, 
    _update_excursions_and_adverse
)
from services.trade_monitor import parse_open_position_hash

class TestPnLAttributionRefined(unittest.TestCase):
    def setUp(self):
        self.spec_mock = Mock()
        self.spec_mock.contract_size = 1.0
        self.spec_mock.max_time_back_ms = 0
        self.spec_mock.pnl_money = Mock(side_effect=lambda e, p, l, d, symbol=None: (p - e) * l if d == "LONG" else (e - p) * l)
        self.spec_mock.calculate_fees = Mock(return_value=0.0)
        self.spec_mock.risk_money = Mock(return_value=1.0)
        self.spec_mock.report_min_risk_usd = 1.0
        self.spec_mock.report_fees_risk_mult = 3.0

    def test_long_excursions_expert(self):
        # LONG: entry=100 at 0ms
        pos = create_position(
            SignalNorm(sid="s1", strategy="s", source="s", symbol="B", tf="1", 
                       direction="LONG", entry_price=100.0, entry_ts_ms=0, lot=1.0, sl=90, tp_levels=[110]),
            self.spec_mock
        )
        
        # ticks: 101@t10, 99@t20, 102@t30
        for ts, px in [(10, 101.0), (20, 99.0), (30, 102.0)]:
            tick = Tick(symbol="B", ts_ms=ts, mid=px, price=px)
            _update_excursions_and_adverse(pos, tick)
            process_tick(pos, tick, self.spec_mock, [1.0])
        
        # Manually finalize using domain helper
        closed = finalize_trade(pos, self.spec_mock, exit_price=102.0, exit_ts_ms=40, close_reason_raw="TP1", tp_ratios=[1.0])
        
        # (102-100)/100 * 10000 = 200 bps
        # (100-99)/100 * 10000 = 100 bps
        self.assertEqual(closed.mfe_bps, 200.0)
        self.assertEqual(closed.mae_bps, 100.0)
        self.assertEqual(pos.max_favorable_ts_ms, 30)

    def test_short_excursions_expert(self):
        # SHORT: entry=100 at 0ms
        pos = create_position(
            SignalNorm(sid="s2", strategy="s", source="s", symbol="B", tf="1", 
                       direction="SHORT", entry_price=100.0, entry_ts_ms=0, lot=1.0, sl=110, tp_levels=[90]),
            self.spec_mock
        )
        
        # ticks: 99@t10, 101@t20, 98@t30
        for ts, px in [(10, 99.0), (20, 101.0), (30, 98.0)]:
            tick = Tick(symbol="B", ts_ms=ts, mid=px, price=px)
            _update_excursions_and_adverse(pos, tick)
            process_tick(pos, tick, self.spec_mock, [1.0])
        
        entry = pos.entry_price
        mfe_bps = abs(entry - pos.max_favorable_price) / entry * 10000.0
        mae_bps = abs(pos.max_adverse_price - entry) / entry * 10000.0
        
        self.assertEqual(mfe_bps, 200.0) # favorable for short is 98
        self.assertEqual(mae_bps, 100.0) # adverse for short is 101
        self.assertEqual(pos.max_favorable_ts_ms, 30)

    def test_survival_probe_fixation(self):
        pos = create_position(
            SignalNorm(sid="s3", strategy="s", source="s", symbol="B", tf="1", 
                       direction="LONG", entry_price=100.0, entry_ts_ms=1000, lot=1.0, sl=90, tp_levels=[110]),
            self.spec_mock
        )
        
        # tick at 1050ms (age 50ms) -> adverse 100 bps
        _update_excursions_and_adverse(pos, Tick(symbol="B", ts_ms=1050, mid=99.0, price=99.0))
        self.assertEqual(pos.adverse_bps_running.get(100), 100.0)
        self.assertNotIn(100, pos.adverse_bps_t)
        
        # tick at 1110ms (age 110ms) -> adverse 50 bps (mid 99.5)
        _update_excursions_and_adverse(pos, Tick(symbol="B", ts_ms=1110, mid=99.5, price=99.5))
        self.assertEqual(pos.adverse_bps_t.get(100), 100.0)
        self.assertNotIn(100, pos.adverse_bps_running)
        
        # subsequent adverse move to 98.0 (200 bps) at 1150ms should NOT affect 100ms bucket
        _update_excursions_and_adverse(pos, Tick(symbol="B", ts_ms=1150, mid=98.0, price=98.0))
        self.assertEqual(pos.adverse_bps_t.get(100), 100.0)
        self.assertEqual(pos.adverse_bps_running.get(200), 200.0)

    def test_recovery_initialization(self):
        h = {
            "status": "open",
            "id": "rec-1",
            "entry_price": "100.0",
            "entry_time": "1000",
            "direction": "LONG",
            "lot": "1.0",
            "p0_regime": "trending",
            "p0_session": "london"
        }
        pos = parse_open_position_hash(h, to_int_ms=lambda v, d: int(v or d))
        self.assertIsNotNone(pos)
        self.assertEqual(pos.max_favorable_price, 100.0)
        self.assertEqual(pos.max_favorable_ts_ms, 1000)
        self.assertEqual(pos.p0_regime, "trending")
        self.assertEqual(pos.p0_session, "london")

if __name__ == "__main__":
    unittest.main()
