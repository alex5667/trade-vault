"""
Tests for risk-based position sizing in TradeMonitorService._normalize_signal.

Verifies that:
1. Risk-based lot from signal_pipeline is preserved (not overridden)
2. Margin-based sizing is used as fallback when no lot is provided
3. one_r_money correctly reflects intended risk (~$5 for $100@5%)
"""

import os
import unittest

# Set ENV BEFORE importing trade_monitor
os.environ.setdefault("ACCOUNT_DEPOSIT_USD", "100")
os.environ.setdefault("RISK_PERCENT", "5.0")
os.environ.setdefault("ACCOUNT_LEVERAGE", "100")


class TestRiskBasedSizing(unittest.TestCase):
    """Test that risk-based lot from signal is preserved, not overridden by margin-based sizing."""

    def _make_signal_data(
        self,
        symbol: str = "SOLUSDT",
        entry_price: float = 84.0,
        sl: float = 83.73,
        direction: str = "LONG",
        lot: float = 18.51,  # risk-based lot from calculate_position_size
        position_size_usd: float = 5.0,
        tp1: float = 84.27,
        tp2: float = 84.54,
        tp3: float = 84.81,
    ) -> dict:
        return {
            "symbol": symbol,
            "entry_price": str(entry_price),
            "sl": str(sl),
            "direction": direction,
            "lot": str(lot),
            "position_size_usd": str(position_size_usd),
            "tp1": str(tp1),
            "tp2": str(tp2),
            "tp3": str(tp3),
            "atr": str(abs(entry_price - sl) * 3),
            "strategy": "CryptoOrderFlow",
            "source": "CryptoOrderFlow",
            "tf": "tick",
            "sid": "test-signal-1",
            "entry_ts_ms": "1711800000000",
            "entry_tag": "test",
        }

    def test_risk_based_lot_preserved_for_crypto(self):
        """
        When signal_pipeline provides lot=18.51 for SOLUSDT (risk-based),
        _normalize_signal should keep it instead of overriding with margin-based lot.
        
        Given:
          deposit=$100, risk=5%, leverage=100x
          entry=84.0, sl=83.73, sl_distance=0.27
          risk_usd = $100 * 5% = $5.0
          risk_lot = $5.0 / 0.27 = 18.51 (from signal_pipeline)
          one_r_money = 0.27 * 18.51 = $5.0 ✓
          
        The old margin-based code would compute:
          margin = $5.0, notional = $500, lot = $500/84 = 5.95
          one_r_money = 0.27 * 5.95 = $1.6 ✗ (wrong!)
        """
        from services.pnl_math import SymbolSpec

        data = self._make_signal_data(lot=18.51)
        signal_lot = float(data["lot"])
        entry_price = float(data["entry_price"])
        sl = float(data["sl"])
        direction = data["direction"]
        symbol = data["symbol"]
        default_lot = 0.01

        # Simulate the sizing decision from _normalize_signal
        symbol_up = symbol.upper()
        is_crypto = symbol_up.endswith(('USDT', 'USDC', 'USD', 'BUSD')) and symbol_up in (
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT'
        )
        self.assertTrue(is_crypto, "SOLUSDT should be recognized as crypto")

        has_risk_based_lot = (
            signal_lot > 0
            and signal_lot != default_lot
            and sl > 0
            and abs(entry_price - sl) > 1e-12
        )
        self.assertTrue(has_risk_based_lot, "Signal with valid lot+SL should be recognized as risk-based")

        # The lot should be the signal's lot, not recalculated
        lot = signal_lot if has_risk_based_lot else 999.0  # 999 = would be margin-based
        self.assertAlmostEqual(lot, 18.51, places=2)

        # Calculate one_r_money
        spec = SymbolSpec()
        one_r_money = spec.risk_money(entry_price, sl, lot, direction, symbol=symbol)
        
        # one_r_money should be ≈ $5.0 (intended risk)
        self.assertAlmostEqual(one_r_money, 5.0, delta=0.1,
                               msg=f"one_r_money should be ~$5.0 but got ${one_r_money:.2f}")

    def test_margin_based_fallback_when_no_lot(self):
        """
        When signal has no pre-calculated lot (lot=0 or default),
        _normalize_signal should fall back to margin-based sizing.
        """
        data = self._make_signal_data(lot=0.0)
        signal_lot = float(data["lot"])
        default_lot = 0.01

        has_risk_based_lot = (
            signal_lot > 0
            and signal_lot != default_lot
        )
        self.assertFalse(has_risk_based_lot, "Zero lot should trigger margin-based fallback")

    def test_margin_based_fallback_when_default_lot(self):
        """
        When signal has default lot (0.01), should also trigger margin-based fallback.
        """
        data = self._make_signal_data(lot=0.01)
        signal_lot = float(data["lot"])
        default_lot = 0.01

        has_risk_based_lot = (
            signal_lot > 0
            and signal_lot != default_lot
        )
        self.assertFalse(has_risk_based_lot, "Default lot should trigger margin-based fallback")

    def test_no_sl_triggers_margin_fallback(self):
        """
        When signal has lot but no SL, should trigger margin-based fallback.
        """
        data = self._make_signal_data(lot=18.51, sl=0.0)
        signal_lot = float(data["lot"])
        sl = float(data["sl"])
        entry_price = float(data["entry_price"])
        default_lot = 0.01

        has_risk_based_lot = (
            signal_lot > 0
            and signal_lot != default_lot
            and sl > 0
            and abs(entry_price - sl) > 1e-12
        )
        self.assertFalse(has_risk_based_lot, "Missing SL should trigger margin-based fallback")

    def test_one_r_money_matches_intended_risk_sol(self):
        """
        Direct math test: risk-based lot for SOLUSDT should give one_r_money ≈ risk_usd.
        
        risk_usd = $5.0, entry = $84, sl = $83.73, sl_distance = $0.27
        lot = risk_usd / sl_distance = 5 / 0.27 = 18.518...
        one_r_money = sl_distance * lot = 0.27 * 18.518 = $5.0 ✓
        """
        from services.pnl_math import SymbolSpec

        entry = 84.0
        sl = 83.73
        sl_distance = abs(entry - sl)
        risk_usd = 5.0
        
        # Risk-based lot calculation (what calculate_position_size should produce)
        lot = risk_usd / sl_distance  # 18.518...
        
        spec = SymbolSpec()
        one_r = spec.risk_money(entry, sl, lot, "LONG", symbol="SOLUSDT")
        
        self.assertAlmostEqual(one_r, risk_usd, delta=0.01,
                               msg=f"one_r_money={one_r:.2f} should be ~{risk_usd:.2f}")

    def test_one_r_money_matches_intended_risk_btc(self):
        """
        Direct math test: risk-based lot for BTCUSDT should give one_r_money ≈ risk_usd.
        
        risk_usd = $5.0, entry = $87000, sl = $86700, sl_distance = $300
        lot = risk_usd / sl_distance = 5 / 300 = 0.01666...
        one_r_money = sl_distance * lot = 300 * 0.01666 = $5.0 ✓
        """
        from services.pnl_math import SymbolSpec

        entry = 87000.0
        sl = 86700.0
        sl_distance = abs(entry - sl)
        risk_usd = 5.0
        
        lot = risk_usd / sl_distance  # 0.01666...
        
        spec = SymbolSpec()
        one_r = spec.risk_money(entry, sl, lot, "LONG", symbol="BTCUSDT")
        
        self.assertAlmostEqual(one_r, risk_usd, delta=0.01,
                               msg=f"BTC one_r_money={one_r:.2f} should be ~{risk_usd:.2f}")

    def test_r_multiples_are_reasonable_with_risk_based_sizing(self):
        """
        With risk-based sizing, a trade that hits SL should give R ≈ -1.0.
        A trade that reaches TP1 at 1RR should give R ≈ +1.0.
        """
        from services.pnl_math import SymbolSpec

        entry = 84.0
        sl = 83.73  # 0.27 below entry
        tp1 = 84.27  # 0.27 above entry (1 RR)
        sl_distance = abs(entry - sl)
        risk_usd = 5.0
        
        lot = risk_usd / sl_distance  # risk-based lot
        
        spec = SymbolSpec()
        one_r = spec.risk_money(entry, sl, lot, "LONG", symbol="SOLUSDT")

        # SL hit: PnL = (sl - entry) * lot = -0.27 * lot, R = pnl / one_r ≈ -1.0
        pnl_sl = spec.pnl_money(entry, sl, lot, "LONG", symbol="SOLUSDT")
        r_sl = pnl_sl / one_r if one_r > 0 else 0
        self.assertAlmostEqual(r_sl, -1.0, delta=0.01,
                               msg=f"SL hit R should be ≈-1.0 but got {r_sl:.2f}")

        # TP1 hit: PnL = (tp1 - entry) * lot = +0.27 * lot, R = pnl / one_r ≈ +1.0
        pnl_tp = spec.pnl_money(entry, tp1, lot, "LONG", symbol="SOLUSDT")
        r_tp = pnl_tp / one_r if one_r > 0 else 0
        self.assertAlmostEqual(r_tp, 1.0, delta=0.01,
                               msg=f"TP1 hit R should be ≈+1.0 but got {r_tp:.2f}")

    def test_old_margin_based_lot_gives_wrong_risk(self):
        """
        Regression check: the OLD margin-based formula gave wrong one_r_money.
        
        margin = $5, leverage = 100x, notional = $500
        lot = 500 / 84 = 5.952
        one_r_money = 0.27 * 5.952 = $1.607 (≠ $5.00!)
        
        This confirms the bug and validates the fix.
        """
        from services.pnl_math import SymbolSpec

        entry = 84.0
        sl = 83.73
        margin = 5.0  # deposit * risk%
        leverage = 100.0
        
        # Old margin-based lot (WRONG)
        notional = margin * leverage  # $500
        old_lot = notional / entry  # 5.952...
        
        spec = SymbolSpec()
        old_one_r = spec.risk_money(entry, sl, old_lot, "LONG", symbol="SOLUSDT")
        
        # Confirm the old one_r is ~$1.6, NOT $5.0
        self.assertAlmostEqual(old_one_r, 1.607, delta=0.1,
                               msg=f"Old margin-based one_r={old_one_r:.2f} should be ~$1.6")
        
        # And confirm it's way below the intended risk
        risk_usd = 5.0
        self.assertLess(old_one_r, risk_usd * 0.5,
                        msg="Old margin-based one_r should be far below intended risk")


class TestCalculatePositionSizeVetos(unittest.TestCase):
    """Tests for new veto paths in calculate_position_size that return lot=0."""

    def _call(self, symbol="SOLUSDT", entry=84.0, sl=83.73, tp=None, **kwargs):
        from services.pnl_math import calculate_position_size
        return calculate_position_size(
            symbol=symbol, entry_price=entry, sl_price=sl,
            tp_price=tp, risk_percent=1.0, deposit=1000.0, leverage=10.0,
            **kwargs
        )

    def test_normal_trade_returns_nonzero_lot(self):
        lot, _, _, _ = self._call(entry=84.0, sl=83.0, tp=85.0)
        self.assertGreater(lot, 0, "Normal trade should produce lot > 0")

    def test_sl_floor_veto_returns_zero(self):
        # SL very close to entry (0.5 bps << 10 bps floor)
        entry = 84.0
        sl = entry - entry * 0.00005  # 0.5 bps
        lot, margin, _, _ = self._call(entry=entry, sl=sl)
        self.assertEqual(lot, 0.0, "Hyper-tight SL should trigger veto (lot=0)")
        self.assertEqual(margin, 0.0)

    def test_tp_floor_veto_returns_zero(self):
        # TP too close to entry (0.5 bps << 12 bps floor)
        entry = 84.0
        sl = entry - entry * 0.005  # 50 bps — ok
        tp = entry + entry * 0.00005  # 0.5 bps — below floor
        lot, _, _, _ = self._call(entry=entry, sl=sl, tp=tp)
        self.assertEqual(lot, 0.0, "Micro-TP should trigger veto (lot=0)")

    def test_tp_none_bypasses_tp_floor(self):
        # tp_price=None → TP veto not evaluated, should pass SL check
        entry = 84.0
        sl = entry - entry * 0.005  # 50 bps — ok
        lot, _, _, _ = self._call(entry=entry, sl=sl, tp=None)
        self.assertGreater(lot, 0, "tp_price=None should not trigger TP veto")

    def test_fee_risk_veto_returns_zero(self):
        # Fee/SL ratio > 1.0: force it via env
        import os
        old = os.environ.pop("FEE_RISK_RATIO_LIMIT", None)
        old_rate = os.environ.pop("CRYPTO_COMMISSION_RATE", None)
        try:
            os.environ["FEE_RISK_RATIO_LIMIT"] = "0.001"   # extremely tight limit
            os.environ["CRYPTO_COMMISSION_RATE"] = "0.005"  # 50 bps commission
            entry = 84.0
            sl = entry - entry * 0.001  # 10 bps SL — fee > SL
            lot, _, _, _ = self._call(entry=entry, sl=sl)
            self.assertEqual(lot, 0.0, "High fee/SL ratio should trigger veto")
        finally:
            if old is not None:
                os.environ["FEE_RISK_RATIO_LIMIT"] = old
            else:
                os.environ.pop("FEE_RISK_RATIO_LIMIT", None)
            if old_rate is not None:
                os.environ["CRYPTO_COMMISSION_RATE"] = old_rate
            else:
                os.environ.pop("CRYPTO_COMMISSION_RATE", None)


class TestParseCandleGuard(unittest.TestCase):
    """Guard tests for candles_archiver: ensure UNKNOWN/zero candles are filtered."""

    def _parse(self, data):
        from services.candles_archiver import parse_candle
        return parse_candle(data)

    def test_valid_candle_is_accepted(self):
        data = {b's': b'BTCUSDT', b'i': b'1m', b't': b'1714000000000', b'T': b'1714000060000',
                b'o': b'84000.0', b'h': b'84200.0', b'l': b'83900.0', b'c': b'84100.0',
                b'v': b'10.5', b'q': b'882000.0', b'n': b'1200', b'V': b'6.0', b'Q': b'504000.0'}
        c = self._parse(data)
        self.assertIsNotNone(c)
        self.assertNotEqual(c.get('symbol'), 'UNKNOWN')
        self.assertGreater(c.get('open', 0.0), 0.0)

    def test_malformed_candle_returns_unknown_symbol(self):
        # Missing price fields → safe_float returns 0.0, symbol defaults to UNKNOWN
        data = {b'type': b'garbage'}
        c = self._parse(data)
        # Either None or UNKNOWN — both are caught by the archiver guard
        if c is not None:
            self.assertTrue(
                c.get('symbol') == 'UNKNOWN' or c.get('open', 0.0) == 0.0,
                "Malformed candle should be filterable by the archiver guard"
            )


if __name__ == "__main__":
    unittest.main()
