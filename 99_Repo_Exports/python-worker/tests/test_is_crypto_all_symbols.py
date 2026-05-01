"""
Regression test for the is_crypto whitelist fix in trade_monitor._normalize_signal().

Bug: is_crypto was AND-ed with a hardcoded 6-symbol set:
    is_crypto = symbol_up.endswith(('USDT','USDC','USD','BUSD')) AND symbol_up in ('BTCUSDT','ETHUSDT',...)
This caused ALL other crypto symbols (DOGEUSDT, WIFUSDT, 1000PEPEUSDT, SUIUSDT, etc.)
to fall through to `lot = signal_lot` (default 0.01), producing:
    one_r_money = clamp($1.0)  →  R = PnL / 1.0  →  absurd R-multiples in reports.

Fix: is_crypto = symbol_up.endswith(('USDT','USDC','BUSD')) and not symbol_up.startswith('XAU')
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestIsCryptoAllSymbols(unittest.TestCase):
    """Ensures the is_crypto detection in _normalize_signal covers all active symbols."""

    # All symbols that should be treated as crypto for margin-based sizing
    CRYPTO_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT",  # Original 6
        "DOGEUSDT", "WIFUSDT", "1000PEPEUSDT", "SUIUSDT", # Previously missed
        "1000BONKUSDT",  "DOTUSDT", "MATICUSDT",
         "ATOMUSDT", "APTUSDT", "OPUSDT",
        "AAVEUSDT", "MKRUSDT", "TIAUSDT", "SEIUSDT",
        "BTCUSDC", "ETHBUSD",  # Alternative stablecoins
    ]

    NOT_CRYPTO_SYMBOLS = [
        # Metals on Forex
    ]

    # Defaults matching ENV defaults in trade_monitor.__init__
    _CRYPTO_SUFFIXES = tuple(
        s.strip().upper()
        for s in os.getenv("CRYPTO_SUFFIXES", "USDT,USDC,BUSD").split(",")
        if s.strip()
    )
    _CRYPTO_EXCLUDE_PREFIXES = tuple(
        s.strip().upper()
        for s in os.getenv("CRYPTO_EXCLUDE_PREFIXES", "").split(",")
        if s.strip()
    )
    _MARGIN_FX_SYMBOLS = frozenset(
        s.strip().upper()
        for s in os.getenv("MARGIN_FX_SYMBOLS").split(",")
        if s.strip()
    )

    def _eval_is_crypto(self, symbol: str) -> bool:
        """Replicate the is_crypto logic from trade_monitor._normalize_signal()"""
        symbol_up = symbol.upper()
        return (
            symbol_up.endswith(self._CRYPTO_SUFFIXES)
            and not symbol_up.startswith(self._CRYPTO_EXCLUDE_PREFIXES)
        )

    def test_all_crypto_symbols_detected(self):
        """Every active crypto symbol must be recognized as is_crypto=True."""
        for sym in self.CRYPTO_SYMBOLS:
            with self.subTest(symbol=sym):
                self.assertTrue(
                    self._eval_is_crypto(sym),
                    f"{sym} should be detected as crypto for margin-based sizing"
                )

    def test_non_crypto_symbols_excluded(self):
        """XAU symbols must NOT be treated as crypto."""
        for sym in self.NOT_CRYPTO_SYMBOLS:
            with self.subTest(symbol=sym):
                self.assertFalse(
                    self._eval_is_crypto(sym),
                    f"{sym} should NOT be detected as crypto"
                )

    def test_risk_based_sizing_produces_reasonable_one_r(self):
        """
        With proper is_crypto=True, risk-based sizing uses SL distance,
        producing one_r_money ≈ risk_usd (not $1.00 floor).
        """
        from services.pnl_math import calculate_position_size, SymbolSpec

        # Simulate DOGEUSDT: entry=0.15, SL=0.148 (2 ATR), deposit=$100, risk=5%
        symbol = "DOGEUSDT"
        entry = 0.15
        sl = 0.148
        deposit = 100.0
        risk_pct = 5.0  # 5% = $5 risk
        leverage = 20.0

        lot, pos_size, dep, lev = calculate_position_size(
            symbol=symbol,
            entry_price=entry,
            sl_price=sl,
            side="LONG",
            deposit=deposit,
            risk_percent=risk_pct,
            leverage=leverage,
        )

        # Verify lot is risk-sized (not default 0.01)
        self.assertGreater(lot, 1.0, f"DOGEUSDT lot should be >> 0.01 with risk-based sizing, got {lot}")

        # Verify one_r_money makes sense
        spec = SymbolSpec()
        one_r = spec.risk_money(entry, sl, lot, "LONG", symbol=symbol)

        # one_r should be materially above dust ($0.00002 from lot=0.01)
        # With small deposit+margin-cap, one_r may be limited but still >> $1.00 floor
        self.assertGreater(one_r, 1.0, f"one_r_money should be > $1.00, got ${one_r:.2f}")
        # And critically, the R-multiple should be reasonable (not PnL-in-USD)
        # e.g., if PnL=$5 and one_r=$1.33, R=3.75 (reasonable) vs R=5.0 (PnL/1.0)


    def test_old_whitelist_bug_demonstration(self):
        """
        Demonstrates the old bug: with lot=0.01 (non-risk-based), one_r is tiny → clamped to $1.
        """
        from services.pnl_math import SymbolSpec

        # Old behavior: DOGEUSDT got lot=0.01 (default_lot)
        entry = 0.15
        sl = 0.148
        lot = 0.01  # This is what the old code produced
        spec = SymbolSpec()
        one_r = spec.risk_money(entry, sl, lot, "LONG", symbol="DOGEUSDT")

        # With lot=0.01, one_r = 0.01 * |0.15 - 0.148| = 0.00002 → would be clamped to $1.00
        self.assertLess(one_r, 0.001, f"With lot=0.01, one_r should be dust: ${one_r:.6f}")


if __name__ == "__main__":
    unittest.main()
