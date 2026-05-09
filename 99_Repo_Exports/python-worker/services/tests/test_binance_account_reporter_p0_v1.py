"""Unit tests for Binance account reporter P0.

We do NOT hit Binance network in tests.
We validate:
  - snapshot parsing (account + positionRisk)
  - open_notional aggregation
  - open_positions_n counts ALL positions (not only top-N)
  - report formatting is stable and includes key metrics
"""

import importlib.util
import sys
import unittest
from pathlib import Path

mod_path = Path(__file__).with_name("binance_account_reporter.py")
spec = importlib.util.spec_from_file_location("binance_account_reporter", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

build_snapshot = mod.build_snapshot
format_report = mod.format_report
_store_history = mod._store_history
_read_delta_available = mod._read_delta_available
_fmt_delta = mod._fmt_delta,
_MS_1H = mod._MS_1H,
_MS_24H = mod._MS_24H,


class DummyClient:
    """Stub client that returns canned Binance API responses without network."""

    def __init__(self):
        self._acct = {
            "totalWalletBalance": "1000.00",
            "totalMarginBalance": "1100.00",
            "availableBalance": "800.00",
            "totalUnrealizedProfit": "25.50",
            "totalInitialMargin": "250.00",
            "totalMaintMargin": "15.00",
        },
        self._pos = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.010",
                "entryPrice": "50000",
                "markPrice": "51000",
                "unRealizedProfit": "10",
                "notional": "510",
                "liquidationPrice": "42000",
                "initialMargin": "51",
                "maintMargin": "3",
                "isolatedMargin": "0",
                "marginType": "cross",
                "leverage": "10",
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "-0.20",
                "entryPrice": "3000",
                "markPrice": "2900",
                "unRealizedProfit": "-5",
                "notional": "-580",
                "liquidationPrice": "0",
                "initialMargin": "58",
                "maintMargin": "4",
                "isolatedMargin": "0",
                "marginType": "cross",
                "leverage": "10",
            },
            {
                "symbol": "XRPUSDT",
                "positionAmt": "100",
                "entryPrice": "0.50",
                "markPrice": "0.51",
                "unRealizedProfit": "0.2",
                "notional": "51",
                "liquidationPrice": "0",
                "initialMargin": "5",
                "maintMargin": "0.3",
                "isolatedMargin": "0",
                "marginType": "cross",
                "leverage": "10",
            }
        ]

    def get_account(self):
        return self._acct

    def get_position_risk(self):
        return self._pos

    def get_open_orders(self, *, symbol=None):
        return [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]


class TestBinanceAccountReporterP0(unittest.TestCase):
    def test_snapshot_aggregation_and_counts(self):
        """Validate aggregation: all positions counted, top-N list truncated, exposure correct."""
        c = DummyClient()
        snap = build_snapshot(client=c, topn_positions=2, include_open_orders=True)

        # All positions counted (not only top-N).
        self.assertEqual(snap.open_positions_n, 3)
        # Top-N list is truncated to 2.
        self.assertEqual(len(snap.positions), 2)

        # Exposure sums absolute notionals: 510 + 580 + 51 = 1141
        self.assertAlmostEqual(snap.open_notional_usdt, 1141.0, places=6)

        self.assertEqual(snap.open_orders_n, 2)
        self.assertAlmostEqual(snap.wallet_balance, 1000.0)
        self.assertAlmostEqual(snap.margin_balance, 1100.0)
        self.assertAlmostEqual(snap.available_balance, 800.0)
        self.assertAlmostEqual(snap.unrealized_pnl, 25.5)

    def test_report_contains_key_fields(self):
        """Validate Telegram HTML report contains all required fields and symbols."""
        c = DummyClient()
        snap = build_snapshot(client=c, topn_positions=2, include_open_orders=True)
        msg = format_report(snap)

        self.assertIn("Binance USDT-M Account", msg)
        self.assertIn("Wallet", msg)
        self.assertIn("Margin", msg)
        self.assertIn("Open positions", msg)
        # Contains one of the top-2 symbols (BTCUSDT or ETHUSDT — both have notional > XRPUSDT)
        self.assertTrue(("BTCUSDT" in msg) or ("ETHUSDT" in msg))
        # No delta block when deltas=None (default)
        self.assertNotIn("Available Δ", msg)

    def test_report_with_deltas_block(self):
        """Report includes the Available Δ block when deltas dict is provided."""
        c = DummyClient()
        snap = build_snapshot(client=c, topn_positions=2, include_open_orders=True)
        deltas = {"1h": 10.5, "24h": -50.0}
        msg = format_report(snap, deltas=deltas)

        self.assertIn("Available Δ", msg)
        self.assertIn("1h", msg)
        self.assertIn("24h", msg)
        self.assertIn("+10.50", msg)   # positive 1h delta
        self.assertIn("-50.00", msg)   # negative 24h delta
        self.assertIn("📈", msg)       # arrow for positive
        self.assertIn("📉", msg)       # arrow for negative

    def test_report_with_none_deltas(self):
        """When delta values are None (no history yet), show em-dash."""
        c = DummyClient()
        snap = build_snapshot(client=c, topn_positions=2, include_open_orders=True)
        deltas = {"1h": None, "24h": None}
        msg = format_report(snap, deltas=deltas)

        self.assertIn("Available Δ", msg)
        self.assertIn("—", msg)

    def test_fmt_delta(self):
        """_fmt_delta formats positive / negative / zero / None correctly."""
        self.assertTrue(_fmt_delta(5.0).startswith("📈"))
        self.assertTrue(_fmt_delta(-3.0).startswith("📉"))
        self.assertTrue(_fmt_delta(0.0).startswith("➡️"))
        self.assertEqual(_fmt_delta(None), "—")

    def test_snapshot_positions_sorted_by_notional(self):
        """Top positions list is sorted by absolute notional descending."""
        c = DummyClient()
        snap = build_snapshot(client=c, topn_positions=3, include_open_orders=False)
        # ETH notional=580 > BTC notional=510 > XRP notional=51
        self.assertEqual(snap.positions[0]["symbol"], "ETHUSDT")
        self.assertEqual(snap.positions[1]["symbol"], "BTCUSDT")
        self.assertEqual(snap.positions[2]["symbol"], "XRPUSDT")

    def test_side_detection(self):
        """LONG for positive positionAmt, SHORT for negative."""
        c = DummyClient()
        snap = build_snapshot(client=c, topn_positions=10, include_open_orders=False)
        sides = {p["symbol"]: p["side"] for p in snap.positions}
        self.assertEqual(sides["BTCUSDT"], "LONG")
        self.assertEqual(sides["ETHUSDT"], "SHORT")
        self.assertEqual(sides["XRPUSDT"], "LONG")


class TestAvailableHistory(unittest.TestCase):
    """Test _store_history and _read_delta_available with a simple in-memory mock."""

    class MockRedis:
        """Minimal sorted-set stub (single-key)."""

        def __init__(self):
            self._data: dict = {}  # {float(value): score_ms}

        def zadd(self, key, mapping):
            for val, score in mapping.items():
                self._data[float(val)] = score

        def zremrangebyscore(self, key, lo, hi):
            lo_f = float("-inf") if lo == "-inf" else float(lo)
            hi_f = float("+inf") if hi == "+inf" else float(hi)
            self._data = {
                val: score for val, score in self._data.items()
                if not (lo_f <= score <= hi_f)
            }

        def zrangebyscore(self, key, lo, hi, withscores=False):
            lo_f = float("-inf") if lo == "-inf" else float(lo)
            hi_f = float("+inf") if hi == "+inf" else float(hi)
            result = [
                (str(val), score) for val, score in self._data.items()
                if lo_f <= score <= hi_f
            ]
            return result if withscores else [v for v, _ in result]

    def test_store_and_read_1h_delta(self):
        r = self.MockRedis()
        now_ms = 1_700_000_000_000
        old_ts = now_ms - _MS_1H
        _store_history(r, "history", old_ts, 500.0)
        deltas = _read_delta_available(r, "history", now_ms, 520.0)

        self.assertIsNotNone(deltas["1h"])
        self.assertAlmostEqual(deltas["1h"], 20.0, places=6)
        self.assertIsNone(deltas["24h"])

    def test_store_and_read_24h_delta(self):
        r = self.MockRedis()
        now_ms = 1_700_000_000_000
        _store_history(r, "history", now_ms - _MS_24H, 1000.0)
        deltas = _read_delta_available(r, "history", now_ms, 950.0)

        self.assertIsNone(deltas["1h"])
        self.assertIsNotNone(deltas["24h"])
        self.assertAlmostEqual(deltas["24h"], -50.0, places=6)

    def test_no_history_returns_none(self):
        r = self.MockRedis()
        deltas = _read_delta_available(r, "history", 1_700_000_000_000, 100.0)
        self.assertIsNone(deltas["1h"])
        self.assertIsNone(deltas["24h"])

    def test_ttl_cleanup(self):
        """Old entries beyond TTL are removed from the sorted set."""
        r = self.MockRedis()
        now_ms = 1_700_000_000_000
        # 3 days ago – should be pruned with ttl_sec=90000 (25h)
        _store_history(r, "history", now_ms - 3 * _MS_24H, 999.0, ttl_sec=90_000)
        _store_history(r, "history", now_ms, 100.0, ttl_sec=90_000)
        # only the current entry should remain
        self.assertEqual(len(r._data), 1)
        self.assertAlmostEqual(list(r._data.keys())[0], 100.0)


if __name__ == "__main__":
    unittest.main()
