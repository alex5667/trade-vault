from __future__ import annotations

"""Tests for decision_snapshot_writer._normalize_row — P1 EMIT aliases.

Coverage:
  * decision_mid_at_emit used as fallback when decision_mid is missing
  * expected_slippage_bps_at_emit used as fallback when decision_expected_slippage_bps missing
  * primary fields win over emit aliases
  * backward compat: rows without aliases still normalized correctly
"""

import unittest

try:
    from services.posttrade.decision_snapshot_writer import _normalize_row
    SKIP_REASON = None
except Exception as e:
    _normalize_row = None  # type: ignore
    SKIP_REASON = str(e)


BASE_ROW = {
    "sid": "test-sid-001",
    "decision_ts_ms": 1_700_000_000_000,
    "symbol": "BTCUSDT",
    "venue": "binance",
    "session": "london",
    "tf": "5m",
    "kind": "breakout",
    "side": "LONG",
    "direction": "LONG",
    "tca_ready": True,
}


@unittest.skipIf(SKIP_REASON, f"import failed: {SKIP_REASON}")
class TestNormalizeRowP1Aliases(unittest.TestCase):

    def test_no_aliases_is_backward_compat(self):
        evt = dict(BASE_ROW)
        evt["decision_mid"] = 48000.5
        evt["decision_expected_slippage_bps"] = 3.5
        row = _normalize_row(evt)  # type: ignore
        self.assertAlmostEqual(float(row["decision_mid"]), 48000.5)
        self.assertAlmostEqual(float(row["decision_expected_slippage_bps"]), 3.5)

    def test_decision_mid_at_emit_fallback(self):
        """When decision_mid is absent, decision_mid_at_emit should be used."""
        evt = dict(BASE_ROW)
        evt["decision_mid_at_emit"] = 48100.0
        # No decision_mid in the event
        row = _normalize_row(evt)  # type: ignore
        self.assertAlmostEqual(float(row["decision_mid"]), 48100.0)

    def test_decision_mid_primary_wins_over_emit(self):
        """decision_mid takes priority over decision_mid_at_emit."""
        evt = dict(BASE_ROW)
        evt["decision_mid"] = 48000.0
        evt["decision_mid_at_emit"] = 99999.0  # different value
        row = _normalize_row(evt)  # type: ignore
        self.assertAlmostEqual(float(row["decision_mid"]), 48000.0)

    def test_expected_slippage_at_emit_fallback(self):
        """When decision_expected_slippage_bps absent, expected_slippage_bps_at_emit used."""
        evt = dict(BASE_ROW)
        evt["expected_slippage_bps_at_emit"] = 4.5
        row = _normalize_row(evt)  # type: ignore
        self.assertAlmostEqual(float(row["decision_expected_slippage_bps"]), 4.5)

    def test_expected_slippage_primary_wins_over_emit(self):
        evt = dict(BASE_ROW)
        evt["decision_expected_slippage_bps"] = 2.0
        evt["expected_slippage_bps_at_emit"] = 9.0
        row = _normalize_row(evt)  # type: ignore
        self.assertAlmostEqual(float(row["decision_expected_slippage_bps"]), 2.0)

    def test_decision_price_falls_back_to_emit(self):
        """decision_price falls back to decision_mid_at_emit if no decision_mid or decision_price."""
        evt = dict(BASE_ROW)
        evt["decision_mid_at_emit"] = 47500.0
        row = _normalize_row(evt)  # type: ignore
        # decision_price = decision_price OR decision_mid OR emit
        self.assertAlmostEqual(float(row["decision_price"]), 47500.0)

    def test_both_aliases_none_decision_mid_is_none(self):
        """Without any mid, decision_mid and decision_price should be None."""
        evt = dict(BASE_ROW)
        row = _normalize_row(evt)  # type: ignore
        self.assertIsNone(row["decision_mid"])
        self.assertIsNone(row["decision_price"])

    def test_raises_for_missing_sid(self):
        evt = dict(BASE_ROW)
        evt.pop("sid")
        with self.assertRaises(ValueError):
            _normalize_row(evt)  # type: ignore

    def test_raises_for_zero_ts(self):
        evt = dict(BASE_ROW)
        evt["decision_ts_ms"] = 0
        with self.assertRaises(ValueError):
            _normalize_row(evt)  # type: ignore

    def test_both_aliases_populated(self):
        """Both aliases present — primary should still be preferred."""
        evt = dict(BASE_ROW)
        evt["decision_mid"] = 50000.0
        evt["decision_mid_at_emit"] = 50100.0
        evt["decision_expected_slippage_bps"] = 3.0
        evt["expected_slippage_bps_at_emit"] = 4.0
        row = _normalize_row(evt)  # type: ignore
        self.assertAlmostEqual(float(row["decision_mid"]), 50000.0)
        self.assertAlmostEqual(float(row["decision_expected_slippage_bps"]), 3.0)


if __name__ == "__main__":
    unittest.main()
