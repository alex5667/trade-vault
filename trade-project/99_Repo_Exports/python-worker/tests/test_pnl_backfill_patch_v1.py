"""Unit tests for tools/pnl_backfill_patch_v1.py correction logic."""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest

from tools.pnl_backfill_patch_v1 import _compute_correction


class TestComputeCorrection(unittest.TestCase):
    def _base(self, **kwargs) -> dict[str, str]:
        base = {
            "close_reason": "INITIAL_SL",
            "lot": "0.10",
            "entry_price": "100.0",
            "sl": "50.0",          # risk = 0.10 * 50 = 5.0
            "pnl_gross": "-10.50", # double-add: 2 × 5.0 + slippage
            "pnl_net": "-10.80",
            "fees": "0.30",
        }
        base.update(kwargs)
        return base

    # ------------------------------------------------------------------
    # Should correct
    # ------------------------------------------------------------------

    def test_double_add_detected(self):
        t = self._base()
        # theory = 0.10 * |100 - 50| = 5.0; |pnl_gross|=10.50 > 1.5*5=7.5 → patch
        c = _compute_correction(t)
        assert c is not None, "expected correction to be generated"
        self.assertAlmostEqual(c["pnl_gross"], -5.0)
        self.assertAlmostEqual(c["pnl_net"], -5.30)    # -5.0 - 0.30 fees
        self.assertAlmostEqual(c["theoretical_loss"], 5.0)
        self.assertAlmostEqual(c["original_pnl_gross"], -10.50)
        self.assertEqual(c["correction_reason"], "double_add_bug_2026-05-14")

    def test_close_reason_raw_sl_accepted(self):
        t = self._base(close_reason="SL", pnl_gross="-10.50")
        c = _compute_correction(t)
        self.assertIsNotNone(c)

    def test_close_reason_initial_sl_case_insensitive(self):
        t = self._base(close_reason="initial_sl")
        c = _compute_correction(t)
        self.assertIsNotNone(c)

    # ------------------------------------------------------------------
    # Should NOT correct
    # ------------------------------------------------------------------

    def test_no_correction_for_tp(self):
        t = self._base(close_reason="TP_LIMIT", pnl_gross="5.0", pnl_net="4.70")
        c = _compute_correction(t)
        self.assertIsNone(c)

    def test_no_correction_within_ratio(self):
        # theory=5.0; |pnl_gross|=7.0 < 1.5*5=7.5 → no patch
        t = self._base(pnl_gross="-7.0", pnl_net="-7.30")
        c = _compute_correction(t)
        self.assertIsNone(c)

    def test_no_correction_dust_risk(self):
        # lot=0.001, risk < $1 threshold → ignored
        t = self._base(lot="0.001", pnl_gross="-0.20", pnl_net="-0.22")
        c = _compute_correction(t)
        self.assertIsNone(c)

    def test_no_correction_missing_sl(self):
        t = self._base(sl="0", pnl_gross="-10.50")
        c = _compute_correction(t)
        self.assertIsNone(c)

    def test_no_correction_normal_1r_loss(self):
        # Honest 1R loss: lot=0.10, entry=100, sl=50, theory=5, pnl_gross=-5.1 (slippage)
        t = self._base(pnl_gross="-5.10", pnl_net="-5.40")
        c = _compute_correction(t)
        self.assertIsNone(c)

    # ------------------------------------------------------------------
    # Fee estimation fallback
    # ------------------------------------------------------------------

    def test_fees_estimated_when_zero(self):
        t = self._base(fees="0", pnl_gross="-10.50", pnl_net="-10.50")
        c = _compute_correction(t)
        assert c is not None, "expected correction to be generated"
        # fees=0, so corrected_pnl_net == corrected_pnl_gross
        self.assertAlmostEqual(c["pnl_net"], c["pnl_gross"])


if __name__ == "__main__":
    unittest.main()
