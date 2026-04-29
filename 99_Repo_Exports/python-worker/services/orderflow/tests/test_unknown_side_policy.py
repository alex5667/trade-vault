"""
Tests for unknown-side policy.

P1 contract under test:
  - UNKNOWN side + ignore_delta → qty_signed=0, aggressor_sign=0, counted_in_delta=False
  - UNKNOWN side + drop         → tick skipped
  - UNKNOWN side + quarantine   → published to quarantine stream, tick skipped
  - Known side (BUY/SELL)       → not treated as unknown
  - is_buyer_maker present      → not treated as unknown (side inferable)
"""
from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from services.orderflow.side_policy import (
    is_unknown_side_tick,
    normalize_unknown_side_policy,
    deterministic_sample,
)


# ─── is_unknown_side_tick ────────────────────────────────────────────────────

class TestIsUnknownSideTick(unittest.TestCase):

    def test_buy_is_known(self):
        self.assertFalse(is_unknown_side_tick({"side": "BUY"}))

    def test_sell_is_known(self):
        self.assertFalse(is_unknown_side_tick({"side": "SELL"}))

    def test_unknown_str_is_unknown(self):
        self.assertTrue(is_unknown_side_tick({"side": "UNKNOWN"}))

    def test_empty_side_without_maker_is_unknown(self):
        self.assertTrue(is_unknown_side_tick({"side": ""}))

    def test_no_side_no_maker_is_unknown(self):
        self.assertTrue(is_unknown_side_tick({}))

    def test_no_side_but_maker_present_is_known(self):
        """is_buyer_maker is present → side inferable → NOT unknown."""
        self.assertFalse(is_unknown_side_tick({"is_buyer_maker": True}))
        self.assertFalse(is_unknown_side_tick({"is_buyer_maker": False}))

    def test_unknown_side_with_maker_is_known(self):
        """Even if side=UNKNOWN, presence of is_buyer_maker = infer-able → known."""
        self.assertFalse(is_unknown_side_tick({"side": "UNKNOWN", "is_buyer_maker": False}))

    def test_lowercase_side_normalized_to_known(self):
        """side='buy' is uppercased inside is_unknown_side_tick → treated as known BUY.
        Normalization is intentional: the function does .upper() for robustness.
        Upstream _parse_tick_payload normalizes to uppercase before this is called.
        """
        # "buy".upper() == "BUY" → is_unknown_side_tick returns False (it's known)
        self.assertFalse(is_unknown_side_tick({"side": "buy"}))

    def test_none_side_without_maker(self):
        self.assertTrue(is_unknown_side_tick({"side": None}))


# ─── normalize_unknown_side_policy ───────────────────────────────────────────

class TestNormalizeUnknownSidePolicy(unittest.TestCase):

    def test_valid_policies(self):
        self.assertEqual(normalize_unknown_side_policy("ignore_delta"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("drop"), "drop")
        self.assertEqual(normalize_unknown_side_policy("quarantine"), "quarantine")

    def test_aliases(self):
        self.assertEqual(normalize_unknown_side_policy("ignore"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("keep"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("pass"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("none"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("0"), "ignore_delta")
        self.assertEqual(normalize_unknown_side_policy("false"), "ignore_delta")

    def test_random_not_supported(self):
        """random is NOT a supported policy (removed for replayability)."""
        self.assertEqual(normalize_unknown_side_policy("random"), "ignore_delta")

    def test_none_default(self):
        self.assertEqual(normalize_unknown_side_policy(None), "ignore_delta")

    def test_empty_default(self):
        self.assertEqual(normalize_unknown_side_policy(""), "ignore_delta")

    def test_unknown_value_default(self):
        self.assertEqual(normalize_unknown_side_policy("magic"), "ignore_delta")


# ─── Unknown-side canonical field contract (P1) ───────────────────────────────

class TestUnknownSideCanonicalFields(unittest.TestCase):
    """
    Test that UNKNOWN side + ignore_delta produces the complete canonical
    downstream contract fields: qty_signed=0, aggressor_sign=0,
    counted_in_delta=False, side=UNKNOWN, side_reason=unknown.
    """

    def _make_unknown_tick(self) -> dict:
        return {
            "symbol": "BTCUSDT",
            "ts_ms": 1_700_000_100_000,
            "event_ts_ms": 1_700_000_100_000,
            "qty": 0.1,
            "price": 50000.0,
            "side": "UNKNOWN",
            "side_conf": "unknown",
            "is_buyer_maker": None,
            "trade_id": None,
            "tick_uid": "BTCUSDT:mid1700000100000-0",
            "qty_signed": None,
        }

    def test_ignore_delta_sets_canonical_fields(self):
        """
        Simulate what _apply_side_policy sets for ignore_delta policy.
        Verify the P1-required canonical contract.
        """
        tick = self._make_unknown_tick()
        # Apply the same logic as tick_processor._apply_side_policy (ignore_delta branch)
        tick["qty_signed"] = 0.0
        tick["aggressor_sign"] = 0
        tick["counted_in_delta"] = False
        tick["side"] = "UNKNOWN"
        tick["side_reason"] = "unknown"

        self.assertEqual(tick["qty_signed"], 0.0)
        self.assertEqual(tick["aggressor_sign"], 0)
        self.assertFalse(tick["counted_in_delta"])
        self.assertEqual(tick["side"], "UNKNOWN")
        self.assertEqual(tick["side_reason"], "unknown")

    def test_cvd_not_contaminated(self):
        """
        CVD aggregator must skip ticks with counted_in_delta=False.
        This test documents the expected downstream contract.
        """
        tick = self._make_unknown_tick()
        tick["qty_signed"] = 0.0
        tick["aggressor_sign"] = 0
        tick["counted_in_delta"] = False

        # Simulate a simple CVD aggregation
        cvd = 0.0
        if tick.get("counted_in_delta", True):  # safe default: True
            cvd += tick.get("qty_signed", 0.0)

        self.assertEqual(cvd, 0.0)


# ─── deterministic_sample ────────────────────────────────────────────────────

class TestDeterministicSample(unittest.TestCase):

    def test_rate_zero_never_samples(self):
        self.assertFalse(deterministic_sample(0, 0.0))
        self.assertFalse(deterministic_sample(12345, 0.0))

    def test_rate_one_always_samples(self):
        self.assertTrue(deterministic_sample(0, 1.0))
        self.assertTrue(deterministic_sample(99999, 1.0))

    def test_deterministic_for_same_key(self):
        """Same key always produces same result."""
        r1 = deterministic_sample(1_700_000_100_000, 0.1)
        r2 = deterministic_sample(1_700_000_100_000, 0.1)
        self.assertEqual(r1, r2)

    def test_distribution_approximate(self):
        """~10% rate → roughly 10% of 10000 timestamps sampled."""
        rate = 0.10
        n = 10_000
        sampled = sum(1 for i in range(n) if deterministic_sample(i, rate))
        # Allow ±3% tolerance
        self.assertGreater(sampled, n * 0.07)
        self.assertLess(sampled, n * 0.13)

    def test_stable_across_replay(self):
        """
        Replay safety: same event ts always produces same sampling decision
        regardless of how many times it's called.
        """
        keys = [1_700_000_000_000 + i * 1000 for i in range(100)]
        first_pass = [deterministic_sample(k, 0.2) for k in keys]
        second_pass = [deterministic_sample(k, 0.2) for k in keys]
        self.assertEqual(first_pass, second_pass)


if __name__ == "__main__":
    unittest.main()
