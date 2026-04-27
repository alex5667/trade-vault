"""Unit tests for BinanceExecutor._validate_protective_prices.

Tests the nudge-fallback logic added in 2026-03:
  - Valid prices are kept as-is.
  - Prices barely crossed (≤ 0.1% of mark) are nudged to sit 0.05% away from mark.
  - Wildly-crossed prices (> 0.1% beyond mark) are dropped entirely.

No network access, Redis, or API keys required.

Run from project root:
  cd python-worker && PYTHONPATH=. python -m pytest services/tests/test_validate_protective_prices_v1.py -v
"""

import math
import types
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Minimal stub – avoids Redis/API-key init in BinanceExecutor.__init__
# ---------------------------------------------------------------------------

def _make_executor(mark_price: float) -> MagicMock:
    """Return a MagicMock that behaves like BinanceExecutor for validate tests."""
    exe = MagicMock()
    exe._PROTECTIVE_NUDGE_THRESHOLD = 0.001
    exe._PROTECTIVE_NUDGE_OFFSET    = 0.0005

    # Bind the real method to our mock instance
    from services.binance_executor import BinanceExecutor
    exe._validate_protective_prices = (
        BinanceExecutor._validate_protective_prices.__get__(exe, type(exe))
    )

    # Stub client always returns the given mark price
    client = MagicMock()
    client.get_mark_price.return_value = mark_price
    exe._client = client
    return exe, client


# Helper: call the method under test
def _validate(mark: float, side: str, sl=None, tps=None):
    exe, client = _make_executor(mark)
    valid_sl, valid_tps = exe._validate_protective_prices(
        "TESTUSDT", side, sl, tps or [],
        client=client, ref_price=None,
    )
    return valid_sl, valid_tps


MARK = 2000.0
NUDGE_OFF = MARK * 0.0005   # 1.0 USDT away from mark


# ===========================================================================
# LONG – SL
# ===========================================================================

class TestLongSL:
    def test_valid_sl_kept(self):
        # sl significantly below mark → kept unchanged
        sl, _ = _validate(MARK, "LONG", sl=1900.0)
        assert sl == 1900.0

    def test_sl_exactly_at_mark_nudged_down(self):
        # sl == mark → barely crossed → nudged
        sl, _ = _validate(MARK, "LONG", sl=MARK)
        assert sl is not None
        assert sl < MARK
        assert math.isclose(sl, MARK * (1.0 - 0.0005), rel_tol=1e-9)

    def test_sl_barely_above_mark_nudged_down(self):
        # sl 0.05% above mark → within threshold → nudged
        sl_in = MARK * 1.0005
        sl, _ = _validate(MARK, "LONG", sl=sl_in)
        assert sl is not None
        assert sl < MARK

    def test_sl_wildly_above_mark_dropped(self):
        # sl 2% above mark → wildly crossed → dropped
        sl, _ = _validate(MARK, "LONG", sl=MARK * 1.02)
        assert sl is None

    def test_sl_none_ignored(self):
        sl, _ = _validate(MARK, "LONG", sl=None)
        assert sl is None


# ===========================================================================
# LONG – TP
# ===========================================================================

class TestLongTP:
    def test_valid_tp_kept(self):
        _, tps = _validate(MARK, "LONG", tps=[2100.0, 2200.0])
        assert tps == [2100.0, 2200.0]

    def test_tp_exactly_at_mark_nudged_up(self):
        _, tps = _validate(MARK, "LONG", tps=[MARK])
        assert len(tps) == 1
        assert tps[0] > MARK
        assert math.isclose(tps[0], MARK * (1.0 + 0.0005), rel_tol=1e-9)

    def test_tp_barely_below_mark_nudged_up(self):
        tp_in = MARK * 0.9995
        _, tps = _validate(MARK, "LONG", tps=[tp_in])
        assert len(tps) == 1
        assert tps[0] > MARK

    def test_tp_wildly_below_mark_dropped(self):
        _, tps = _validate(MARK, "LONG", tps=[MARK * 0.97])
        assert tps == []

    def test_mixed_tps_partial_keep(self):
        # One valid, one wildly crossed
        _, tps = _validate(MARK, "LONG", tps=[2100.0, MARK * 0.97])
        assert len(tps) == 1
        assert tps[0] == 2100.0


# ===========================================================================
# SHORT – SL
# ===========================================================================

class TestShortSL:
    def test_valid_sl_kept(self):
        sl, _ = _validate(MARK, "SHORT", sl=2100.0)
        assert sl == 2100.0

    def test_sl_exactly_at_mark_nudged_up(self):
        sl, _ = _validate(MARK, "SHORT", sl=MARK)
        assert sl is not None
        assert sl > MARK
        assert math.isclose(sl, MARK * (1.0 + 0.0005), rel_tol=1e-9)

    def test_sl_barely_below_mark_nudged_up(self):
        sl_in = MARK * 0.9995
        sl, _ = _validate(MARK, "SHORT", sl=sl_in)
        assert sl is not None
        assert sl > MARK

    def test_sl_wildly_below_mark_dropped(self):
        sl, _ = _validate(MARK, "SHORT", sl=MARK * 0.97)
        assert sl is None


# ===========================================================================
# SHORT – TP
# ===========================================================================

class TestShortTP:
    def test_valid_tp_kept(self):
        _, tps = _validate(MARK, "SHORT", tps=[1900.0, 1800.0])
        assert tps == [1900.0, 1800.0]

    def test_tp_exactly_at_mark_nudged_down(self):
        _, tps = _validate(MARK, "SHORT", tps=[MARK])
        assert len(tps) == 1
        assert tps[0] < MARK
        assert math.isclose(tps[0], MARK * (1.0 - 0.0005), rel_tol=1e-9)

    def test_tp_barely_above_mark_nudged_down(self):
        tp_in = MARK * 1.0005
        _, tps = _validate(MARK, "SHORT", tps=[tp_in])
        assert len(tps) == 1
        assert tps[0] < MARK

    def test_tp_wildly_above_mark_dropped(self):
        _, tps = _validate(MARK, "SHORT", tps=[MARK * 1.02])
        assert tps == []


# ===========================================================================
# Edge cases
# ===========================================================================

class TestEdgeCases:
    def test_all_prices_invalid_returns_empty(self):
        # Both sl and tp wildly crossed → both dropped
        sl, tps = _validate(MARK, "LONG", sl=MARK * 1.05, tps=[MARK * 0.93])
        assert sl is None
        assert tps == []

    def test_no_mark_price_passthrough(self):
        """When mark price is unavailable (returns 0), return prices as-is."""
        exe, client = _make_executor(0.0)  # 0 → treated as unavailable
        sl, tps = exe._validate_protective_prices(
            "TESTUSDT", "LONG", 1900.0, [2100.0],
            client=client, ref_price=None,
        )
        assert sl == 1900.0
        assert tps == [2100.0]

    def test_ref_price_used_when_mark_unavailable(self):
        """ref_price is used as fallback when client returns 0."""
        from services.binance_executor import BinanceExecutor
        exe = MagicMock()
        exe._PROTECTIVE_NUDGE_THRESHOLD = 0.001
        exe._PROTECTIVE_NUDGE_OFFSET    = 0.0005
        exe._validate_protective_prices = (
            BinanceExecutor._validate_protective_prices.__get__(exe, type(exe))
        )
        client = MagicMock()
        client.get_mark_price.return_value = 0.0   # unavailable
        sl, tps = exe._validate_protective_prices(
            "TESTUSDT", "LONG", 1900.0, [2100.0],
            client=client, ref_price=MARK,
        )
        # ref_price=2000 → sl=1900 valid (below mark), tp=2100 valid (above mark)
        assert sl == 1900.0
        assert tps == [2100.0]

    def test_empty_tps_list(self):
        sl, tps = _validate(MARK, "LONG", sl=1900.0, tps=[])
        assert sl == 1900.0
        assert tps == []
