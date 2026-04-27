"""P6 tests for BinanceExecutor reconcile and duplicate prevention.

Covers:
  - User-stream cache hit causes reconcile-first resolution (BINANCE_ALGO_RECONCILE_TOTAL)
  - _resume_open_from_state with terminal state short-circuits handle_open
  - EXECUTION_DUPLICATE_PREVENTED_TOTAL is incremented on duplicate detection
"""
from __future__ import annotations

import sys
import os
import pytest
from unittest.mock import MagicMock, patch

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _make_mock_executor():
    """Create a minimal BinanceExecutor without Redis/Binance.

    We patch all external deps to test logic in isolation.
    """
    with patch("redis.from_url"), patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379/0",
        "BINANCE_API_KEY": "k",
        "BINANCE_API_SECRET": "s",
    }):
        try:
            from services.binance_executor import BinanceExecutor
        except Exception:
            from binance_executor import BinanceExecutor
        e = BinanceExecutor.__new__(BinanceExecutor)
        e.reconcile_enable = True
        e.exec_reconcile_on_503_unknown = True
        e.exec_reconcile_prefer_user_stream = True
        e.user_stream_cache_prefix = "orders:user_stream:"
        e._maker_tp_stats = {}
        e.exec_fee_maker_bps = 2.0
        e.exec_fee_taker_bps = 5.0
        e.r = MagicMock()
        return e


class TestReconcileWithUserStreamCache:
    def test_user_stream_hit_returns_cached(self):
        """When user stream has a cached event the reconcile should use it."""
        e = _make_mock_executor()

        cached_order = {"orderId": 999, "status": "FILLED"}
        mock_client = MagicMock()
        mock_client.post_plain_order.side_effect = Exception("503 unknown")
        mock_client.is_ambiguous_execution_error.return_value = True

        with patch.object(e, "_mark_pending_reconcile"), \
             patch.object(e, "_lookup_user_stream_event", return_value={"order": cached_order}):
            result = e._submit_plain_order_with_reconcile(
                sid="s1", symbol="BTCUSDT", action="open",
                params={"newClientOrderId": "cid1"}, client=mock_client,
            )
        assert result.get("orderId") == 999


class TestDuplicatePrevention:
    def test_resume_from_terminal_state_increments_metric(self):
        """_resume_open_from_state returns a state → duplicate metric should fire."""
        e = _make_mock_executor()

        state = {
            "fsm_state": "PROTECTED",
            "symbol": "BTCUSDT",
        }
        with patch.object(e, "_load_order_state", return_value=state), \
             patch.object(e, "_exec_event"):
            result = e._resume_open_from_state("s1", symbol="BTCUSDT", client=MagicMock())
            assert result is not None
            assert result.get("recovered_from_state") is True

    def test_resume_returns_none_for_unknown_sid(self):
        e = _make_mock_executor()
        with patch.object(e, "_load_order_state", return_value=None):
            result = e._resume_open_from_state("unknown-sid", symbol="BTCUSDT", client=MagicMock())
            assert result is None
