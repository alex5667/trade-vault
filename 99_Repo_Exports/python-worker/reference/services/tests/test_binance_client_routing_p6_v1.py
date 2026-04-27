"""P6 tests for BinanceFuturesClient routing and replace_algo_order.

Verifies:
  - STOP_MARKET / TAKE_PROFIT_MARKET → post_algo_order (Algo endpoint)
  - MARKET → post_plain_order (plain endpoint)
  - replace_algo_order cancel+replace semantics
"""
from __future__ import annotations

import pytest
import sys
import os
from unittest.mock import MagicMock, patch, call

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

try:
    from services.binance_futures_client import BinanceFuturesClient
except Exception:
    from binance_futures_client import BinanceFuturesClient


def _make_client() -> BinanceFuturesClient:
    return BinanceFuturesClient(
        api_key="k", api_secret="s", base_url="http://mock"
    )


class TestRouting:
    def test_stop_market_goes_to_algo(self):
        c = _make_client()
        with patch.object(c, "post_algo_order", return_value={"algoId": 1}) as m_algo, \
             patch.object(c, "post_plain_order", return_value={}) as m_plain:
            result = c.post_order({"type": "STOP_MARKET", "symbol": "BTCUSDT", "side": "SELL", "closePosition": "true"})
            m_algo.assert_called_once()
            m_plain.assert_not_called()

    def test_take_profit_market_goes_to_algo(self):
        c = _make_client()
        with patch.object(c, "post_algo_order", return_value={"algoId": 2}) as m_algo, \
             patch.object(c, "post_plain_order", return_value={}) as m_plain:
            c.post_order({"type": "TAKE_PROFIT_MARKET", "symbol": "BTCUSDT", "side": "SELL"})
            m_algo.assert_called_once()
            m_plain.assert_not_called()

    def test_market_goes_to_plain(self):
        c = _make_client()
        with patch.object(c, "post_algo_order", return_value={}) as m_algo, \
             patch.object(c, "post_plain_order", return_value={"orderId": 3}) as m_plain:
            c.post_order({"type": "MARKET", "symbol": "BTCUSDT", "side": "BUY", "quantity": "0.01"})
            m_plain.assert_called_once()
            m_algo.assert_not_called()


class TestReplaceAlgoOrder:
    def test_replace_calls_cancel_then_create(self):
        c = _make_client()
        cancel_resp = {"algoId": 10, "status": "NEW"}
        create_resp = {"algoId": 11, "status": "NEW"}
        with patch.object(c, "cancel_algo_order", return_value=cancel_resp) as m_cancel, \
             patch.object(c, "post_algo_order", return_value=create_resp) as m_create:
            result = c.replace_algo_order(
                "BTCUSDT",
                cancel_algo_id=10,
                new_params={"symbol": "BTCUSDT", "side": "SELL", "type": "STOP_MARKET"},
            )
            m_cancel.assert_called_once_with("BTCUSDT", algo_id=10, client_algo_id=None)
            m_create.assert_called_once()
            assert result["cancel"] is cancel_resp
            assert result["create"] is create_resp
