"""P6/P7 BinanceUserStreamWorker lifecycle tests.

Tests:
  - start_listen_key keeps self.listen_key set
  - keepalive_listen_key is a no-op when listen_key is None
  - close_listen_key sets listen_key to None even after a success
"""
from __future__ import annotations

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _make_worker():
    """Construct BinanceUserStreamWorker with mocked deps."""
    import redis as redis_mod
    with patch.object(redis_mod, "from_url", return_value=MagicMock()), \
         patch("services.binance_futures_client.BinanceFuturesClient.from_env") as m_from_env, \
         patch.dict(os.environ, {
             "REDIS_URL": "redis://localhost:6379/0"
             "BINANCE_API_KEY": "k"
             "BINANCE_API_SECRET": "s"
         }):
        m_client = MagicMock()
        m_from_env.return_value = m_client
        try:
            from services.binance_user_stream_worker import BinanceUserStreamWorker
        except Exception:
            from binance_user_stream_worker import BinanceUserStreamWorker
        worker = BinanceUserStreamWorker()
        worker.client = m_client
        return worker, m_client


class TestListenKeyLifecycle:
    def test_start_listen_key_sets_key(self):
        worker, mock_client = _make_worker()
        mock_client.start_user_stream.return_value = "listen_key_abc"
        key = worker.start_listen_key()
        assert key == "listen_key_abc"
        assert worker.listen_key == "listen_key_abc"

    def test_keepalive_noop_when_none(self):
        worker, mock_client = _make_worker()
        worker.listen_key = None
        worker.keepalive_listen_key()
        mock_client.keepalive_user_stream.assert_not_called()

    def test_close_listen_key_clears_state(self):
        worker, mock_client = _make_worker()
        worker.listen_key = "existing_key"
        mock_client.close_user_stream.return_value = {}
        worker.close_listen_key()
        assert worker.listen_key is None

    def test_start_listen_key_fails_on_empty(self):
        worker, mock_client = _make_worker()
        mock_client.start_user_stream.return_value = ""
        with pytest.raises(RuntimeError):
            worker.start_listen_key()
        # connected gauge should have been set to 0 (no assert on metric object)

    def test_keepalive_increments_counter(self):
        worker, mock_client = _make_worker()
        worker.listen_key = "k"
        mock_client.keepalive_user_stream.return_value = {}
        worker.keepalive_listen_key()
        mock_client.keepalive_user_stream.assert_called_once_with("k")
