from __future__ import annotations

"""
Tests for Redis Stream integration in calibration and replay tools.

Tests:
- calib_replay_from_inputs.py: load_payload_redis_streams
- replay_runner.py: iter_ctx_redis_streams
- calibrate_local_thresholds.py: load_signals_from_redis
"""


import json
from unittest.mock import Mock, patch

from local_calibration.calibrate_local_thresholds import (
    _to_float,
    load_signals_from_redis,
)

# Test imports
from tools.calib_replay_from_inputs import _to_str, load_payload_redis_streams
from tools.replay.replay_runner import iter_ctx_redis_streams


class TestToStr:
    """Test _to_str helper function."""

    def test_to_str_none(self):
        assert _to_str(None) == ""

    def test_to_str_string(self):
        assert _to_str("test") == "test"

    def test_to_str_bytes(self):
        assert _to_str(b"test") == "test"

    def test_to_str_bytearray(self):
        assert _to_str(bytearray(b"test")) == "test"

    def test_to_str_int(self):
        assert _to_str(123) == "123"

    def test_to_str_exception(self):
        # Test with object that raises exception on str()
        class BadStr:
            def __str__(self):
                raise Exception("bad")

        assert _to_str(BadStr()) == ""


class TestLoadPayloadRedisStreams:
    """Test load_payload_redis_streams function."""

    @patch("tools.calib_replay_from_inputs.redis")
    def test_load_payload_redis_streams_single_stream(self, mock_redis_module):
        """Test loading from a single stream (no {sym} template)."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        # Mock xread response: single stream, no {sym}
        mock_redis.xread.return_value = [
            (
                "events:microbar_closed",
                [
                    ("1000-0", {"payload": json.dumps({"symbol": "BTCUSDT", "ts_ms": 1000})}),
                    ("1001-0", {"symbol": "ETHUSDT", "ts_ms": 1001}),
                ],
            )
        ]

        result = load_payload_redis_streams(
            redis_url="redis://localhost:6379/0",
            stream="events:microbar_closed",
            symbols_set="events:microbar_closed:symbols",
            start_id="0-0",
            count=1000,
            max_batches=1,
        )

        assert len(result) == 2
        assert result[0]["symbol"] == "BTCUSDT"
        assert result[1]["symbol"] == "ETHUSDT"

    @patch("tools.calib_replay_from_inputs.redis")
    def test_load_payload_redis_streams_with_sym_template(self, mock_redis_module):
        """Test loading from per-symbol streams using {sym} template."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        # Mock smembers for symbols set
        mock_redis.smembers.return_value = {"BTCUSDT", "ETHUSDT"}

        # Mock xread responses for each symbol
        def xread_side_effect(streams, **kwargs):
            stream_name = list(streams.keys())[0]
            if "BTCUSDT" in stream_name:
                return [
                    (
                        "events:microbar_closed:BTCUSDT",
                        [("1000-0", {"payload": json.dumps({"symbol": "BTCUSDT", "ts_ms": 1000})})],
                    )
                ]
            elif "ETHUSDT" in stream_name:
                return [
                    (
                        "events:microbar_closed:ETHUSDT",
                        [("1001-0", {"payload": json.dumps({"symbol": "ETHUSDT", "ts_ms": 1001})})],
                    )
                ]
            return []

        mock_redis.xread.side_effect = xread_side_effect

        result = load_payload_redis_streams(
            redis_url="redis://localhost:6379/0",
            stream="events:microbar_closed:{sym}",
            symbols_set="events:microbar_closed:symbols",
            start_id="0-0",
            count=1000,
            max_batches=1,
        )

        assert len(result) == 2
        symbols = [r["symbol"] for r in result]
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols

    @patch("tools.calib_replay_from_inputs.redis")
    def test_load_payload_redis_streams_flat_fields(self, mock_redis_module):
        """Test loading when payload is not JSON, using flat fields."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        mock_redis.xread.return_value = [
            (
                "events:microbar_closed",
                [
                    ("1000-0", {"symbol": "BTCUSDT", "ts_ms": "1000", "regime": "trend"}),
                ],
            )
        ]

        result = load_payload_redis_streams(
            redis_url="redis://localhost:6379/0",
            stream="events:microbar_closed",
            symbols_set="events:microbar_closed:symbols",
            start_id="0-0",
            count=1000,
            max_batches=1,
        )

        assert len(result) == 1
        assert result[0]["symbol"] == "BTCUSDT"
        assert result[0]["ts_ms"] == "1000"
        assert result[0]["regime"] == "trend"


class TestIterCtxRedisStreams:
    """Test iter_ctx_redis_streams function."""

    @patch("tools.replay.replay_runner.redis")
    def test_iter_ctx_redis_streams_single_stream(self, mock_redis_module):
        """Test iterating ctx from a single stream."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        mock_redis.xread.return_value = [
            (
                "events:microbar_closed",
                [
                    ("1000-0", {"payload": json.dumps({"symbol": "BTCUSDT", "ts_ms": 1000})}),
                ],
            )
        ]

        ctxs = list(
            iter_ctx_redis_streams(
                redis_url="redis://localhost:6379/0",
                stream="events:microbar_closed",
                symbols_set="events:microbar_closed:symbols",
                start_id="0-0",
                count=500,
                max_batches=1,
            )
        )

        assert len(ctxs) == 1
        assert ctxs[0].symbol == "BTCUSDT"
        assert ctxs[0].ts_ms == 1000

    @patch("tools.replay.replay_runner.redis")
    def test_iter_ctx_redis_streams_with_symbols_param(self, mock_redis_module):
        """Test iterating with explicit symbols parameter."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        def xread_side_effect(streams, **kwargs):
            stream_name = list(streams.keys())[0]
            if "BTCUSDT" in stream_name:
                return [
                    (
                        "events:microbar_closed:BTCUSDT",
                        [("1000-0", {"payload": json.dumps({"symbol": "BTCUSDT", "ts_ms": 1000})})],
                    )
                ]
            return []

        mock_redis.xread.side_effect = xread_side_effect

        ctxs = list(
            iter_ctx_redis_streams(
                redis_url="redis://localhost:6379/0",
                stream="events:microbar_closed:{sym}",
                symbols_set="events:microbar_closed:symbols",
                start_id="0-0",
                count=500,
                max_batches=1,
                symbols=["BTCUSDT"],
            )
        )

        assert len(ctxs) == 1
        assert ctxs[0].symbol == "BTCUSDT"


class TestLoadSignalsFromRedis:
    """Test load_signals_from_redis function."""

    @patch("local_calibration.calibrate_local_thresholds.redis")
    def test_load_signals_from_redis_position_closed(self, mock_redis_module):
        """Test loading POSITION_CLOSED events from Redis stream."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        mock_redis.xread.return_value = [
            (
                "trades:closed",
                [
                    (
                        "1000-0",
                        {
                            "event_type": "POSITION_CLOSED",
                            "symbol": "BTCUSDT",
                            "entry_tag": "breakout",
                            "regime": "trend",
                            "r_multiple": "2.5",
                        },
                    ),
                    (
                        "1001-0",
                        {
                            "event": "POSITION_CLOSED",
                            "symbol": "ETHUSDT",
                            "entry_tag": "reversal",
                            "r_mult": "1.8",
                        },
                    ),
                ],
            )
        ]

        with patch("local_calibration.calibrate_local_thresholds.REDIS_URL", "redis://localhost:6379/0"), \
             patch("local_calibration.calibrate_local_thresholds.TRADES_CLOSED_STREAM", "trades:closed"), \
             patch("local_calibration.calibrate_local_thresholds.TRADES_CLOSED_START_ID", "0-0"):
            result = load_signals_from_redis()

        assert len(result) == 2
        assert result[0].symbol == "BTCUSDT"
        assert result[0].session == "breakout"
        assert result[0].regime == "trend"
        assert result[0].pnl_r == 2.5

        assert result[1].symbol == "ETHUSDT"
        assert result[1].session == "reversal"
        assert result[1].pnl_r == 1.8

    @patch("local_calibration.calibrate_local_thresholds.redis")
    def test_load_signals_from_redis_filter_non_position_closed(self, mock_redis_module):
        """Test that non-POSITION_CLOSED events are filtered out."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        mock_redis.xread.return_value = [
            (
                "trades:closed",
                [
                    ("1000-0", {"event_type": "POSITION_OPENED", "symbol": "BTCUSDT"}),
                    ("1001-0", {"event_type": "POSITION_CLOSED", "symbol": "ETHUSDT", "r_multiple": "1.5"}),
                ],
            )
        ]

        with patch("local_calibration.calibrate_local_thresholds.REDIS_URL", "redis://localhost:6379/0"), \
             patch("local_calibration.calibrate_local_thresholds.TRADES_CLOSED_STREAM", "trades:closed"), \
             patch("local_calibration.calibrate_local_thresholds.TRADES_CLOSED_START_ID", "0-0"):
            result = load_signals_from_redis()

        assert len(result) == 1
        assert result[0].symbol == "ETHUSDT"

    @patch("local_calibration.calibrate_local_thresholds.redis")
    def test_load_signals_from_redis_pnl_calculation(self, mock_redis_module):
        """Test PnL calculation from pnl/risk_usd when r_multiple is missing."""
        mock_redis = Mock()
        mock_redis_module.Redis.from_url.return_value = mock_redis

        mock_redis.xread.return_value = [
            (
                "trades:closed",
                [
                    (
                        "1000-0",
                        {
                            "event_type": "POSITION_CLOSED",
                            "symbol": "BTCUSDT",
                            "pnl": "100.0",
                            "risk_usd": "50.0",
                        },
                    ),
                ],
            )
        ]

        with patch("local_calibration.calibrate_local_thresholds.REDIS_URL", "redis://localhost:6379/0"), \
             patch("local_calibration.calibrate_local_thresholds.TRADES_CLOSED_STREAM", "trades:closed"), \
             patch("local_calibration.calibrate_local_thresholds.TRADES_CLOSED_START_ID", "0-0"):
            result = load_signals_from_redis()

        assert len(result) == 1
        assert result[0].pnl_r == 2.0  # 100.0 / 50.0

    def test_to_float(self):
        """Test _to_float helper function."""
        assert _to_float("1.5") == 1.5
        assert _to_float(2.0) == 2.0
        assert _to_float(None) == 0.0
        assert _to_float("") == 0.0
        assert _to_float("invalid", default=99.0) == 99.0

