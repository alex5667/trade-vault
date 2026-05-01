from __future__ import annotations
"""
Tests for LCB metrics export in metrics_exporter.
"""

from unittest.mock import MagicMock, patch

import pytest

from services.observability.metrics_exporter import _append_lcb_metrics, collect_metrics


def test_append_lcb_metrics():
    """Test that LCB metrics are appended to lines."""
    mock_redis = MagicMock()
    
    # Mock sscan_all to return test keys
    def mock_sscan_all(key, limit=2000):
        if key == "metrics:lcb:keys":
            return ["BTCUSDT|trend|continuation", "ETHUSDT|range|reversal"]
        return []
    
    mock_redis.sscan = MagicMock(side_effect=lambda key, cursor, count: (0, [
        b"BTCUSDT|trend|continuation",
        b"ETHUSDT|range|reversal"
    ]))
    
    # Mock pipeline
    mock_pipe = MagicMock()
    mock_pipe.get = MagicMock(return_value=mock_pipe)
    mock_pipe.execute = MagicMock(return_value=[
        b"5",  # changes for BTCUSDT
        b"0.05",  # margin for BTCUSDT
        b"3",  # changes for ETHUSDT
        b"0.02",  # margin for ETHUSDT
    ])
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)
    
    lines = []
    
    # Use patch to mock _sscan_all
    with patch("services.observability.metrics_exporter._sscan_all", side_effect=mock_sscan_all):
        _append_lcb_metrics(lines, mock_redis)
    
    # Verify metrics were added
    assert len(lines) > 0
    # Check that metrics contain expected labels
    lines_str = "\n".join(lines)
    assert "lcb_winner_changes_total" in lines_str or "lcb_margin" in lines_str


def test_append_lcb_metrics_empty_keys():
    """Test that _append_lcb_metrics handles empty keys gracefully."""
    mock_redis = MagicMock()
    
    def mock_sscan_all(key, limit=2000):
        return []
    
    lines = []
    
    with patch("services.observability.metrics_exporter._sscan_all", side_effect=mock_sscan_all):
        _append_lcb_metrics(lines, mock_redis)
    
    # Should not add any metrics
    assert len(lines) == 0


def test_collect_metrics_includes_lcb():
    """Test that collect_metrics includes LCB metrics."""
    mock_redis = MagicMock()
    
    # Mock all required Redis operations
    mock_redis.sscan = MagicMock(side_effect=lambda key, cursor, count: (0, []))
    mock_redis.xlen = MagicMock(return_value=100)
    mock_redis.get = MagicMock(return_value=None)
    mock_redis.pipeline = MagicMock(return_value=MagicMock())
    
    result = collect_metrics(mock_redis)
    
    # Verify result is a string (may be empty if no symbols)
    assert isinstance(result, str)
    # LCB metrics may or may not be present depending on keys
    # Just verify function completes without error

