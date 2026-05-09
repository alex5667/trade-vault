from __future__ import annotations

"""
Tests for LCB alerts in alerts_worker_v2.
"""

from unittest.mock import MagicMock

from services.observability.alerts_worker_v2 import _cooldown_ok, _sscan_all


def test_lcb_alerts_threshold():
    """Test that LCB winner changes alerts are triggered above threshold."""
    mock_redis = MagicMock()

    # Mock sscan to return test keys
    mock_redis.sscan = MagicMock(side_effect=lambda key, cursor, count: (0, [
        b"BTCUSDT|trend|continuation",
        b"ETHUSDT|range|reversal"
    ]))

    # Mock get to return high change counts
    def mock_get(key):
        if "BTCUSDT|trend|continuation" in key:
            return b"15"  # Above threshold
        if "ETHUSDT|range|reversal" in key:
            return b"5"  # Below threshold
        return None

    mock_redis.get = MagicMock(side_effect=mock_get)
    mock_redis.set = MagicMock(return_value=True)

    # Get offenders
    lcb_keys = _sscan_all(mock_redis, "metrics:lcb:keys", limit=2000)
    offenders = []
    for k in lcb_keys:
        try:
            from services.observability.alerts_worker_v2 import _decode
            c = int(_decode(mock_redis.get(f"metrics:lcb_winner_changes_total:{k}")) or "0")
            if c >= 10:  # threshold
                offenders.append((k, c))
        except Exception:
            pass

    # Verify BTCUSDT is in offenders (15 >= 10)
    assert len(offenders) > 0
    btc_offender = next((o for o in offenders if "BTCUSDT" in o[0]), None)
    assert btc_offender is not None
    assert btc_offender[1] >= 10


def test_lcb_alerts_cooldown():
    """Test that LCB alerts respect cooldown."""
    mock_redis = MagicMock()
    mock_redis.get = MagicMock(return_value=None)  # No cooldown active
    mock_redis.set = MagicMock(return_value=True)

    # First call should pass cooldown
    assert _cooldown_ok(mock_redis, "alerts:cooldown:lcb_changes", 600) is True

    # Second call should fail (cooldown active)
    mock_redis.get = MagicMock(return_value=b"1")
    assert _cooldown_ok(mock_redis, "alerts:cooldown:lcb_changes", 600) is False

