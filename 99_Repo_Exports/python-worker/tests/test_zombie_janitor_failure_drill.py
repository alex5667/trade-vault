"""
Failure drill for the ZombiePositionJanitor.

Simulates target write failures inside the main cleanup loop, verifying that 
the service catches transient connection states elegantly and continues operating.
"""
from unittest.mock import Mock, patch

from services.zombie_position_janitor import ORDERS_OPEN, run_cleanup


def test_zombie_janitor_handles_redis_srem_failure():
    # Setup mock Redis
    mock_redis = Mock()
    # Return 1 pos id
    mock_redis.smembers.return_value = {b"BTCUSDT_virtual_123"}
    mock_redis.exists.return_value = True
    mock_redis.scard.return_value = 1

    # We pretend the key lives long enough to be swept
    # Mock _get_position_age_sec to return an old age

    # Simulate a network failure ONLY when attempting to save close reason
    mock_redis.hset.side_effect = ConnectionError("Connection reset by peer")

    # And srem throws timeout
    mock_redis.srem.side_effect = TimeoutError("Redis SREM timeout")

    # We must patch the time to make it look old
    with patch("services.zombie_position_janitor.time.time", return_value=1700000000000), \
         patch("services.zombie_position_janitor._get_position_age_sec", return_value=5000):
        # Run the sweeping cycle
        # Because of the ConnectionError inside the try block, the loop logs it and should not crash
        run_cleanup(mock_redis)

    # Check that srem was formally attempted without an unhandled crash propagating up
    mock_redis.srem.assert_called_once_with(ORDERS_OPEN, b"BTCUSDT_virtual_123")

def test_zombie_janitor_handles_redis_smembers_failure():
    # Setup mock Redis
    mock_redis = Mock()

    # If the smembers fails, it should catch and return 0
    mock_redis.smembers.side_effect = ConnectionError("Redis completely down")

    with patch("services.zombie_position_janitor.OPEN_GAUGE"), \
         patch("services.zombie_position_janitor.SCANNED"):
        removed = run_cleanup(mock_redis)

    assert removed == 0
    mock_redis.smembers.assert_called_once()
