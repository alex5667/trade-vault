from utils.time_utils import get_ny_time_millis
import pytest
import time
import json
from unittest.mock import MagicMock, patch

# Need to reload or inject DRY_RUN to test it since services.zombie_position_janitor
# imports os.environ at module level.
import services.zombie_position_janitor as janitor

@pytest.fixture
def redis_client():
    r = MagicMock()
    # Mock decode_responses just in case
    r.decode_responses = True
    yield r

def test_zombie_janitor_dry_run(redis_client):
    """Verify that when DRY_RUN=True, positions are not removed."""
    pos_id = "test_pos_dry"
    now_ms = get_ny_time_millis()
    ancient_ms = now_ms - (janitor.MAX_AGE_SEC * 1000) - 50000 

    # Setup mock behavior
    redis_client.smembers.return_value = {pos_id}
    redis_client.exists.return_value = True
    
    # hget fallback isn't used directly, it uses hmget for age
    redis_client.hmget.return_value = [str(int(ancient_ms)), None, None, None]
    
    # Mock DRY_RUN to True
    with patch.object(janitor, 'DRY_RUN', True):
        removed = janitor.run_cleanup(redis_client)
        
        # In DRY_RUN, it claims 'removed' internally for logging but doesn't actually delete
        assert removed == 1
        
        # Verify it wasn't actually deleted
        redis_client.srem.assert_not_called()
        # Verify hash wasn't updated with "closed"
        redis_client.hset.assert_not_called()

def test_zombie_janitor_normal_run(redis_client):
    """Verify that when DRY_RUN=False, positions are actually removed."""
    pos_id = "test_pos_real"
    now_ms = get_ny_time_millis()
    ancient_ms = now_ms - (janitor.MAX_AGE_SEC * 1000) - 50000 

    # Setup mock behavior
    redis_client.smembers.return_value = {pos_id}
    redis_client.exists.return_value = True
    redis_client.hmget.return_value = [str(int(ancient_ms)), None, None, None]
    
    # Mock DRY_RUN to False
    with patch.object(janitor, 'DRY_RUN', False):
        removed = janitor.run_cleanup(redis_client)
        
        assert removed == 1
        
        # Verify it WAS actually deleted from the set
        redis_client.srem.assert_called_once_with(janitor.ORDERS_OPEN, pos_id)
        
        # Verify hash was marked closed
        redis_client.hset.assert_called_once()
        args, kwargs = redis_client.hset.call_args
        assert args[0] == f"order:{pos_id}"
        assert kwargs['mapping']['closed'] == "1"
        assert kwargs['mapping']['close_reason'] == "ZOMBIE_JANITOR"
