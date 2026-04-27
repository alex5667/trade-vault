from unittest.mock import MagicMock
from news_pipeline.standby_ingestor import LeaderLock

def test_leader_lock_try_acquire_calls_set_nx_px():
    r = MagicMock()
    r.set.return_value = True
    r.register_script.return_value = lambda keys, args: 1

    lock = LeaderLock(r=r, key="news:ingestor:leader", ttl_ms=8000)
    assert lock.try_acquire() is True
