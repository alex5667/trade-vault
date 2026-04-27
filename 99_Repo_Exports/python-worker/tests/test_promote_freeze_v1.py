import os
import sys
import time
import pytest
from unittest.mock import patch, MagicMock

# Force python to load tick_flow_full's core before python-worker's core
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tick_flow_full')))

from core.promote_freeze import FreezeState, read_freeze, set_freeze, clear_freeze, freeze_key


@pytest.fixture
def mock_redis():
    with patch("core.promote_freeze._client") as mock:
        r = MagicMock()
        mock.return_value = r
        yield r

def test_read_freeze_not_active(mock_redis):
    mock_redis.hgetall.return_value = {}
    st = read_freeze("redis://dummy")
    assert not st.active
    assert st.until_ts_ms == 0

def test_set_freeze(mock_redis):
    # Mock time
    with patch("time.time", return_value=1000.0):
        ok = set_freeze("redis://dummy", duration_s=100, reason="test_reason")
        assert ok
        # 1000.0 s * 1000 = 1000000 ms
        # + 100 * 1000 ms = 1000000 + 100000 = 1100000 ms
        mock_redis.hset.assert_called_with(freeze_key(), mapping={
            "active": "1",
            "until_ts_ms": "1100000",
            "set_ts_ms": "1000000",
            "reason": "test_reason",
            "source": "monitoring_smoke"
        })
        mock_redis.expire.assert_called()

def test_read_freeze_active(mock_redis):
    with patch("time.time", return_value=1000.0):
        mock_redis.hgetall.return_value = {
            "active": "1",
            "until_ts_ms": "1100000",
            "reason": "test",
            "source": "src"
        }
        st = read_freeze("redis://dummy")
        assert st.active
        assert st.until_ts_ms == 1100000
        assert st.reason == "test"
        assert st.source == "src"

def test_read_freeze_expired(mock_redis):
    with patch("time.time", return_value=2000.0):
        mock_redis.hgetall.return_value = {
            "active": "1",
            "until_ts_ms": "1100000",
            "reason": "test",
            "source": "src"
        }
        st = read_freeze("redis://dummy")
        assert not st.active
        assert st.until_ts_ms == 1100000
        mock_redis.delete.assert_called_with(freeze_key())

def test_clear_freeze(mock_redis):
    ok = clear_freeze("redis://dummy")
    assert ok
    mock_redis.delete.assert_called_with(freeze_key())
