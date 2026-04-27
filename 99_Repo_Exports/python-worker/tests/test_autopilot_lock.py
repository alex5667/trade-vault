from utils.time_utils import get_ny_time_millis
import time
import fakeredis


def test_setnx_lock():
    r = fakeredis.FakeRedis(decode_responses=True)
    key = "lock:tm_autopilot:v1"
    assert r.set(key, str(get_ny_time_millis()), nx=True, ex=5) is True
    res = r.set(key, str(get_ny_time_millis()), nx=True, ex=5)
    assert res is None or res is False
