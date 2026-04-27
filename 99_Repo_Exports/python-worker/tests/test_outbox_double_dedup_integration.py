import pytest
import uuid
from core.outbox_writer import OutboxWriter
from core.outbox_envelope import make_envelope
import redis

# Use a real or mock redis depending on environment. For safety in CI, we use a real redis if running, else skip.
@pytest.fixture
def real_redis():
    r = redis.Redis(host='localhost', port=6379, db=0)
    try:
        r.ping()
        yield r
    except redis.ConnectionError:
        pytest.skip("Redis not available for integration tests")

def test_integration_outbox_double_dedup(real_redis):
    stream = f"test:outbox:stream:{uuid.uuid4().hex}"
    
    writer = OutboxWriter(redis=real_redis, logger=None, stream_name=stream, max_retries=1)
    
    sid = f"int-test-sig-{uuid.uuid4().hex}"
    
    env = make_envelope(
        signal_id=sid,
        source="test",
        ts_ms=1000,
        kind="test",
        symbol="BTC",
        payload={"msg": "hello"}
    )
    
    # Write 1
    res1 = writer.write(env)
    assert res1.ok
    assert res1.written
    assert not res1.duplicate
    
    # Write 2 (double dedup check on real redis)
    res2 = writer.write(env)
    assert res2.ok
    assert not res2.written
    assert res2.duplicate
    
    # Verify stream only has 1 element
    entries = real_redis.xrange(stream)
    assert len(entries) == 1
    
    # Cleanup
    real_redis.delete(stream)
    real_redis.delete(f"outbox:dedup:{sid}")
