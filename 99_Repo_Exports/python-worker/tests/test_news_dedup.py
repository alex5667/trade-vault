import fakeredis
from news_pipeline.ingestor_service import _dedup_pass

def test_dedup_pass_once():
    r = fakeredis.FakeRedis(decode_responses=True)
    uid = "abc"
    assert _dedup_pass(r, uid, ttl_sec=10) is True
    assert _dedup_pass(r, uid, ttl_sec=10) is False
