from news_pipeline.standby_ingestor import bucket_start_ms, stable_uid


def test_stable_uid_deterministic():
    a = stable_uid("cryptopanic", "u", "t", "id", "123")
    b = stable_uid("cryptopanic", "u", "t", "id", "123")
    assert a == b
    assert len(a) == 24

def test_bucket_start_ms():
    # 6h bucket
    b = bucket_start_ms(10_000, 6 * 3600)
    assert b == 0
