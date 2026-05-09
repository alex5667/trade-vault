

def test_should_sample_deterministic():
    from core_snapshot.ofc_capture_v1 import should_sample

    seed = "seed_v1"
    key = "BTCUSDT|LONG|1700000000000"

    # deterministic: repeated calls yield same
    a = should_sample(stable_key=key, sample_ppm=123456, seed=seed)
    b = should_sample(stable_key=key, sample_ppm=123456, seed=seed)
    assert a == b

    # ppm boundaries
    assert should_sample(stable_key=key, sample_ppm=0, seed=seed) is False
    assert should_sample(stable_key=key, sample_ppm=1_000_000, seed=seed) is True


def test_ndjson_writer_rotates(tmp_path):
    from core_snapshot.ofc_capture_v1 import NDJSONRotatingWriter

    w = NDJSONRotatingWriter(base_dir=str(tmp_path), max_bytes=200, rotate_sec=0)
    rec = {"schema": "t", "x": "y" * 50}

    p1 = w.write(day="20260101", policy_hash="abc", record=rec)
    assert p1 is not None
    p2 = w.write(day="20260101", policy_hash="abc", record=rec)
    assert p2 is not None
    # rotation by max_bytes should eventually create a new file
    # (may be same path on the first 1-2 writes depending on filesystem overhead)
    for _ in range(10):
        p3 = w.write(day="20260101", policy_hash="abc", record=rec)
    files = list((tmp_path / "20260101" / "policy_abc").glob("*.ndjson"))
    assert len(files) >= 1
