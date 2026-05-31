from common.time_utils import normalize_epoch_ms


def test_normalize_epoch_ms_seconds():
    # Example: 1672531200 (2023-01-01) in seconds
    res = normalize_epoch_ms(1672531200, now_ms=1672531200000)
    assert res.ts_ms == 1672531200000
    assert res.src_unit == "s"
    assert res.ok is True

def test_normalize_epoch_ms_ms():
    res = normalize_epoch_ms(1672531200000, now_ms=1672531200000)
    assert res.ts_ms == 1672531200000
    assert res.src_unit == "ms"
    assert res.ok is True

def test_normalize_epoch_ms_us():
    res = normalize_epoch_ms(1672531200000000, now_ms=1672531200000)
    assert res.ts_ms == 1672531200000
    assert res.src_unit == "us"
    assert res.ok is True

def test_normalize_epoch_ms_ns():
    res = normalize_epoch_ms(1672531200000000000, now_ms=1672531200000)
    assert res.ts_ms == 1672531200000
    assert res.src_unit == "ns"
    assert res.ok is True

def test_normalize_epoch_ms_invalid():
    res = normalize_epoch_ms("invalid")
    assert res.ok is False
    assert res.err == "ts_parse"

    res = normalize_epoch_ms(None)
    assert res.ok is False
    assert res.err == "ts_missing"

    res = normalize_epoch_ms(-100)
    assert res.ok is False
    assert res.err == "ts_nonpositive"
