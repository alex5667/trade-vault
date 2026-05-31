from datetime import UTC, datetime

from handlers.utils import (
    _parse_bool,
    compute_daily_pivots,
    minutes_of_day_from_epoch_ms,
    normalize_epoch_ms,
    normalize_pivots_bundle,
)


def test_normalize_epoch_ms_seconds_to_ms():
    now = 1700000000000
    assert normalize_epoch_ms(1700000000, now_ms=now) == 1700000000 * 1000


def test_normalize_epoch_ms_ms_passthrough():
    now = 1700000000000
    assert normalize_epoch_ms(1700000000000, now_ms=now) == 1700000000000


def test_normalize_epoch_ms_string_numeric_seconds():
    now = 1700000000000
    assert normalize_epoch_ms("1700000000", now_ms=now) == 1700000000 * 1000


def test_normalize_epoch_ms_string_iso():
    now = 1700000000000
    ts = "2025-12-23T10:00:00Z"
    out = normalize_epoch_ms(ts, now_ms=now)
    assert out > 0


def test_normalize_epoch_ms_datetime():
    now = 1700000000000
    dt = datetime(2025, 12, 23, 10, 0, 0, tzinfo=UTC)
    assert normalize_epoch_ms(dt, now_ms=now) == int(dt.timestamp() * 1000)


def test_normalize_epoch_ms_nan_inf_do_not_break():
    now = 1700000000000
    assert normalize_epoch_ms(float("nan"), now_ms=now) == now
    assert normalize_epoch_ms(float("inf"), now_ms=now) == now
    assert normalize_epoch_ms(-123, now_ms=now) == now


def test_normalize_epoch_ms_rejects_intraday_values():
    now = 1700000000000
    # 1439 minutes-of-day must NOT be treated as epoch
    assert normalize_epoch_ms(1439, now_ms=now) == now
    # strict mode should raise
    try:
        normalize_epoch_ms(1439, now_ms=now, strict=True)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_minutes_of_day_from_epoch_ms():
    dt = datetime(2025, 12, 23, 10, 15, 0, tzinfo=UTC)
    ms = int(dt.timestamp() * 1000)
    assert minutes_of_day_from_epoch_ms(ms) == 10 * 60 + 15


def test_normalize_pivots_bundle_from_raw_levels():
    now = 1700000000000
    raw = {"pivot": 100.0, "r1": "101.5", "bad": "x"}
    b = normalize_pivots_bundle(raw, now_ms=now)
    assert isinstance(b, dict)
    assert b["ts_ms"] == now
    assert "date" in b and isinstance(b["date"], str)
    assert b["hlc"] is None
    assert b["pivots"]["pivot"] == 100.0
    assert b["pivots"]["r1"] == 101.5
    assert "bad" not in b["pivots"]


def test_normalize_pivots_bundle_keeps_bundle_and_normalizes_ts_seconds_to_ms():
    now = 1700000000000
    # ts_ms accidentally provided in seconds
    bundle = {
        "ts_ms": 1700000000,
        "hlc": {"high": 110, "low": 90, "close": 100},
        "pivots": {"pivot": 100, "r1": 101.0, "s1": 99.0},
    }
    b = normalize_pivots_bundle(bundle, now_ms=now)
    assert b["ts_ms"] == 1700000000 * 1000
    assert b["hlc"]["high"] == 110.0
    assert b["pivots"]["pivot"] == 100.0


def test_normalize_pivots_bundle_invalid_json_strict_raises():
    try:
        normalize_pivots_bundle("{bad json", strict=True)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_normalize_pivots_bundle_non_dict_strict_raises():
    try:
        normalize_pivots_bundle(123, strict=True)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_parse_bool_extended():
    assert _parse_bool("true") is True
    assert _parse_bool(" yes ") is True
    assert _parse_bool("0") is False
    assert _parse_bool("off") is False
    assert _parse_bool("") is False


def test_compute_daily_pivots_happy_path():
    p = compute_daily_pivots({"high": 110, "low": 90, "close": 100})
    assert p["pivot"] == (110 + 90 + 100) / 3
    assert "r1" in p and "s1" in p


def test_compute_daily_pivots_invalid_input():
    assert compute_daily_pivots({"high": 0, "low": 90, "close": 100}) == {}
    assert compute_daily_pivots({"high": "bad", "low": 90, "close": 100}) == {}
    assert compute_daily_pivots(None) == {}
