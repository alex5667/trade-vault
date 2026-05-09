from ml_analysis.tools.replay_inputs_archiver import _pick_ts_ms, _utc_day_from_ts_ms


def test_utc_day_from_ts_ms_epoch_boundaries():
    assert _utc_day_from_ts_ms(0) == "1970-01-01"
    assert _utc_day_from_ts_ms(24 * 3600 * 1000) == "1970-01-02"


def test_pick_ts_ms_prefers_close_ts_ms():
    p = {"sid": "x", "close": {"close_ts_ms": 1234567890000}, "ts_ms": 1}
    assert _pick_ts_ms(p) == 1234567890000


def test_pick_ts_ms_fallback_ts_ms():
    p = {"sid": "x", "ts_ms": 999}
    assert _pick_ts_ms(p) >= 999
