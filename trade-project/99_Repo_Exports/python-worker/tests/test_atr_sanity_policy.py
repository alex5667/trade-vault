
from core.atr_sanity_policy import sanitize_atr
from utils.time_utils import get_ny_time_millis


def test_sanitize_atr_stale_falls_back_to_runtime_last():
    now = get_ny_time_millis()
    cfg = {"atr_sanity_max_age_ms": 60_000}
    dec, ind = sanitize_atr(
        atr=10.0,
        entry=100_000.0,
        atr_meta={"age_ms": 120_000},
        atr_tf="1m",
        runtime_last_atr=25.0,
        runtime_last_atr_ts_ms=now - 10_000,
        now_ms=now,
        cfg=cfg,
    )
    assert dec.ok is True
    assert dec.used_fallback == 1
    assert dec.atr == 25.0
    assert ind["atr_sanity_stale"] == 1

def test_sanitize_atr_invalid_uses_pct_fallback_when_no_runtime():
    now = get_ny_time_millis()
    cfg = {"atr_sanity_fallback_pct": 0.0003}
    dec, ind = sanitize_atr(
        atr=0.0,
        entry=100_000.0,
        atr_meta={},
        atr_tf="1m",
        runtime_last_atr=0.0,
        runtime_last_atr_ts_ms=0,
        now_ms=now,
        cfg=cfg,
    )
    assert dec.ok is True
    assert abs(dec.atr - 30.0) < 1e-6
