from types import SimpleNamespace

from handlers.crypto_orderflow.utils.quality_gates import DataQualityGate
from utils.time_utils import get_ny_time_millis


def test_data_quality_veto_atr_stale(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_ATR_STALE_MAX_MS", "1000")
    monkeypatch.setenv("DATA_STRICT_MISSING_ATR_TS", "1")
    monkeypatch.setenv("DATA_REQUIRE_EPOCH_TS", "0")
    monkeypatch.setenv("DATA_QUARANTINE_VETO", "0")

    g = DataQualityGate.from_env()
    now = get_ny_time_millis()

    ctx = SimpleNamespace(
        ts_event_ms=now,
        of=SimpleNamespace(atr_ts_ms=now - 10_000),
    )
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is True
    assert dec.reason_code == "VETO_ATR_STALE"
