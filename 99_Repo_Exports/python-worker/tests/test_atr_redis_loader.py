import os
from types import SimpleNamespace

from orderflow.base_handler_legacy import load_tracker_atr_from_redis_hmget


class _FakeRedis:
    def __init__(self, atr: str | None, ts: str | None, *, raise_exc: bool = False):
        self._atr = atr
        self._ts = ts
        self._raise = raise_exc

    def hmget(self, key, *fields):
        if self._raise:
            raise RuntimeError("boom")
        return (self._atr, self._ts)


def _tf_to_ms(tf: str) -> int:
    tf = (tf or "").lower()
    if tf == "1m":
        return 60_000
    return 60_000


def test_load_tracker_atr_returns_value_and_ts(monkeypatch):
    monkeypatch.setenv("ATR_REDIS_STALENESS_MULT", "3")
    r = _FakeRedis("1.25", "1700000000000")
    log = SimpleNamespace(warning=lambda *a, **k: None)

    v, ts, logged = load_tracker_atr_from_redis_hmget(
        redis_client=r,
        key="ATR:BTCUSDT:1m",
        timeframe="1m",
        current_ts=1700000000000,
        timeframe_to_ms_fn=_tf_to_ms,
        logger=log,
        warning_logged=False,
    )
    assert v == 1.25
    assert ts == 1700000000000
    assert logged is False


def test_load_tracker_atr_filters_stale(monkeypatch):
    monkeypatch.setenv("ATR_REDIS_STALENESS_MULT", "1")  # max_age = 60s
    r = _FakeRedis("1.25", "1700000000000")
    log = SimpleNamespace(warning=lambda *a, **k: None)

    v, ts, logged = load_tracker_atr_from_redis_hmget(
        redis_client=r,
        key="ATR:BTCUSDT:1m",
        timeframe="1m",
        current_ts=1700000200000,  # +200s -> stale
        timeframe_to_ms_fn=_tf_to_ms,
        logger=log,
        warning_logged=False,
    )
    assert v is None
    assert ts is None


def test_load_tracker_atr_warn_once_on_error():
    r = _FakeRedis(None, None, raise_exc=True)
    warned = {"n": 0}
    log = SimpleNamespace(warning=lambda *a, **k: warned.__setitem__("n", warned["n"] + 1))

    v, ts, logged = load_tracker_atr_from_redis_hmget(
        redis_client=r,
        key="ATR:BTCUSDT:1m",
        timeframe="1m",
        current_ts=1700000000000,
        timeframe_to_ms_fn=_tf_to_ms,
        logger=log,
        warning_logged=False,
    )
    assert v is None and ts is None
    assert logged is True
    assert warned["n"] == 1
