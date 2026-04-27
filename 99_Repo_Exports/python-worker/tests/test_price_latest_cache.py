from __future__ import annotations

from tests.fake_redis import FakeRedis
from services.price_latest_cache import write_price_latest


def _hget_str(d, k: str) -> str:
    # FakeRedis может возвращать bytes; поддержим оба варианта.
    if k in d:
        v = d[k]
    elif k.encode() in d:
        v = d[k.encode()]
    else:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return str(v)


def test_write_price_latest_writes_mid_and_ts_ms():
    r = FakeRedis()
    write_price_latest(
        r,
        symbol="BTCUSDT",
        ts_ms=1_700_000_000_000,
        bid=100.0,
        ask=101.0,
        last=100.5,
        mid=None,
        venue="mt5",
    )
    d = r.hgetall("price:latest:BTCUSDT") or {}
    assert d
    assert _hget_str(d, "venue") == "mt5"
    assert int(float(_hget_str(d, "ts_ms"))) == 1_700_000_000_000
    mid = float(_hget_str(d, "mid"))
    assert 100.4 < mid < 100.6  # fallback to (bid+ask)/2
    sp = float(_hget_str(d, "spread_bps") or "0")
    assert sp > 0.0


def test_write_price_latest_normalizes_seconds_to_ms():
    r = FakeRedis()
    # seconds epoch
    write_price_latest(
        r,
        symbol="ETHUSDT",
        ts_ms=1_700_000_000,  # seconds
        bid=50.0,
        ask=50.1,
        last=50.05,
        mid=None,
        venue="mt5",
    )
    d = r.hgetall("price:latest:ETHUSDT") or {}
    assert d
    assert int(float(_hget_str(d, "ts_ms"))) == 1_700_000_000_000


def test_write_price_latest_skips_invalid_ts():
    r = FakeRedis()
    write_price_latest(
        r,
        symbol="SOLUSDT",
        ts_ms=0,  # invalid
        bid=20.0,
        ask=20.1,
        last=20.05,
        mid=None,
        venue="mt5",
    )
    d = r.hgetall("price:latest:SOLUSDT") or {}
    assert not d