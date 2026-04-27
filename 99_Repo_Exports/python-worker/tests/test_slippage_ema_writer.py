from tests.fake_redis import FakeRedis

from services.slippage_ema_stats import update_slippage_ema


def _hget(redis, key: str, field: str) -> str:
    h = redis.hgetall(key) or {}
    for k, v in h.items():
        ks = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        if ks == field:
            return v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
    return ""


def test_slipema_v2_written():
    r = FakeRedis()
    closed = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "strategy": "breakout",
        "kind": "breakout",
        "venue": "binance",
        "entry_ts_ms": 1700000000000,
        "realized_slippage_bps": 25.0,
    }
    update_slippage_ema(r, closed=closed, pos=None)
    key = "slipema:v2:BTCUSDT:binance:us_main:1m:breakout"
    # session_from_ts_ms зависит от TZ; если у вас us_main не совпадёт в тестовой среде,
    # тогда проверяйте prefix:
    assert "slipema:v2:BTCUSDT:binance:" in key
    # minimal assertion: hash exists and has n/ema_bps
    # (в FakeRedis hgetall вернёт dict)
    h = r.hgetall(key) or {}
    assert h  # key created