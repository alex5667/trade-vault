import asyncio
import importlib
import json
import sys
import types


# -----------------------------------------------------------------------------
# Test-only dependency shims
# -----------------------------------------------------------------------------
# The unit test validates LiqMap injection logic and does not require external
# drivers (Postgres / Redis). In production those deps must be installed.


def _install_module_stub(mod_name: str, attrs: dict) -> None:
    m = types.ModuleType(mod_name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[mod_name] = m


# asyncpg (Postgres driver)
try:
    importlib.import_module("asyncpg")
except ModuleNotFoundError:
    class Pool:  # pragma: no cover
        pass

    async def create_pool(*args, **kwargs):  # pragma: no cover
        return Pool()

    _install_module_stub("asyncpg", {"Pool": Pool, "create_pool": create_pool})


# redis-py (sync + asyncio API)
try:
    importlib.import_module("redis")
    importlib.import_module("redis.asyncio")
    importlib.import_module("redis.exceptions")
except ModuleNotFoundError:
    class ConnectionPool:  # pragma: no cover
        def __init__(self, *args, **kwargs):
            pass

    class Redis:  # pragma: no cover
        def __init__(self, *args, **kwargs):
            pass

        def ping(self):
            return True

    class RedisError(Exception):  # pragma: no cover
        pass

    exc_mod = types.ModuleType("redis.exceptions")
    exc_mod.RedisError = RedisError
    exc_mod.ConnectionError = RedisError
    exc_mod.TimeoutError = RedisError
    exc_mod.BusyLoadingError = RedisError
    exc_mod.ResponseError = RedisError
    sys.modules["redis.exceptions"] = exc_mod

    redis_asyncio = types.ModuleType("redis.asyncio")
    redis_asyncio.Redis = Redis
    sys.modules["redis.asyncio"] = redis_asyncio

    redis_mod = types.ModuleType("redis")
    redis_mod.Redis = Redis
    redis_mod.ConnectionPool = ConnectionPool
    redis_mod.exceptions = exc_mod
    sys.modules["redis"] = redis_mod


# Optional internal trackers used by runtime; not required for this unit test.
for _mod, _cls in (
    ("core.rolling_vwap_tracker", "RollingVWAPTracker")
    ("core.rolling_momentum_tracker", "RollingMomentumTracker")
    ("core.rolling_volatility_tracker", "RollingVolatilityTracker")
):
    try:
        importlib.import_module(_mod)
    except ModuleNotFoundError:
        _install_module_stub(_mod, {_cls: type(_cls, (), {"__init__": lambda self, *a, **k: None})})


from services.orderflow.components.tick_processor import TickProcessor


class _DummyRuntime:
    def __init__(self, symbol: str):
        self.symbol = symbol


class _DummyRedis:
    def __init__(self, payload_by_key):
        self.payload_by_key = dict(payload_by_key)
        self.get_calls = []

    async def get(self, key: str):
        self.get_calls.append(key)
        return self.payload_by_key.get(key)


def _make_tp(redis: _DummyRedis, *, refresh_ms=1500, stale_ms=120000):
    # Bypass heavy __init__; only fields used by _inject_liqmap_features are set.
    tp = TickProcessor.__new__(TickProcessor)
    tp.redis = redis

    tp.liqmap_features_enable = True
    tp.liqmap_features_windows = ["1h"]
    tp.liqmap_features_refresh_ms = int(refresh_ms)
    tp.liqmap_features_failopen_stale_ms = int(stale_ms)

    tp.liqmap_snapshot_key_prefix = "liqmap:snapshot"
    tp.liqmap_near_band_bps = 20.0
    tp.liqmap_peak_min_share = 0.05

    tp._liqmap_cache = {}
    tp._liqmap_next_refresh_ts_ms = {}

    return tp


def test_liqmap_injection_updates_indicators_and_throttles_refresh():
    now_ms = 10_000
    snap = {
        "ts_ms": now_ms - 2_000
        "symbol": "BTCUSDT"
        "window": "1h"
        "levels": [
            {"price": 99.0, "long_usd": 100.0, "short_usd": 200.0}
            {"price": 100.0, "long_usd": 400.0, "short_usd": 100.0}
            {"price": 101.0, "long_usd": 50.0, "short_usd": 900.0}
        ]
    }

    key = "liqmap:snapshot:BTCUSDT:1h"
    r = _DummyRedis({key: json.dumps(snap)})
    tp = _make_tp(r, refresh_ms=1500)

    runtime = _DummyRuntime("BTCUSDT")
    indicators = {}

    asyncio.run(tp._inject_liqmap_features(runtime=runtime, now_ms=now_ms, price=100.0, indicators=indicators))

    # A couple of core keys should exist (full set is tested in core tests).
    assert indicators["liqmap_1h_levels_n"] == 3.0
    assert indicators["liqmap_1h_age_ms"] == 2000.0
    assert "liqmap_1h_total_usd" in indicators

    # Second call within refresh interval should not hit Redis again.
    indicators2 = {}
    asyncio.run(tp._inject_liqmap_features(runtime=runtime, now_ms=now_ms + 500, price=100.0, indicators=indicators2))
    assert len(r.get_calls) == 1
    assert "liqmap_1h_total_usd" in indicators2

    # After refresh interval passes, Redis should be called again.
    indicators3 = {}
    asyncio.run(tp._inject_liqmap_features(runtime=runtime, now_ms=now_ms + 2000, price=100.0, indicators=indicators3))
    assert len(r.get_calls) == 2


def test_liqmap_injection_failopen_reuses_last_good_when_snapshot_missing():
    base_ms = 20_000
    snap = {
        "ts_ms": base_ms - 2_000
        "symbol": "BTCUSDT"
        "window": "1h"
        "levels": [
            {"price": 100.0, "long_usd": 100.0, "short_usd": 100.0}
            {"price": 101.0, "long_usd": 200.0, "short_usd": 0.0}
        ]
    }

    key = "liqmap:snapshot:BTCUSDT:1h"
    r = _DummyRedis({key: json.dumps(snap)})
    # refresh_ms=1 forces refresh on every call
    tp = _make_tp(r, refresh_ms=1, stale_ms=120_000)
    runtime = _DummyRuntime("BTCUSDT")

    ind1 = {}
    asyncio.run(tp._inject_liqmap_features(runtime=runtime, now_ms=base_ms, price=100.0, indicators=ind1))
    assert "liqmap_1h_total_usd" in ind1
    assert ind1["liqmap_1h_age_ms"] == 2000.0

    # Now simulate missing snapshot (Redis returns None); we still expect last-good feats.
    r.payload_by_key[key] = None

    ind2 = {}
    asyncio.run(tp._inject_liqmap_features(runtime=runtime, now_ms=base_ms + 10_000, price=100.0, indicators=ind2))
    assert "liqmap_1h_total_usd" in ind2

    # Age must increase deterministically based on cached snap ts.
    assert ind2["liqmap_1h_age_ms"] == 12_000.0




