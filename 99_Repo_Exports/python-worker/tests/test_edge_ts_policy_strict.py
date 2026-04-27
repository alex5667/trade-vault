from utils.time_utils import get_ny_time_millis
import os
import time
from types import SimpleNamespace
import pytest
import math

from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

class RedisExplodes:
    def hgetall(self, *a, **k):
        raise AssertionError("EMA must NOT touch Redis in this scenario")

class RedisOk:
    def __init__(self, key, samples="50", ema="80"):
        self.key = key
        self.samples = samples
        self.ema = ema
        self.calls = 0
    def hgetall(self, k):
        self.calls += 1
        if k == self.key:
            return {"samples": self.samples, "ema_slip_bps": self.ema}
        return {}

def test_correct_skip_ema_allows_ema_when_ts_valid(monkeypatch):
    # IMPORTANT: correct_skip_ema should only skip EMA on invalid ts.
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_SECONDS_POLICY", "normalize")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "1")

    now = get_ny_time_millis()
    # session_from_ts_ms is used inside estimate; we don't need exact string for this test
    # We'll accept "calls>0" as proof EMA path is reachable.
    ctx = SimpleNamespace(ts_ms=now, bid=100.0, ask=100.1, tf="1m", venue="binance", kind="absorption")

    # We don't know exact session string here, so this test should instead verify:
    # - Redis was called at least once (EMA path enabled)
    # - returned slippage >= base
    # Use a Redis that returns empty always -> EMA None -> base returned, but Redis called.
    r = RedisOk(key="__no_match__")
    v = estimate_slippage_bps(ctx, redis_client=r, symbol="BTCUSDT", venue="binance", ts_ms=now, kind="absorption")
    assert r.calls > 0
    assert v >= 5.0

def test_invalid_ts_skips_ema_and_never_calls_redis(monkeypatch):
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    ctx = SimpleNamespace(ts_ms=0, bid=100.0, ask=101.0, tf="1m", venue="binance", kind="absorption")
    v = estimate_slippage_bps(ctx, redis_client=RedisExplodes(), symbol="BTCUSDT", venue="binance", ts_ms=0, kind="absorption")
    assert getattr(ctx, "_ts_invalid", False) is True
    assert v > 40.0  # spread/2 dominates

def test_seconds_input_skip_ema_policy(monkeypatch):
    # If seconds_policy=skip_ema -> treat seconds input as invalid => skip EMA and don't call Redis.
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "allow")           # allow EMA in general
    monkeypatch.setenv("EDGE_TS_SECONDS_POLICY", "skip_ema")    # BUT for seconds input, disable EMA
    ctx = SimpleNamespace(ts=1700000000, bid=100.0, ask=100.2, tf="1m", venue="binance", kind="absorption")
    v = estimate_slippage_bps(ctx, redis_client=RedisExplodes(), symbol="BTCUSDT", venue="binance", ts_ms=ctx.ts, kind="absorption")
    assert getattr(ctx, "_ts_seconds_input", False) is True
    assert getattr(ctx, "_ts_invalid", False) is True
    assert v >= 5.0

def test_conservative_policy_applies_floor_on_bad_ts(monkeypatch):
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "conservative")
    monkeypatch.setenv("EDGE_TS_BAD_FLOOR_BPS", "80")
    ctx = SimpleNamespace(ts_ms=0, bid=100.0, ask=100.02, tf="1m", venue="binance")
    v = estimate_slippage_bps(ctx, redis_client=None, symbol="BTCUSDT", venue="binance", ts_ms=0, kind="na", default_bps=5.0)
    assert v >= 80.0

def test_skew_exceeds_max_skew_marks_ts_invalid_and_skips_ema(monkeypatch):
    # Hard regression guard:
    # if ts looks like epoch-ms BUT is сильно "в прошлом/будущем" -> считаем невалидным,
    # не используем EMA и НЕ трогаем Redis.
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_SECONDS_POLICY", "normalize")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "60000")  # 1 minute

    now = get_ny_time_millis()
    bad_ts = now - 48 * 3600 * 1000  # 48h назад -> сильно больше max_skew
    ctx = SimpleNamespace(ts_ms=bad_ts, bid=100.0, ask=100.2, tf="1m", venue="binance", kind="absorption")

    v = estimate_slippage_bps(ctx, redis_client=RedisExplodes(), symbol="BTCUSDT", venue="binance", ts_ms=bad_ts, kind="absorption")
    assert getattr(ctx, "_ts_invalid", False) is True
    assert "skew_ms=" in str(getattr(ctx, "_ts_reason", ""))
    # Base должен быть >= spread/2 (спред=0.2, mid≈100.1 => ~19.98bps /2 => ~9.99bps)
    assert v >= 5.0

def test_veto_policy_forces_huge_slippage(monkeypatch):
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "veto")
    monkeypatch.setenv("EDGE_TS_BAD_VETO_BPS", "999999")
    ctx = SimpleNamespace(ts_ms=0, bid=100.0, ask=100.02, tf="1m", venue="binance")
    v = estimate_slippage_bps(ctx, redis_client=None, symbol="BTCUSDT", venue="binance", ts_ms=0, kind="na", default_bps=5.0)
    assert v >= 999999.0
