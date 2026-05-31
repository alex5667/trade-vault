from __future__ import annotations

"""
EdgeCostGate timestamp policy tests.

Tests that verify:
  - invalid ts (0) => EMA skipped, base slippage returned
  - skewed ts => EMA skipped in default profile
  - skewed ts + hard profile => veto slippage returned
"""
import time
from types import SimpleNamespace

import fakeredis; FakeRedis = fakeredis.FakeRedis

from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps
from utils.time_utils import get_ny_time_millis


def _hset(r: FakeRedis, key: str, mapping: dict) -> None:
    """Write hash to FakeRedis - uses the new hset(key, mapping=...) API."""
    r.hset(key, mapping=mapping)


def test_estimate_slippage_bps_invalid_ts_skips_ema(monkeypatch):
    """
    ts=0 (invalid) => EMA must be skipped, return base slippage only.
    Key follows slipema:{SYM}:{venue}:{session}:{tf}:{kind} format (NOT v2 prefix).
    """
    r = FakeRedis()
    # Write EMA at the actual key format used by the gate
    _hset(r, "slipema:BTCUSDT:binance:na:1m:breakout", {"samples": "100", "ema_slippage_bps": "30"})

    ctx = SimpleNamespace(
        ts_ms=0,          # invalid
        spread_bps=4.0,   # half-spread=2.0
        tf="1m",
        kind="breakout",
        strategy="breakout",
    )

    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")

    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance",
        ts_ms=ctx.ts_ms,
        kind="breakout",
        tf="1m",
        default_bps=5.0,
        use_spread_half=True,
    )

    # base=max(default=5, halfspread=2)=5; EMA at 30 must be ignored (invalid ts)
    assert float(v) == 5.0, f"Expected 5.0, got {v}"


def test_estimate_slippage_bps_skewed_ts_skips_ema_default(monkeypatch):
    """
    ts skewed by 3y vs now_ms => skip EMA in default profile.
    """
    r = FakeRedis()
    _hset(r, "slipema:BTCUSDT:binance:us_main:1m:breakout", {"samples": "100", "ema_slippage_bps": "80"})

    now_ms = 1_700_000_000_000
    monkeypatch.setattr(time, "time", lambda: now_ms / 1000.0)

    ctx = SimpleNamespace(
        ts_ms=1_600_000_000_000,  # ~3y skew -> must skip EMA
        spread_bps=10.0,          # halfspread=5
        tf="1m",
        kind="breakout",
        strategy="breakout",
        session="us_main",
    )

    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")  # 6h threshold
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")

    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance",
        ts_ms=ctx.ts_ms,
        kind="breakout",
        tf="1m",
        default_bps=5.0,
        use_spread_half=True,
    )
    # base=max(5, 10/2=5)=5; EMA at 80 ignored (skew too large)
    assert float(v) == 5.0, f"Expected 5.0, got {v}"


def test_estimate_slippage_bps_skewed_ts_hard_profile_returns_veto_floor(monkeypatch):
    """
    Skewed ts + EDGE_TS_BAD_POLICY=veto => returns huge veto_bps floor.
    """
    r = FakeRedis()
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(time, "time", lambda: now_ms / 1000.0)

    ctx = SimpleNamespace(ts_ms=1_600_000_000_000, spread_bps=2.0, tf="1m", kind="k", strategy="k")

    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "veto")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")
    monkeypatch.setenv("EDGE_TS_BAD_VETO_BPS", "999999")

    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance",
        ts_ms=ctx.ts_ms,
        kind="k",
        tf="1m",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert float(v) >= 999999.0, f"Expected >= 999999.0, got {v}"


def test_estimate_slippage_bps_valid_ts_uses_ema(monkeypatch):
    """
    Valid ts (close to now) => EMA should be used (returns max(base, ema)).
    """
    r = FakeRedis()
    from domain.time_utils import session_from_ts_ms
    now_ms = get_ny_time_millis()
    sess = str(session_from_ts_ms(now_ms)).lower()

    # Write EMA at the correct key
    _hset(r, f"slipema:BTCUSDT:binance:{sess}:1m:breakout",
          {"samples": "100", "ema_slippage_bps": "40"})

    ctx = SimpleNamespace(spread_bps=0.0, tf="1m", kind="breakout")

    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "20")

    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance",
        ts_ms=now_ms,     # valid ts
        kind="breakout",
        tf="1m",
        default_bps=5.0,
        use_spread_half=False,
    )
    # EMA=40 > default=5 => should return 40
    assert float(v) == 40.0, f"Expected 40.0, got {v}"
