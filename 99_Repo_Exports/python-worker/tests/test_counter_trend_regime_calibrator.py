"""Unit tests for counter_trend_regime_calibrator_v1 + runtime overrides reader.

Tests pure functions only (no Redis connection); covers:
  - aggregate_buckets: grouping + virtual filter
  - evaluate_buckets: passes/block + dwell tracking + enforce
  - build_block_lists: enforce filter + direction split
  - parse_trade: required fields + alias normalization
  - HMAC sign roundtrip
  - Reader: TTL cache, HMAC verify, enforce filter, fail-open default
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from orderflow_services.counter_trend_regime_calibrator_v1 import (
    Cfg,
    _hmac_sign,
    _parse_trade_for_ct,
    aggregate_buckets,
    build_block_lists,
    evaluate_buckets,
)
from services.counter_trend_runtime_overrides import (
    CounterTrendReader,
    reset_reader_for_tests,
)


def _cfg(**overrides: Any) -> Cfg:
    defaults: dict[str, Any] = dict(
        enable=True,
        enforce=True,
        interval_sec=900,
        window_h=168.0,
        min_samples=10,
        block_avg_r_max=-0.5,
        dwell_h=24.0,
        include_virtual=True,
        hmac_secret="",
        prom_port=9870,
        stream="trades:closed",
        redis_url="redis://localhost",
    )
    defaults.update(overrides)
    return Cfg(**defaults)


# ────────────────────────────────────────────────────────────────────────────
# parse_trade_for_ct
# ────────────────────────────────────────────────────────────────────────────

def test_parse_trade_basic():
    p = _parse_trade_for_ct({
        "direction": "SHORT",
        "entry_regime": "trending_bull",
        "r_multiple": "-1.1",
        "is_virtual": "0",
    })
    assert p == {
        "direction": "SHORT",
        "regime": "trending_bull",
        "r_multiple": -1.1,
        "is_virtual": False,
    }


def test_parse_trade_alias_uptrend():
    """uptrend → trending_bull via alias map."""
    p = _parse_trade_for_ct({
        "direction": "buy",
        "regime": "UPTREND",
        "r_multiple": "0.5",
    })
    assert p["direction"] == "LONG"
    assert p["regime"] == "trending_bull"


def test_parse_trade_missing_r_multiple():
    p = _parse_trade_for_ct({
        "direction": "SHORT",
        "entry_regime": "trending_bull",
    })
    assert p is None


def test_parse_trade_na_regime_dropped():
    p = _parse_trade_for_ct({
        "direction": "SHORT",
        "entry_regime": "na",
        "r_multiple": "-0.5",
    })
    assert p is None


def test_parse_trade_invalid_direction():
    p = _parse_trade_for_ct({
        "direction": "garbage",
        "entry_regime": "range",
        "r_multiple": "0.0",
    })
    assert p is None


# ────────────────────────────────────────────────────────────────────────────
# aggregate_buckets
# ────────────────────────────────────────────────────────────────────────────

def test_aggregate_buckets_basic():
    trades = [
        {"direction": "SHORT", "regime": "trending_bull", "r_multiple": -1.0, "is_virtual": False},
        {"direction": "SHORT", "regime": "trending_bull", "r_multiple": -0.5, "is_virtual": False},
        {"direction": "SHORT", "regime": "trending_bull", "r_multiple":  0.3, "is_virtual": False},
        {"direction": "LONG",  "regime": "trending_bear", "r_multiple": -1.2, "is_virtual": False},
    ]
    b = aggregate_buckets(trades, include_virtual=True)
    assert "SHORT|trending_bull" in b
    bb = b["SHORT|trending_bull"]
    assert bb["n_total"] == 3
    assert bb["n_winners"] == 1  # only +0.3
    assert bb["avg_r"] == round((-1.0 - 0.5 + 0.3) / 3, 4)
    assert bb["win_rate"] == round(1 / 3, 4)
    assert b["LONG|trending_bear"]["n_total"] == 1


def test_aggregate_buckets_virtual_filter():
    trades = [
        {"direction": "SHORT", "regime": "trending_bull", "r_multiple": -1.0, "is_virtual": True},
        {"direction": "SHORT", "regime": "trending_bull", "r_multiple": -0.5, "is_virtual": False},
    ]
    b_all = aggregate_buckets(trades, include_virtual=True)
    b_real = aggregate_buckets(trades, include_virtual=False)
    assert b_all["SHORT|trending_bull"]["n_total"] == 2
    assert b_real["SHORT|trending_bull"]["n_total"] == 1


# ────────────────────────────────────────────────────────────────────────────
# evaluate_buckets — block + dwell + enforce
# ────────────────────────────────────────────────────────────────────────────

def test_evaluate_bucket_block_passes():
    """avg_r ≤ block_avg_r_max AND n_total ≥ min_samples → passes=1, block=1."""
    cfg = _cfg(min_samples=10, block_avg_r_max=-0.5)
    raw = {"SHORT|trending_bull": {"n_total": 24, "n_winners": 8, "sum_r": -13.4, "avg_r": -0.56, "win_rate": 0.33}}
    out = evaluate_buckets(raw, cfg, prev_buckets={}, now_ms=1_000_000_000)
    bucket = out["SHORT|trending_bull"]
    assert bucket["passes"] == 1
    assert bucket["block"] == 1


def test_evaluate_bucket_below_min_samples_no_block():
    cfg = _cfg(min_samples=30, block_avg_r_max=-0.5)
    raw = {"SHORT|trending_bull": {"n_total": 5, "n_winners": 1, "sum_r": -5.0, "avg_r": -1.0, "win_rate": 0.2}}
    out = evaluate_buckets(raw, cfg, prev_buckets={}, now_ms=1_000_000_000)
    assert out["SHORT|trending_bull"]["passes"] == 0
    assert out["SHORT|trending_bull"]["block"] == 0


def test_evaluate_bucket_avg_r_above_threshold_no_block():
    """avg_r > block_avg_r_max → не блок."""
    cfg = _cfg(min_samples=10, block_avg_r_max=-0.5)
    raw = {"SHORT|range": {"n_total": 20, "n_winners": 12, "sum_r": -2.0, "avg_r": -0.1, "win_rate": 0.6}}
    out = evaluate_buckets(raw, cfg, prev_buckets={}, now_ms=1_000_000_000)
    assert out["SHORT|range"]["passes"] == 0


def test_evaluate_bucket_dwell_accumulates():
    cfg = _cfg(min_samples=10, block_avg_r_max=-0.5, dwell_h=24.0, enforce=True)
    raw = {"SHORT|trending_bull": {"n_total": 24, "n_winners": 8, "sum_r": -13.4, "avg_r": -0.56, "win_rate": 0.33}}
    now = 1_000_000_000
    out1 = evaluate_buckets(raw, cfg, prev_buckets={}, now_ms=now)
    assert out1["SHORT|trending_bull"]["dwell_h"] == 0.0  # no prev pass
    assert out1["SHORT|trending_bull"]["enforce"] == 0  # dwell<24h
    # Second cycle 30 min later — passes again
    prev_buckets = {"SHORT|trending_bull": out1["SHORT|trending_bull"]}
    now2 = now + 30 * 60 * 1000
    out2 = evaluate_buckets(raw, cfg, prev_buckets, now_ms=now2)
    # delta_h = 0.5, cap_h = (900/3600)*2 = 0.5 → new_dwell = 0 + 0.5 = 0.5
    assert out2["SHORT|trending_bull"]["dwell_h"] == 0.5


def test_evaluate_bucket_enforce_after_dwell():
    """passes + dwell_h>=cfg.dwell_h + cfg.enforce → enforce=1."""
    cfg = _cfg(min_samples=10, block_avg_r_max=-0.5, dwell_h=24.0, enforce=True)
    raw = {"SHORT|trending_bull": {"n_total": 24, "n_winners": 8, "sum_r": -13.4, "avg_r": -0.56, "win_rate": 0.33}}
    # Симулируем уже накопленный dwell
    prev_buckets = {"SHORT|trending_bull": {"dwell_h": 25.0, "last_pass_ms": 999_000_000}}
    out = evaluate_buckets(raw, cfg, prev_buckets, now_ms=999_000_000 + 30 * 60 * 1000)
    assert out["SHORT|trending_bull"]["enforce"] == 1


def test_evaluate_bucket_dwell_resets_on_fail():
    cfg = _cfg(min_samples=10, block_avg_r_max=-0.5, dwell_h=24.0, enforce=True)
    # Bucket был passing, теперь avg_r поднялся
    raw = {"SHORT|range": {"n_total": 20, "n_winners": 12, "sum_r": -2.0, "avg_r": -0.1, "win_rate": 0.6}}
    prev_buckets = {"SHORT|range": {"dwell_h": 25.0, "last_pass_ms": 999_000_000}}
    out = evaluate_buckets(raw, cfg, prev_buckets, now_ms=999_000_000 + 60 * 60 * 1000)
    assert out["SHORT|range"]["dwell_h"] == 0.0
    assert out["SHORT|range"]["enforce"] == 0


# ────────────────────────────────────────────────────────────────────────────
# build_block_lists
# ────────────────────────────────────────────────────────────────────────────

def test_build_block_lists_filters_enforce():
    eval_b = {
        "SHORT|trending_bull": {"block": 1, "enforce": 1},
        "SHORT|expansion":    {"block": 1, "enforce": 0},  # shadow — skipped
        "LONG|trending_bear": {"block": 1, "enforce": 1},
        "SHORT|range":        {"block": 0, "enforce": 0},
    }
    short, long_ = build_block_lists(eval_b, require_enforce=True)
    assert short == ["trending_bull"]
    assert long_ == ["trending_bear"]


def test_build_block_lists_no_enforce_filter():
    eval_b = {
        "SHORT|trending_bull": {"block": 1, "enforce": 1},
        "SHORT|expansion":    {"block": 1, "enforce": 0},
        "LONG|trending_bear": {"block": 1, "enforce": 1},
    }
    short, long_ = build_block_lists(eval_b, require_enforce=False)
    assert sorted(short) == ["expansion", "trending_bull"]
    assert long_ == ["trending_bear"]


# ────────────────────────────────────────────────────────────────────────────
# HMAC sign
# ────────────────────────────────────────────────────────────────────────────

def test_hmac_sign_deterministic():
    payload = {"ts_ms": 12345, "buckets": {}, "short_block_regimes": []}
    sig1 = _hmac_sign(payload, "secret")
    sig2 = _hmac_sign(payload, "secret")
    assert sig1 == sig2
    assert sig1 != _hmac_sign(payload, "other_secret")


# ────────────────────────────────────────────────────────────────────────────
# CounterTrendReader
# ────────────────────────────────────────────────────────────────────────────

def _make_state(
    *,
    short_block: list[str],
    long_block: list[str],
    ts_ms: int | None = None,
    secret: str = "",
    buckets: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
        "window_hours": 168,
        "n_trades": 100,
        "min_samples": 30,
        "block_avg_r_max": -0.5,
        "dwell_h_required": 24.0,
        "buckets": buckets or {},
        "short_block_regimes": short_block,
        "long_block_regimes": long_block,
    }
    if secret:
        payload["sig"] = _hmac_sign(payload, secret)
    return json.dumps(payload)


def test_reader_returns_default_when_disabled():
    """Reader без свежего snapshot → default_set."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    reader = CounterTrendReader(mock_redis, refresh_ms=1, stale_ms=1000)
    default = frozenset({"trending_bull"})
    out = reader.get_block_regimes(direction="SHORT", default_set=default)
    assert out == default


def test_reader_returns_calibrator_block_list():
    mock_redis = MagicMock()
    mock_redis.get.return_value = _make_state(
        short_block=["trending_bull", "expansion"],
        long_block=["trending_bear"],
    )
    reader = CounterTrendReader(mock_redis, refresh_ms=1, stale_ms=10 * 60 * 1000)
    out = reader.get_block_regimes(direction="SHORT", default_set=frozenset())
    assert out == frozenset({"trending_bull", "expansion"})


def test_reader_falls_back_on_stale_snapshot():
    mock_redis = MagicMock()
    # ts_ms 10 часов назад, stale_ms 1 час → stale → fail-open
    stale_ts = int(time.time() * 1000) - 10 * 60 * 60 * 1000
    mock_redis.get.return_value = _make_state(
        short_block=["trending_bull"], long_block=[], ts_ms=stale_ts,
    )
    reader = CounterTrendReader(mock_redis, refresh_ms=1, stale_ms=60 * 60 * 1000)
    default = frozenset({"trending_bull_default"})
    out = reader.get_block_regimes(direction="SHORT", default_set=default)
    assert out == default


def test_reader_hmac_mismatch_ignored():
    mock_redis = MagicMock()
    mock_redis.get.return_value = _make_state(
        short_block=["trending_bull"], long_block=[], secret="wrong_secret",
    )
    reader = CounterTrendReader(
        mock_redis, refresh_ms=1, stale_ms=10 * 60 * 1000, hmac_secret="real_secret",
    )
    default = frozenset({"default_only"})
    out = reader.get_block_regimes(direction="SHORT", default_set=default)
    # HMAC mismatch → ignore snapshot → fail-open to default
    assert out == default


def test_reader_hmac_valid_accepted():
    mock_redis = MagicMock()
    mock_redis.get.return_value = _make_state(
        short_block=["trending_bull"], long_block=[], secret="match_secret",
    )
    reader = CounterTrendReader(
        mock_redis, refresh_ms=1, stale_ms=10 * 60 * 1000, hmac_secret="match_secret",
    )
    out = reader.get_block_regimes(direction="SHORT", default_set=frozenset())
    assert out == frozenset({"trending_bull"})


def test_reader_long_direction():
    mock_redis = MagicMock()
    mock_redis.get.return_value = _make_state(
        short_block=["trending_bull"], long_block=["trending_bear"],
    )
    reader = CounterTrendReader(mock_redis, refresh_ms=1, stale_ms=10 * 60 * 1000)
    out = reader.get_block_regimes(direction="LONG", default_set=frozenset())
    assert out == frozenset({"trending_bear"})


def test_reader_empty_block_list_returns_default():
    """Если calibrator пуст (пока warmup) — fallback на ENV defaults."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = _make_state(short_block=[], long_block=[])
    reader = CounterTrendReader(mock_redis, refresh_ms=1, stale_ms=10 * 60 * 1000)
    default = frozenset({"env_default"})
    out = reader.get_block_regimes(direction="SHORT", default_set=default)
    assert out == default


def test_reader_no_enforce_uses_block_flag():
    """require_enforce=False — берёт из buckets все block=1."""
    mock_redis = MagicMock()
    mock_redis.get.return_value = _make_state(
        short_block=[],  # пусто (enforce-only filter)
        long_block=[],
        buckets={
            "SHORT|trending_bull": {"block": 1, "enforce": 0, "n_total": 24, "avg_r": -0.56},
            "LONG|trending_bear":  {"block": 1, "enforce": 0, "n_total": 11, "avg_r": -0.95},
        },
    )
    reader = CounterTrendReader(mock_redis, refresh_ms=1, stale_ms=10 * 60 * 1000)
    short = reader.get_block_regimes(
        direction="SHORT", default_set=frozenset(), require_enforce=False,
    )
    long_ = reader.get_block_regimes(
        direction="LONG", default_set=frozenset(), require_enforce=False,
    )
    assert short == frozenset({"trending_bull"})
    assert long_ == frozenset({"trending_bear"})


def test_module_singleton_disabled_by_default(monkeypatch):
    """get_reader() returns None when AUTOCAL_COUNTER_TREND_READ_ENABLED=0."""
    reset_reader_for_tests()
    monkeypatch.delenv("AUTOCAL_COUNTER_TREND_READ_ENABLED", raising=False)
    from services.counter_trend_runtime_overrides import get_reader, get_block_regimes
    assert get_reader() is None
    default = frozenset({"x"})
    assert get_block_regimes(direction="SHORT", default_set=default) == default
