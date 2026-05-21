"""Tests for ConfirmationBarrierCalibrator and reader wiring."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core.confirmation_barrier_calibrator import (
    ABSORPTION_DEFAULT,
    BREAKOUT_DEFAULT,
    ConfirmationBarrierCalibrator,
    _quantile,
)


# ── _quantile helper ──────────────────────────────────────────────────────────

def test_quantile_empty():
    assert _quantile([], 0.8) == 0.0


def test_quantile_single():
    assert _quantile([1.5], 0.8) == 1.5


def test_quantile_sorted():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _quantile(xs, 0.0) == pytest.approx(1.0)
    assert _quantile(xs, 1.0) == pytest.approx(5.0)
    assert _quantile(xs, 0.5) == pytest.approx(3.0)


def test_quantile_interpolated():
    xs = [1.0, 2.0]
    # q=0.25 → между 1.0 и 2.0, ближе к 1.0
    v = _quantile(xs, 0.25)
    assert 1.0 <= v <= 2.0


# ── warmup guard ─────────────────────────────────────────────────────────────

def test_warmup_returns_none_then_commits():
    cal = ConfirmationBarrierCalibrator(min_samples=5, quantile=0.8)
    ts = 1_000_000
    for i in range(4):
        cal.observe("BTCUSDT", "breakout", 1.3 + i * 0.1, ts + i * 1000)
    # 4 samples < min_samples=5 → shadow_threshold is None
    result = cal.shadow_threshold_for("BTCUSDT", "breakout", ts + 5000)
    assert result is None

    cal.observe("BTCUSDT", "breakout", 1.7, ts + 5000)
    # now 5 samples → committed
    result = cal.shadow_threshold_for("BTCUSDT", "breakout", ts + 6000)
    assert result is not None
    assert 1.3 <= result <= 2.0


# ── hardcoded defaults when enforce=False ─────────────────────────────────────

def test_threshold_returns_default_when_not_enforce():
    cal = ConfirmationBarrierCalibrator(min_samples=1, quantile=0.8)
    ts = 1_000_000
    for i in range(10):
        cal.observe("BTCUSDT", "breakout", 1.5, ts + i * 1000)

    # enforce=False → always default
    t = cal.threshold_for("BTCUSDT", "breakout", ts + 20000, enforce=False)
    assert t == BREAKOUT_DEFAULT


def test_threshold_returns_calibrated_when_enforce():
    cal = ConfirmationBarrierCalibrator(min_samples=5, quantile=0.8)
    ts = 1_000_000
    for i in range(20):
        cal.observe("SOLUSDT", "breakout", 1.8, ts + i * 1000)

    t = cal.threshold_for("SOLUSDT", "breakout", ts + 30000, enforce=True)
    # q80 of 20 identical samples = 1.8
    assert t == pytest.approx(1.8, abs=0.05)


# ── per-symbol adaptation ─────────────────────────────────────────────────────

def test_sol_vs_btc_different_thresholds():
    """SOL имеет более высокий средний OBI → более высокий threshold."""
    cal = ConfirmationBarrierCalibrator(min_samples=10, quantile=0.8)
    ts = 1_000_000
    btc_obi = 1.2  # BTC — низкий фоновый OBI
    sol_obi = 1.6  # SOL — высокий фоновый OBI

    for i in range(20):
        cal.observe("BTCUSDT", "breakout", btc_obi, ts + i * 1000)
        cal.observe("SOLUSDT", "breakout", sol_obi, ts + i * 1000)

    t_btc = cal.threshold_for("BTCUSDT", "breakout", ts + 30000, enforce=True)
    t_sol = cal.threshold_for("SOLUSDT", "breakout", ts + 30000, enforce=True)
    assert t_sol > t_btc, f"SOL threshold ({t_sol}) must exceed BTC ({t_btc})"


# ── absorption vs breakout separate bins ─────────────────────────────────────

def test_breakout_absorption_separate_bins():
    cal = ConfirmationBarrierCalibrator(min_samples=5, quantile=0.8)
    ts = 1_000_000
    for i in range(10):
        cal.observe("ETHUSDT", "breakout", 1.25, ts + i * 1000)
        cal.observe("ETHUSDT", "absorption", 1.60, ts + i * 1000)

    t_br = cal.threshold_for("ETHUSDT", "breakout", ts + 20000, enforce=True)
    t_ab = cal.threshold_for("ETHUSDT", "absorption", ts + 20000, enforce=True)
    assert t_ab > t_br, "absorption threshold must exceed breakout for same data"


# ── hierarchical fallback ─────────────────────────────────────────────────────

def test_fallback_to_kind_default_when_cold():
    cal = ConfirmationBarrierCalibrator(min_samples=100)
    ts = 1_000_000
    # ни одного наблюдения → fallback к kind default
    t = cal.threshold_for("XRPUSDT", "breakout", ts, enforce=True)
    assert t == BREAKOUT_DEFAULT


def test_fallback_absorption_default():
    cal = ConfirmationBarrierCalibrator(min_samples=100)
    t = cal.threshold_for("XRPUSDT", "absorption", 1_000_000, enforce=True)
    assert t == ABSORPTION_DEFAULT


# ── OBI floor / ceil clipping ─────────────────────────────────────────────────

def test_obi_floor_clipping():
    cal = ConfirmationBarrierCalibrator(min_samples=1, obi_floor=1.01, quantile=0.5)
    # observe значения ниже floor
    cal.observe("BTCUSDT", "breakout", 0.5, 1_000_000)
    cal.observe("BTCUSDT", "breakout", 0.0, 1_001_000)  # 0 = должен отбрасываться
    cal.observe("BTCUSDT", "breakout", float("nan"), 1_002_000)
    # floor-clipped: 0.5 → 1.01; 0.0 → отброшен (not finite / <= 0)
    t = cal.shadow_threshold_for("BTCUSDT", "breakout", 1_003_000)
    if t is not None:
        assert t >= 1.01


def test_obi_ceil_clipping():
    cal = ConfirmationBarrierCalibrator(min_samples=1, obi_ceil=3.0, quantile=0.9)
    for i in range(5):
        cal.observe("BTCUSDT", "breakout", 99.0, 1_000_000 + i * 1000)  # clipped to 3.0
    t = cal.threshold_for("BTCUSDT", "breakout", 1_006_000, enforce=True)
    assert t <= 3.0


# ── time-window expiry ────────────────────────────────────────────────────────

def test_old_samples_expire_sticky():
    """Committed threshold остаётся (sticky) даже когда все samples истекли.
    Реверт к hardcoded происходит только при явном сбросе или перезапуске без snapshot.
    Новые данные перекроют committed_tau через гистерезис."""
    window_ms = 10_000  # 10 секунд
    cal = ConfirmationBarrierCalibrator(min_samples=3, window_ms=window_ms, quantile=0.8)
    base_ts = 1_000_000

    for i in range(10):
        cal.observe("BTCUSDT", "breakout", 1.5, base_ts + i * 100)

    t1 = cal.threshold_for("BTCUSDT", "breakout", base_ts + 9000, enforce=True)
    assert t1 is not None  # warmed up

    # 11 секунд спустя — samples истекли, но committed_tau sticky
    t2 = cal.threshold_for("BTCUSDT", "breakout", base_ts + 11_000, enforce=True)
    assert t2 == t1  # sticky — не возвращается к BREAKOUT_DEFAULT

    # Cold bin (никогда не калиброванный) → hardcoded default
    t_cold = cal.threshold_for("XRPUSDT", "breakout", base_ts + 11_000, enforce=True)
    assert t_cold == BREAKOUT_DEFAULT


# ── hysteresis ────────────────────────────────────────────────────────────────

def test_hysteresis_prevents_small_updates():
    cal = ConfirmationBarrierCalibrator(min_samples=5, quantile=0.8, hysteresis=0.05, max_jump=1.0)
    ts = 1_000_000
    for i in range(10):
        cal.observe("BTCUSDT", "breakout", 1.4, ts + i * 1000)
    t1 = cal.threshold_for("BTCUSDT", "breakout", ts + 20000, enforce=True)
    assert t1 is not None

    # добавляем samples чуть выше → delta < hysteresis → нет обновления
    for i in range(5):
        cal.observe("BTCUSDT", "breakout", 1.41, ts + 20000 + i * 1000)
    t2 = cal.threshold_for("BTCUSDT", "breakout", ts + 30000, enforce=True)
    assert t2 == pytest.approx(t1, abs=0.001)


# ── snapshot / restore ────────────────────────────────────────────────────────

def test_snapshot_restore_committed_tau():
    cal = ConfirmationBarrierCalibrator(min_samples=5, quantile=0.8)
    ts = 1_000_000
    for i in range(20):
        cal.observe("SOLUSDT", "absorption", 1.7, ts + i * 1000)
    t_before = cal.threshold_for("SOLUSDT", "absorption", ts + 30000, enforce=True)
    assert t_before is not None

    snap = cal.snapshot()

    cal2 = ConfirmationBarrierCalibrator(min_samples=5)
    cal2.load_state(snap)

    # После restore — threshold совпадает (committed_tau восстановлен)
    t_after = cal2.threshold_for("SOLUSDT", "absorption", ts + 30000, enforce=True)
    # Sample buffer не восстанавливается → холодный warmup; committed_tau есть
    assert t_after == pytest.approx(t_before, abs=0.001)


def test_snapshot_schema_version():
    cal = ConfirmationBarrierCalibrator()
    snap = cal.snapshot()
    assert snap["schema_version"] == 1


def test_load_state_invalid_ignored():
    cal = ConfirmationBarrierCalibrator()
    cal.load_state(None)  # type: ignore
    cal.load_state({})
    cal.load_state({"bins": {"bad_key_no_colon": {"committed_tau": 1.5}}})
    # не должно бросать


# ── reader wiring ─────────────────────────────────────────────────────────────

def test_reader_returns_default_when_enforce_false():
    from core.confirmation_barrier_reader import ConfirmationBarrierReader
    reader = ConfirmationBarrierReader(None, enforce=False)
    assert reader.threshold_for("BTCUSDT", "breakout") == BREAKOUT_DEFAULT
    assert reader.threshold_for("SOLUSDT", "absorption") == ABSORPTION_DEFAULT


def test_reader_returns_calibrated_when_enforce():
    from core.confirmation_barrier_reader import ConfirmationBarrierReader
    mock_redis = MagicMock()
    import json
    snapshot = {
        "schema_version": 1,
        "bins": {
            "SOLUSDT:breakout": {"committed_tau": 1.55, "last_apply_ms": 1000},
        },
    }
    mock_redis.get.return_value = json.dumps(snapshot).encode()
    reader = ConfirmationBarrierReader(mock_redis, enforce=True, cache_ttl_sec=0.0)
    t = reader.threshold_for("SOLUSDT", "breakout")
    assert t == pytest.approx(1.55)


def test_reader_fallback_to_global_bin():
    from core.confirmation_barrier_reader import ConfirmationBarrierReader
    mock_redis = MagicMock()
    import json
    snapshot = {
        "schema_version": 1,
        "bins": {
            "*:breakout": {"committed_tau": 1.30, "last_apply_ms": 1000},
        },
    }
    mock_redis.get.return_value = json.dumps(snapshot).encode()
    reader = ConfirmationBarrierReader(mock_redis, enforce=True, cache_ttl_sec=0.0)
    # XRPUSDT не в bins → fallback к *:breakout
    t = reader.threshold_for("XRPUSDT", "breakout")
    assert t == pytest.approx(1.30)


def test_reader_fallback_to_hardcoded_when_no_cache():
    from core.confirmation_barrier_reader import ConfirmationBarrierReader
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    reader = ConfirmationBarrierReader(mock_redis, enforce=True, cache_ttl_sec=0.0)
    assert reader.threshold_for("BTCUSDT", "breakout") == BREAKOUT_DEFAULT


# ── L2ConfirmBreakout integration ─────────────────────────────────────────────

def _make_confirm_cfg(breakout_min=1.15, absorption_min=1.20):
    from handlers.crypto_orderflow.core.crypto_orderflow_confirmations import L2ConfirmCfg
    return L2ConfirmCfg(breakout_imbalance_min=breakout_min, absorption_imbalance_min=absorption_min)


def _make_l2_snapshot(bid=1000.0, ask=800.0):
    """Реальный L2Snapshot с одним уровнем bid и ask."""
    from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot
    return L2Snapshot(
        bids=[L2Level(price=100.0, size=bid / 100.0, notional=bid)],
        asks=[L2Level(price=100.1, size=ask / 100.1, notional=ask)],
    )


def _make_ctx(snap, symbol="BTCUSDT", ts=1_000_000):
    ctx = MagicMock()
    ctx.l2_snapshot = snap
    # Обнуляем все ts-атрибуты, чтобы int(MagicMock()) != 1 не вызывал stale_l2
    ctx.l2_ts_ms = None
    ctx.orderbook_ts_ms = None
    ctx.book_ts_ms = None
    ctx.ts = ts
    ctx.ts_ms = ts
    ctx.symbol = symbol
    ctx.wall_ask = False
    ctx.wall_bid = False
    return ctx


def test_calibrator_observes_on_ok():
    """observe() вызывается с правильным symbol и OBI когда check() → ok=True."""
    from handlers.crypto_orderflow.core.crypto_orderflow_confirmations import L2ConfirmBreakout
    cfg = _make_confirm_cfg(breakout_min=1.10)
    cal = ConfirmationBarrierCalibrator(min_samples=1)
    snap = _make_l2_snapshot(bid=1200.0, ask=1000.0)  # imbalance = 1.2 > 1.10
    ctx = _make_ctx(snap, symbol="BTCUSDT")

    confirmer = L2ConfirmBreakout(
        cfg=cfg,
        get_snapshot=L2ConfirmBreakout.default_get_snapshot,
        get_snapshot_ts_ms=L2ConfirmBreakout.default_get_snapshot_ts_ms,
        calibrator=cal,
    )
    ok, details = confirmer.check(ctx, dir_up=True)
    assert ok is True
    counts = cal.sample_counts()
    assert counts.get(("BTCUSDT", "breakout"), 0) == 1


def test_calibrator_not_observed_on_fail():
    """observe() НЕ вызывается когда check() → ok=False (imbalance_low)."""
    from handlers.crypto_orderflow.core.crypto_orderflow_confirmations import L2ConfirmBreakout
    cfg = _make_confirm_cfg(breakout_min=2.0)  # высокий порог → провал
    cal = ConfirmationBarrierCalibrator(min_samples=1)
    snap = _make_l2_snapshot(bid=1200.0, ask=1000.0)  # imbalance = 1.2 < 2.0
    ctx = _make_ctx(snap, symbol="BTCUSDT")

    confirmer = L2ConfirmBreakout(
        cfg=cfg,
        get_snapshot=L2ConfirmBreakout.default_get_snapshot,
        get_snapshot_ts_ms=L2ConfirmBreakout.default_get_snapshot_ts_ms,
        calibrator=cal,
    )
    ok, details = confirmer.check(ctx, dir_up=True)
    assert ok is False
    counts = cal.sample_counts()
    assert counts.get(("BTCUSDT", "breakout"), 0) == 0


def test_reader_overrides_threshold_in_check():
    """reader меняет imb_min → сигнал НЕ проходит когда calibrated threshold выше hardcoded."""
    import json

    from core.confirmation_barrier_reader import ConfirmationBarrierReader
    from handlers.crypto_orderflow.core.crypto_orderflow_confirmations import L2ConfirmBreakout

    cfg = _make_confirm_cfg(breakout_min=1.10)  # hardcoded: 1.10
    mock_redis = MagicMock()
    snapshot = {"schema_version": 1, "bins": {"BTCUSDT:breakout": {"committed_tau": 1.50}}}
    mock_redis.get.return_value = json.dumps(snapshot).encode()
    reader = ConfirmationBarrierReader(mock_redis, enforce=True, cache_ttl_sec=0.0)

    # imbalance = 1.2 → проходит при 1.10, НЕ проходит при 1.50
    snap = _make_l2_snapshot(bid=1200.0, ask=1000.0)
    ctx = _make_ctx(snap, symbol="BTCUSDT")

    confirmer = L2ConfirmBreakout(
        cfg=cfg,
        get_snapshot=L2ConfirmBreakout.default_get_snapshot,
        get_snapshot_ts_ms=L2ConfirmBreakout.default_get_snapshot_ts_ms,
        reader=reader,
    )
    ok, details = confirmer.check(ctx, dir_up=True)
    assert ok is False
    assert details["imbalance_min"] == pytest.approx(1.50)
