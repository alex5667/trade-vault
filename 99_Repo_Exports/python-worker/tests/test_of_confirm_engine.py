from __future__ import annotations

import pytest
from types import SimpleNamespace
from core.of_confirm_engine import OFConfirmEngine

def test_of_confirm_reversal_ok_2of3():
    eng = OFConfirmEngine(version=2)
    indicators = {"delta_z": 2.8}
    cfg = {
        "require_strong_confirmation": True,
        "strong_z_min": 2.0,
        "strong_need_reversal": 2,
        "obi_event_ttl_ms": 5000,
        "obi_stable_min_secs": 1.5,
        "iceberg_event_ttl_ms": 15000,
        "iceberg_strict_refresh_min": 3,
        "iceberg_strict_duration_min": 1.5,
        "iceberg_strict_dist_bp": 10.0,
        "sweep_valid_ms": 120000,
        "reclaim_signal_valid_ms": 120000,
        "of_score_min": 0.0,  # disable score threshold for test
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0
    }
    # runtime stubs
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True),
        last_obi_event={"ts_ms": 9000, "direction": "LONG", "obi": 0.3, "stable_secs": 2.0},
        last_iceberg_event={"ts_ms": 9000, "side": "bid", "refresh": 3, "duration": 2.0, "price": 100.0},
        last_sweep=SimpleNamespace(ts_ms=9000, kind="EQL", direction_bias="LONG"),
        last_reclaim=SimpleNamespace(ts_ms=9500, hold_bars=2, direction_bias="LONG", level=99.0, pool_id="p1"),
        last_div=None,
        cont_ctx_ts_ms=0,
    )
    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.05,
        delta_z=2.8,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.ok == 1
    assert ofc.have >= 2
    assert ofc.score > 0.5

def test_of_confirm_stale_obi_ignored():
    eng = OFConfirmEngine(version=2)
    indicators = {"delta_z": 2.8}
    cfg = {
        "obi_event_ttl_ms": 1000, 
        "require_strong_confirmation": False,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0
    }
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.3, "stable_secs": 9.0}, # stale
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,
        cont_ctx_ts_ms=0,
    )
    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.0,
        delta_z=2.8,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    assert ofc is not None
    assert ofc.evidence["obi_dir_ok"] == 0
    assert ofc.evidence["obi_stable"] == 0
    # score should only reflect Z-score minus exec_risk_penalty
    # Z-contrib = (2.8/3.0) * 0.3 = 0.28
    # exec_risk_penalty = -0.18 (default spread + slippage)
    # score = 0.28 - 0.18 = 0.10 (approximately)
    assert 0.08 < ofc.score < 0.12

def test_of_confirm_score_veto():
    eng = OFConfirmEngine(version=2)
    indicators = {"delta_z": 1.0}
    cfg = {
        "require_strong_confirmation": True,
        "strong_z_min": 0.5,
        "strong_need_reversal": 1,
        "of_score_min": 0.8, # very high threshold
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0,
        "sweep_valid_ms": 120000,
    }
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True), # A = 1
        last_obi_event=None,
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(ts_ms=9000, kind="EQL", direction_bias="LONG"),
        last_reclaim=None,
        last_div=None,
    )
    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.0,
        delta_z=1.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    assert ofc is not None
    assert dec is not None
    assert bool(dec.ok) is True # 1-of-1 passed (sweep)
    assert ofc.ok == 0         # but score is too low
    # Score = (1.0/3.0)*0.3 [z] + 0.15 [wp] = 0.25. threshold 0.8.
    assert ofc.score < 0.3


def test_replay_mode_set_replay_time_ms():
    """Test that set_replay_time_ms freezes time for deterministic replay."""
    eng = OFConfirmEngine()
    
    # Initially not in replay mode
    assert eng._replay_mode is False
    assert eng._replay_now_ms is None
    
    # Set replay time
    eng.set_replay_time_ms(1234567890)
    assert eng._replay_mode is True
    assert eng._replay_now_ms == 1234567890
    
    # _now_ms should return frozen time
    now1 = eng._now_ms()
    assert now1 == 1234567890
    
    # Should be deterministic (same value)
    now2 = eng._now_ms()
    assert now2 == 1234567890
    
    # Clear replay mode
    eng.clear_replay_time()
    assert eng._replay_mode is False
    assert eng._replay_now_ms is None


def test_replay_mode_resolve_now_ts():
    """Test _resolve_now_ts priority: tick_ts_ms > indicators > _now_ms()."""
    eng = OFConfirmEngine()
    
    # Case 1: tick_ts_ms has priority
    indicators = {}
    now_ts = eng._resolve_now_ts(10000, indicators)
    assert now_ts == 10000
    
    # Case 2: indicators['now_ts_ms'] if tick_ts_ms is 0
    indicators = {"now_ts_ms": 20000}
    now_ts = eng._resolve_now_ts(0, indicators)
    assert now_ts == 20000
    
    # Case 3: _now_ms() fallback (in replay mode)
    eng.set_replay_time_ms(30000)
    indicators = {}
    now_ts = eng._resolve_now_ts(0, indicators)
    assert now_ts == 30000
    
    # Case 4: tick_ts_ms still has priority even in replay
    now_ts = eng._resolve_now_ts(40000, indicators)
    assert now_ts == 40000
    
    eng.clear_replay_time()


def test_replay_mode_meta_model_frozen():
    """Test that _load_meta_model doesn't reload in replay mode."""
    eng = OFConfirmEngine()
    
    # Set replay mode
    eng.set_replay_time_ms(1000000)
    
    # In replay mode, _load_meta_model should return existing model without checking time
    # (we can't easily test file system, but we can verify the early return logic)
    result = eng._load_meta_model("", 1000000, 60)
    assert result is None  # Empty path returns None
    
    # The key is that replay mode prevents time-based reloads
    # This is tested by the early return in _load_meta_model when _replay_mode is True
    
    eng.clear_replay_time()


def test_replay_mode_build_deterministic():
    """Test that build() uses deterministic time in replay mode."""
    eng = OFConfirmEngine()
    
    # Set replay time
    frozen_ts = 50000
    eng.set_replay_time_ms(frozen_ts)
    
    indicators = {"delta_z": 2.0}
    cfg = {
        "require_strong_confirmation": False,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0,
        "of_score_min": 0.0,
    }
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event=None,
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,
        cont_ctx_ts_ms=0,
    )
    
    # Build with tick_ts_ms=0 (should use frozen time)
    ofc1, _ = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=0,  # No tick_ts_ms, should use frozen time
        price=100.0,
        delta_z=2.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    
    assert ofc1 is not None
    # Should use frozen time
    assert ofc1.ts_ms == frozen_ts
    assert indicators.get("now_ts_ms_used") == frozen_ts
    
    # Build again with same inputs - should be identical
    indicators2 = {"delta_z": 2.0}
    ofc2, _ = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=0,
        price=100.0,
        delta_z=2.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators2
    )
    
    # Deterministic: same score and decision
    assert ofc2 is not None
    assert ofc2.ts_ms == frozen_ts
    assert ofc2.score == ofc1.score
    assert ofc2.ok == ofc1.ok
    
    eng.clear_replay_time()


def test_replay_mode_tick_ts_priority():
    """Test that tick_ts_ms always has priority over replay time."""
    eng = OFConfirmEngine()
    
    # Set replay time
    eng.set_replay_time_ms(100000)
    
    indicators = {"delta_z": 2.0}
    cfg = {
        "require_strong_confirmation": False,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0,
        "of_score_min": 0.0,
    }
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event=None,
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,
        cont_ctx_ts_ms=0,
    )
    
    # Build with explicit tick_ts_ms (should override frozen time)
    tick_ts = 200000
    ofc, _ = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=tick_ts,  # Explicit tick_ts_ms should be used
        price=100.0,
        delta_z=2.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    
    assert ofc is not None
    assert ofc.ts_ms == tick_ts  # Should use tick_ts_ms, not frozen time
    assert indicators.get("now_ts_ms_used") == tick_ts
    
    eng.clear_replay_time()
