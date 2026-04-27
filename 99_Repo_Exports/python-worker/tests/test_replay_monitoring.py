import pytest
import time
from backtest_hooks import ReplayConfig, replay, ReplayTimeStats

def test_replay_monotonic_ok():
    ticks = [
        {"ts": 1000, "bid": 100, "ask": 101},
        {"ts": 1010, "bid": 100, "ask": 101},
        {"ts": 1020, "bid": 100, "ask": 101},
    ]
    cfg = ReplayConfig(speed=0)
    # Mocking iter_ticks_from_parquet/timescale is tricky because of yields
    # Let's monkeypatch replay's src
    import backtest_hooks
    orig_iter = backtest_hooks.iter_ticks_from_parquet
    backtest_hooks.iter_ticks_from_parquet = lambda c: iter(ticks)
    try:
        cfg.parquet_path = "mock.parquet"
        res = replay(cfg)
        assert len(res) == 3
    finally:
        backtest_hooks.iter_ticks_from_parquet = orig_iter

def test_replay_reorder_fail_fast():
    ticks = [
        {"ts": 1000, "bid": 100, "ask": 101},
        {"ts": 999, "bid": 100, "ask": 101}, # -1ms reorder
    ]
    cfg = ReplayConfig(speed=0)
    import backtest_hooks
    orig_iter = backtest_hooks.iter_ticks_from_parquet
    backtest_hooks.iter_ticks_from_parquet = lambda c: iter(ticks)
    try:
        cfg.parquet_path = "mock.parquet"
        with pytest.raises(ValueError, match="strict monotonicity violation"):
            replay(cfg)
    finally:
        backtest_hooks.iter_ticks_from_parquet = orig_iter

def test_replay_gap_warn(caplog):
    ticks = [
        {"ts": 1000, "bid": 100, "ask": 101},
        {"ts": 4000, "bid": 100, "ask": 101}, # 3s gap, > 2s
    ]
    cfg = ReplayConfig(speed=0, gap_warn_ms=2000)
    import backtest_hooks
    orig_iter = backtest_hooks.iter_ticks_from_parquet
    backtest_hooks.iter_ticks_from_parquet = lambda c: iter(ticks)
    try:
        cfg.parquet_path = "mock.parquet"
        replay(cfg)
        assert "ts gap dt=3000ms" in caplog.text
    finally:
        backtest_hooks.iter_ticks_from_parquet = orig_iter

def test_replay_gap_severe_log(caplog):
    ticks = [
        {"ts": 1000, "bid": 100, "ask": 101},
        {"ts": 12000, "bid": 100, "ask": 101}, # 11s gap, > 10s
    ]
    cfg = ReplayConfig(speed=0, gap_severe_ms=10000, gap_severe_policy="log")
    import backtest_hooks
    orig_iter = backtest_hooks.iter_ticks_from_parquet
    backtest_hooks.iter_ticks_from_parquet = lambda c: iter(ticks)
    try:
        cfg.parquet_path = "mock.parquet"
        replay(cfg)
        assert "SEVERE ts gap" in caplog.text
    finally:
        backtest_hooks.iter_ticks_from_parquet = orig_iter

def test_replay_gap_severe_raise():
    ticks = [
        {"ts": 1000, "bid": 100, "ask": 101},
        {"ts": 12000, "bid": 100, "ask": 101}, # 11s gap, > 10s
    ]
    cfg = ReplayConfig(speed=0, gap_severe_ms=10000, gap_severe_policy="raise")
    import backtest_hooks
    orig_iter = backtest_hooks.iter_ticks_from_parquet
    backtest_hooks.iter_ticks_from_parquet = lambda c: iter(ticks)
    try:
        cfg.parquet_path = "mock.parquet"
        with pytest.raises(ValueError, match="SEVERE ts gap"):
            replay(cfg)
    finally:
        backtest_hooks.iter_ticks_from_parquet = orig_iter

def test_replay_invalid_ts():
    ticks = [{"ts": 0}]
    cfg = ReplayConfig(speed=0)
    import backtest_hooks
    orig_iter = backtest_hooks.iter_ticks_from_parquet
    backtest_hooks.iter_ticks_from_parquet = lambda c: iter(ticks)
    try:
        cfg.parquet_path = "mock.parquet"
        with pytest.raises(ValueError, match="invalid ts"):
            replay(cfg)
    finally:
        backtest_hooks.iter_ticks_from_parquet = orig_iter

def test_parquet_normalization_scales():
    import pandas as pd
    import numpy as np
    from backtest_hooks import ReplayConfig, iter_ticks_from_parquet
    import backtest_hooks

    # Helper to test scale
    def run_norm_test(data_dict):
        df = pd.DataFrame(data_dict)
        orig_read = pd.read_parquet
        pd.read_parquet = lambda p: df
        try:
            cfg = ReplayConfig(parquet_path="dummy.parquet")
            return list(iter_ticks_from_parquet(cfg))
        finally:
            pd.read_parquet = orig_read

    # 1. ns scale
    ticks = run_norm_test({
        "ts": [1704067200000000000, 1704067200010000000],
        "bid": [100.0, 100.1], "ask": [100.2, 100.3]
    })
    assert ticks[0]["ts"] == 1704067200000
    assert ticks[1]["ts"] == 1704067200010

    # 2. us scale
    ticks = run_norm_test({
        "ts": [1704067200000000, 1704067200010000],
        "bid": [100.0, 100.1], "ask": [100.2, 100.3]
    })
    assert ticks[0]["ts"] == 1704067200000
    assert ticks[1]["ts"] == 1704067200010

    # 3. ms scale
    ticks = run_norm_test({
        "ts": [1704067200000, 1704067200010],
        "bid": [100.0, 100.1], "ask": [100.2, 100.3]
    })
    assert ticks[0]["ts"] == 1704067200000
    assert ticks[1]["ts"] == 1704067200010

    # 4. datetime64[ns] scale
    ticks = run_norm_test({
        "ts": [pd.Timestamp("2024-01-01 00:00:00"), pd.Timestamp("2024-01-01 00:00:00.010")],
        "bid": [100.0, 100.1], "ask": [100.2, 100.3]
    })
    assert ticks[0]["ts"] == 1704067200000
    assert ticks[1]["ts"] == 1704067200010
