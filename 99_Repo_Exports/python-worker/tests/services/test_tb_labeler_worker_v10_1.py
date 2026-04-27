import os
import pytest
from unittest.mock import MagicMock, patch
from services.tb_labeler_worker_v10_1 import (
    _pick_price, sample_ticks, TBLabelerWorker
)
from core.tb_labeling import (
    infer_tp_sl_bps, barrier_stats, exec_cost_r, Barriers
)

def test_infer_tp_sl_bps_stop() -> None:
    ind = {"stop_bps": 50.0}
    b = infer_tp_sl_bps(ind, tp_k_atr=1.0, sl_k_atr=1.0, fallback_tp_bps=30, fallback_sl_bps=30)
    assert b.tp_bps == 50.0
    assert b.scale_bps == 50.0

def test_infer_tp_sl_bps_atr() -> None:
    ind = {"atr_bps": 40.0}
    b = infer_tp_sl_bps(ind, tp_k_atr=2.0, sl_k_atr=1.5, fallback_tp_bps=30, fallback_sl_bps=30)
    assert b.tp_bps == 80.0
    assert b.sl_bps == 60.0
    assert b.scale_bps == 40.0

def test_pick_price() -> None:
    assert _pick_price({"mid": 10.5}) == 10.5
    assert _pick_price({"price": 10.6}) == 10.6
    assert _pick_price({"bid": 10, "ask": 11}) == 10.5

def test_sample_ticks() -> None:
    path = [(1000, 1.0), (2000, 1.1), (3000, 1.2), (4000, 1.3), (5000, 1.4)]
    res = sample_ticks(path, every=2, max_n=10)
    assert res is not None
    assert len(res) == 3  # [1000, 3000, 5000]
    assert res[1] == [3000.0, 1.2]

def test_exec_cost_r() -> None:
    ind = {"spread_bps": 2.0, "expected_slippage_bps": 3.0}
    cost = exec_cost_r(ind, 10.0)
    assert cost == 0.5

def test_barrier_stats_tp_hit_long() -> None:
    path = [(1000, 100.0), (1100, 100.2), (1200, 100.55)]
    b = Barriers(tp_bps=50.0, sl_bps=30.0, scale_bps=50.0)
    result = barrier_stats(ts0=1000, direction="LONG", entry_px=100.0, path=path, b=b, h_ms=5000, adv_max=1.2)
    assert result["label"] == "TP"
    assert result["hit_ms"] == 1200
    assert result["ret_bps"] >= 50.0
    assert result["y_edge"] == 1

def test_barrier_stats_sl_hit_short() -> None:
    path = [(2000, 100.0), (2100, 100.1), (2200, 100.35)]
    b = Barriers(tp_bps=50.0, sl_bps=30.0, scale_bps=30.0)
    result = barrier_stats(ts0=2000, direction="SHORT", entry_px=100.0, path=path, b=b, h_ms=5000, adv_max=1.2)
    assert result["label"] == "SL"
    assert result["hit_ms"] == 2200
    assert result["y_edge"] == 0

def test_barrier_stats_timeout() -> None:
    path = [(1000, 100.0), (1100, 100.05), (1200, 100.08)]
    b = Barriers(tp_bps=50.0, sl_bps=30.0, scale_bps=50.0)
    result = barrier_stats(ts0=1000, direction="LONG", entry_px=100.0, path=path, b=b, h_ms=5000, adv_max=1.2)
    assert result["label"] == "TIMEOUT"
    assert result["hit_ms"] == 6000

def test_barrier_stats_no_ticks() -> None:
    b = Barriers(tp_bps=50.0, sl_bps=30.0, scale_bps=50.0)
    result = barrier_stats(ts0=1000, direction="LONG", entry_px=100.0, path=[], b=b, h_ms=5000, adv_max=1.2)
    assert result["label"] == "NO_TICKS"
    assert result["hit_ms"] == 6000
