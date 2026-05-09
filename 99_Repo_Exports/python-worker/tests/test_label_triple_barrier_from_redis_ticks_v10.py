# python-worker/tests/test_label_triple_barrier_from_redis_ticks_v10.py
from __future__ import annotations

from typing import Any

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tools.label_triple_barrier_from_redis_ticks_v10 import (
    Barriers,
    infer_tp_sl_bps,
    label_one,
)


def test_infer_tp_sl_bps_stop_first() -> None:
    ind = {"stop_bps": 50.0, "atr_bps": 100.0}
    b = infer_tp_sl_bps(ind, tp_k_atr=1.0, sl_k_atr=1.0, fallback_tp_bps=30.0, fallback_sl_bps=30.0)
    assert isinstance(b, Barriers)
    assert b.tp_bps == 50.0
    assert b.sl_bps == 50.0
    assert b.scale_bps == 50.0


def test_infer_tp_sl_bps_atr_fallback() -> None:
    ind = {"stop_bps": 0.0, "atr_bps": 80.0}
    b = infer_tp_sl_bps(ind, tp_k_atr=1.5, sl_k_atr=1.0, fallback_tp_bps=30.0, fallback_sl_bps=30.0)
    assert b.tp_bps == 120.0
    assert b.sl_bps == 80.0
    assert b.scale_bps == 80.0


def test_label_tp_hit_long() -> None:
    # entry at 100.0; TP=+50bps => 100.50
    inp: dict[str, Any] = {
        "symbol": "BTCUSDT",
        "ts_ms": 1000,
        "direction": "LONG",
        "indicators": {"stop_bps": 50.0},
    }
    series: list[tuple[int, float]] = [
        (1000, 100.0),
        (1100, 100.2),
        (1200, 100.55),  # TP
        (1300, 100.1),
    ]
    out = label_one(inp, series, h_ms=5000, tp_k_atr=1.0, sl_k_atr=1.0, fallback_tp_bps=30.0, fallback_sl_bps=30.0)
    assert out["tb_label"] == "TP"
    assert out["tb_y_edge"] == 1
    assert out["tb_t_hit_ms"] == 1200
    assert out["tb_ret_bps"] > 50.0


def test_label_sl_hit_short() -> None:
    # SHORT: profit when price goes down.
    # SL=+30bps adverse move => price up by 0.30%
    inp: dict[str, Any] = {
        "symbol": "ETHUSDT",
        "ts_ms": 2000,
        "direction": "SHORT",
        "indicators": {"atr_bps": 30.0},
    }
    series: list[tuple[int, float]] = [
        (2000, 100.0),
        (2100, 100.1),
        (2200, 100.35),  # adverse for SHORT, should hit SL
    ]
    out = label_one(inp, series, h_ms=5000, tp_k_atr=1.0, sl_k_atr=1.0, fallback_tp_bps=30.0, fallback_sl_bps=30.0)
    assert out["tb_label"] == "SL"
    assert out["tb_y_edge"] == 0


def test_label_no_ticks() -> None:
    inp: dict[str, Any] = {"symbol": "BTCUSDT", "ts_ms": 1000, "direction": "LONG", "indicators": {"stop_bps": 20.0}}
    out = label_one(inp, [], h_ms=1000, tp_k_atr=1.0, sl_k_atr=1.0, fallback_tp_bps=30.0, fallback_sl_bps=30.0)
    assert out["tb_label"] == "NO_TICKS"
    assert out["tb_y_edge"] == 0

