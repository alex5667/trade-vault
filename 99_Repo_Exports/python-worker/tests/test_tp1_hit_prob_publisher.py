"""Tests for orderflow_services/tp1_hit_prob_publisher_v1.py — pure functions."""

from __future__ import annotations

import json
from typing import Any

import pytest

from orderflow_services.tp1_hit_prob_publisher_v1 import (
    Cfg,
    evaluate_window,
    publish_state,
)


def _cfg(**overrides: Any) -> Cfg:
    base = dict(
        enable=True,
        interval_sec=900,
        window_h=168.0,
        min_samples=10,
        grid=[0.5, 1.0, 1.5],
        include_virtual=True,
        hmac_secret="",
        prom_port=9999,
        stream="trades:closed",
        redis_url="redis://localhost:0",
        state_key="autocal:tp1_phit:state",
    )
    base.update(overrides)
    return Cfg(**base)


def _trade(symbol: str, kind: str, regime: str, direction: str, mfe_r: float) -> dict:
    return {
        "symbol": symbol, "kind": kind, "regime": regime,
        "direction": direction, "mfe_r": mfe_r, "is_virtual": False,
    }


class _CapturingRedis:
    def __init__(self) -> None:
        self.last: dict[str, Any] | None = None
        self.ex: int | None = None

    def set(self, key: str, value: str, *, ex: int | None = None) -> None:
        self.last = {"key": key, "value": value}
        self.ex = ex


def test_evaluate_window_produces_buckets_with_curves() -> None:
    # 50 trades with mfe_r descending → curve must be calibrated.
    trades = [
        _trade("BTCUSDT", "of", "range", "LONG", v * 2.0 / 50)
        for v in range(50)
    ]
    recs = evaluate_window(trades, _cfg(min_samples=20))
    bk = "BTCUSDT|of|range|LONG"
    assert bk in recs
    assert recs[bk]["passes"] == 1
    assert recs[bk]["n_total"] == 50


def test_publish_state_serialises_and_signs() -> None:
    cap = _CapturingRedis()
    recs = {"*|*|*|*": {"n_total": 100, "curve": {"1.00": 0.5},
                       "calibration_ok": 1, "passes": 1}}
    cfg = _cfg(hmac_secret="my-secret")
    ok = publish_state(cap, recs, cfg, n_trades=500)
    assert ok is True
    assert cap.last is not None
    payload = json.loads(cap.last["value"])
    assert payload["n_trades"] == 500
    assert "sig" in payload
    assert payload["buckets"]["*|*|*|*"]["curve"]["1.00"] == pytest.approx(0.5)
    # TTL is 4× interval
    assert cap.ex == cfg.interval_sec * 4


def test_publish_state_no_sig_when_no_secret() -> None:
    cap = _CapturingRedis()
    publish_state(cap, {}, _cfg(), n_trades=0)
    payload = json.loads(cap.last["value"])
    assert "sig" not in payload
