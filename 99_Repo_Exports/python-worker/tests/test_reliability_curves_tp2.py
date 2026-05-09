from __future__ import annotations

from typing import Any

import pytest

from services.reliability_curves import (
    make_reliability_key_v4,
    update_reliability_curve,
)
from tests.fake_redis import FakeRedis


def _pos_with_envelope(*, envelope: dict[str, Any]) -> dict[str, Any]:
    # PositionState.__dict__ shape used by StatsAggregator.update_stats
    return {
        "strategy": envelope.get("strategy", "TestStrategy"),
        "symbol": envelope.get("symbol", "BTCUSDT"),
        "tf": envelope.get("tf", "1m"),
        "signal_payload": envelope,
    }


def test_reliability_curves_tp2_writes_global_and_smt_ctx(monkeypatch: pytest.MonkeyPatch) -> None:
    r = FakeRedis()

    # Ensure writer selects TP2 as target
    monkeypatch.setenv("RELIABILITY_TARGETS", "tp2")
    monkeypatch.setenv("RELIABILITY_WRITE_LEGACY", "0")  # keep test focused on v4
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")
    monkeypatch.setenv("RELIABILITY_SMT_COH_THR", "0.65")

    envelope = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "kind": "absorption",
        "venue": "binance_futures",
        "entry_regime": "trending_bull",
        "ctx": {
            "confidence": 0.63,  # -> 63 -> bucket 65 with step=5 (round)
            "smt_leader_confirm": 1,
            "smt_coh": 0.71,
            "smt_leader_dir": "UP",
        },
    }
    pos = _pos_with_envelope(envelope=envelope)

    closed = {
        "strategy": "TestStrategy",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "direction": "LONG",
        "exit_ts_ms": 1_700_000_000_000,
        "tp2_hit": True,   # <- target
    }

    update_reliability_curve(r, closed=closed, pos=pos)

    # bucket for 0.63 with step=5:
    #   63/5=12.6 -> round=13 -> 65
    bucket = 65

    k_global = make_reliability_key_v4(
        target="tp2",
        strategy="TestStrategy",
        symbol="BTCUSDT",
        tf="1m",
        venue="binance_futures",
        kind="absorption",
        regime="trending_bull",
        ctx_key="na",
    )
    d = r.hgetall(k_global)
    assert int(d.get("samples") or 0) == 1
    assert int(d.get("hits") or 0) == 1
    assert int(d.get(f"n:{bucket}") or 0) == 1
    assert int(d.get(f"h:{bucket}") or 0) == 1

    # SMT ctx_key should be: smtc1_coh1_al1 (confirm=1, coh>=0.65, align UP==LONG)
    k_ctx = make_reliability_key_v4(
        target="tp2",
        strategy="TestStrategy",
        symbol="BTCUSDT",
        tf="1m",
        venue="binance_futures",
        kind="absorption",
        regime="trending_bull",
        ctx_key="smtc1_coh1_al1",
    )
    d2 = r.hgetall(k_ctx)
    assert int(d2.get("samples") or 0) == 1
    assert int(d2.get("hits") or 0) == 1
    assert int(d2.get(f"n:{bucket}") or 0) == 1
    assert int(d2.get(f"h:{bucket}") or 0) == 1


def test_reliability_curves_tp2_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    r = FakeRedis()
    monkeypatch.setenv("RELIABILITY_TARGETS", "tp2")
    monkeypatch.setenv("RELIABILITY_WRITE_LEGACY", "0")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "10")

    envelope = {"symbol": "ETHUSDT", "tf": "1m", "kind": "x", "venue": "mt5", "entry_regime": "range", "ctx": {"confidence": 0.21}}
    pos = _pos_with_envelope(envelope=envelope)
    closed = {"strategy": "S", "symbol": "ETHUSDT", "tf": "1m", "direction": "SHORT", "exit_ts_ms": 1_700_000_000_111, "tp2_hit": False}

    update_reliability_curve(r, closed=closed, pos=pos)

    # 0.21 => 21 -> step=10 -> round(2.1)=2 -> bucket 20
    bucket = 20
    k_global = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="ETHUSDT", tf="1m",
        venue="mt5", kind="x", regime="range", ctx_key="na",
    )
    d = r.hgetall(k_global)
    assert int(d.get("samples") or 0) == 1
    assert int(d.get("hits") or 0) == 0
    assert int(d.get(f"n:{bucket}") or 0) == 1
    assert int(d.get(f"h:{bucket}") or 0) == 0
