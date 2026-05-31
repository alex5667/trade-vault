"""Integration smoke test for gate_value_reporter using fakeredis.

Writes synthetic entries to the three input streams, runs `run_once`,
asserts:
  - Redis `report:gate_value:latest` key is set
  - history stream `stream:reports:gate_value` has one entry
  - report JSON has expected shape (overall + groups, decision present)
  - Prometheus gauges have been touched
"""

from __future__ import annotations

import json

import fakeredis.aioredis
import pytest

from services.gate_value_reporter import prometheus_metrics as pm
from services.gate_value_reporter.reporter import run_once


def _payload(
    sid: str,
    *,
    y_edge: int,
    r_mult: float,
    symbol: str = "BTCUSDT",
    primary: int = 1,
    h_ms: int = 1_800_000,
) -> str:
    return json.dumps(
        {
            "sid": sid,
            "y_edge": y_edge,
            "r_mult": r_mult,
            "primary": primary,
            "symbol": symbol,
            "direction": "LONG",
            "h_ms": h_ms,
            "tp_bps": 15.0,
            "sl_bps": 10.0,
            "ret_bps": 10.0 if y_edge else -10.0,
            "entry_px": 50000.0,
        }
    )


@pytest.mark.asyncio
async def test_run_once_produces_report_and_history(monkeypatch):
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)

    # Lower thresholds so synthetic data triggers a real decision.
    monkeypatch.setenv("GATE_VALUE_MIN_N_PASSED", "10")
    monkeypatch.setenv("GATE_VALUE_MIN_N_GATED_OUT", "10")
    monkeypatch.setenv("GATE_VALUE_BOOTSTRAP_N", "100")
    monkeypatch.setenv("GATE_VALUE_BOOTSTRAP_SEED", "42")

    # 1) passed cohort: 20 mostly-winning samples
    for i in range(20):
        sid = f"of:BTCUSDT:{1700000000000 + i}"
        await r.xadd(
            "metrics:ml_confirm",
            {"sid": sid, "kind": "edge_stack_v1", "p_edge_cal": "0.7"},
        )
        await r.xadd(
            "labels:tb",
            {"payload": _payload(sid, y_edge=1 if i % 4 != 0 else 0, r_mult=1.0 if i % 4 != 0 else -1.0)},
        )

    # 2) gated_out cohort: 20 mostly-losing samples
    for i in range(20):
        sid = f"of:BTCUSDT:{1700000100000 + i}"
        await r.xadd(
            "stream:signals:gated_out_outcomes",
            {
                "sid": sid,
                "symbol": "BTCUSDT",
                "direction": "LONG",
                "entry": "50000.0",
                "ts_ms": str(1700000100000 + i),
                "horizon_ms": "1800000",
                "tp_bps": "15",
                "sl_bps": "10",
                "ret_bps": "-10",
                "r_mult": "-1.0" if i % 5 != 0 else "1.0",
                "y": "0" if i % 5 != 0 else "1",
                "tp_hit": "0" if i % 5 != 0 else "1",
                "sl_hit": "1" if i % 5 != 0 else "0",
                "kind": "confidence_v1_gated_out",
            },
        )

    report = await run_once(r, lookback_hours=24)

    assert report["n_groups"] >= 1
    assert "overall" in report
    overall = report["overall"]
    assert overall["passed"]["n"] == 20
    assert overall["gated_out"]["n"] == 20
    assert overall["passed"]["avg_r"] > overall["gated_out"]["avg_r"]
    assert overall["decision"]["action"] in {
        "KEEP_GATE",
        "RELAX_GATE",
        "INCONCLUSIVE",
        "DISABLE_GATE",
        "TIGHTEN_GATE",
        "INSUFFICIENT_DATA",
    }
    # With clearly positive lift the decision should not be INSUFFICIENT_DATA.
    assert overall["decision"]["action"] != "INSUFFICIENT_DATA"

    # Redis side effects
    latest_raw = await r.get("report:gate_value:latest")
    assert latest_raw is not None
    parsed = json.loads(latest_raw)
    assert parsed["n_groups"] >= 1

    history = await r.xrange("stream:reports:gate_value")
    assert len(history) == 1

    # Prometheus liveness
    assert pm.gate_value_reporter_up._value.get() == 1.0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_run_once_insufficient_data_when_streams_empty():
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    report = await run_once(r, lookback_hours=1)
    assert report["overall"]["passed"]["n"] == 0
    assert report["overall"]["gated_out"]["n"] == 0
    assert report["overall"]["decision"]["action"] == "INSUFFICIENT_DATA"


@pytest.mark.asyncio
async def test_run_once_replay_determinism(monkeypatch):
    """Same fixture + seed → identical decision and CI bounds."""
    monkeypatch.setenv("GATE_VALUE_MIN_N_PASSED", "5")
    monkeypatch.setenv("GATE_VALUE_MIN_N_GATED_OUT", "5")
    monkeypatch.setenv("GATE_VALUE_BOOTSTRAP_N", "200")
    monkeypatch.setenv("GATE_VALUE_BOOTSTRAP_SEED", "1234")

    async def _populate(client):
        for i in range(10):
            sid = f"of:ETHUSDT:{1700000000000 + i}"
            await client.xadd(
                "metrics:ml_confirm",
                {"sid": sid, "kind": "k", "p_edge_cal": "0.5"},
            )
            await client.xadd(
                "labels:tb",
                {"payload": _payload(sid, y_edge=i % 2, r_mult=0.5 if i % 2 else -0.5, symbol="ETHUSDT")},
            )
        for i in range(10):
            sid = f"of:ETHUSDT:{1700001000000 + i}"
            await client.xadd(
                "stream:signals:gated_out_outcomes",
                {
                    "sid": sid,
                    "symbol": "ETHUSDT",
                    "direction": "LONG",
                    "ts_ms": str(1700001000000 + i),
                    "horizon_ms": "1800000",
                    "tp_bps": "15",
                    "sl_bps": "10",
                    "r_mult": "-0.4" if i % 3 else "0.3",
                    "y": "0" if i % 3 else "1",
                    "tp_hit": "0" if i % 3 else "1",
                    "sl_hit": "1" if i % 3 else "0",
                },
            )

    r1 = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _populate(r1)
    report1 = await run_once(r1, lookback_hours=24)

    r2 = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _populate(r2)
    report2 = await run_once(r2, lookback_hours=24)

    assert report1["overall"]["decision"]["action"] == report2["overall"]["decision"]["action"]
    assert report1["overall"]["ci"] == report2["overall"]["ci"]
