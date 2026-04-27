from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

import pytest

from core.signal_json_logger import build_signal_json_log, log_signal_one_json


class FakeLogger:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def info(self, msg: str) -> None:
        self.lines.append(str(msg))


@dataclass
class FakeCtx:
    symbol: str = "BTCUSDT"
    ts: int = 1700000000000
    price: float = 42000.0
    spread_bps: float = 1.7
    obi_avg: float = 0.12
    microprice_shift_bps_20: float = 3.3
    cancel_to_trade_bid_20s: float = 9.0
    taker_rate_ema: float = 0.58
    market_regime_score: float = 0.35
    geometry_score: float = 0.22
    data_quality_flags: list[str] = None  # type: ignore


def test_build_signal_json_log_has_required_fields() -> None:
    ctx = FakeCtx(data_quality_flags=["hlc_fallback"])
    payload = {
        "signal_id": "sid-1",
        "kind": "breakout",
        "side": "buy",
        "symbol": ctx.symbol,
        "ts": ctx.ts,
        "price": ctx.price,
        "raw_score": 2.0,
        "conf_factor": 0.6,
        "final_score": 1.2,
        "level_price": 42010.0,
    }
    parts = {"l2_stale": 0}
    obj = build_signal_json_log(payload=payload, ctx=ctx, parts=parts)

    assert obj["signal_id"] == "sid-1"
    assert obj["kind"] == "breakout"
    assert obj["side"] == "buy"
    assert obj["level_key"] is not None  # fallback from level_price
    assert obj["raw_score"] == 2.0
    assert obj["conf_factor"] == 0.6
    assert obj["final_score"] == 1.2

    feats = obj["features"]
    assert feats["spread_bps"] == pytest.approx(1.7)
    assert feats["obi_avg"] == pytest.approx(0.12)
    assert feats["microprice_shift"] == pytest.approx(3.3)
    assert feats["cancel_to_trade"] == pytest.approx(9.0)
    assert feats["taker_rate"] == pytest.approx(0.58)
    assert feats["regime_score"] == pytest.approx(0.35)
    assert feats["geometry_score"] == pytest.approx(0.22)

    dq = obj["data_quality"]
    assert dq["used_fallback_hlc"] is True
    assert dq["missing_htf"] is False
    assert dq["missing_l3"] is False


def test_log_signal_one_json_is_single_line_json() -> None:
    logger = FakeLogger()
    ctx = FakeCtx(data_quality_flags=[])
    payload = {
        "signal_id": "sid-2",
        "kind": "absorption",
        "side": "sell",
        "symbol": ctx.symbol,
        "ts": ctx.ts,
        "price": ctx.price,
        "raw_score": 1.0,
        "conf_factor": 0.9,
        "final_score": 0.9,
        "level_key": "p:42000.0",
    }

    log_signal_one_json(logger, payload=payload, ctx=ctx, parts={"l2_is_stale": False})
    assert len(logger.lines) == 1

    line = logger.lines[0]
    # 1 строка JSON (без \n внутри)
    assert "\n" not in line
    obj = json.loads(line)
    assert obj["signal_id"] == "sid-2"
    assert obj["level_key"] == "p:42000.0"


def test_missing_l3_flag_when_no_l3_fields() -> None:
    @dataclass
    class CtxNoL3:
        symbol: str = "BTCUSDT"
        ts: int = 1
        price: float = 1.0
        spread_bps: Optional[float] = None
        microprice_shift_bps_20: Optional[float] = None
        cancel_to_trade_bid_20s: Optional[float] = None
        data_quality_flags: list[str] = None  # type: ignore

    ctx = CtxNoL3(data_quality_flags=[])
    payload = {"signal_id": "sid-3", "kind": "breakout", "side": "buy", "symbol": "BTCUSDT", "ts": 1, "price": 1.0}
    obj = build_signal_json_log(payload=payload, ctx=ctx, parts={})
    assert obj["data_quality"]["missing_l3"] is True
