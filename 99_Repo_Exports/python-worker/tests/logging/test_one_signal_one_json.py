from __future__ import annotations

import json
import logging
from types import SimpleNamespace

from handlers.crypto_orderflow.logging.logging_utils import log_signal_one_json_unified
from signal_scoring import reason_registry as rr


def _ctx():
    # Minimal ctx skeleton for the logger helper
    return SimpleNamespace(
        symbol="BTCUSDT",
        ts=1234567890000,
        price=100.0,
        spread_bps=1.2,
        obi_avg=0.15,
        microprice_shift_bps_20=-0.8,
        cancel_to_trade_bid_5s=2.5,
        taker_rate_ema=0.08,
        market_regime_score=0.35,
        geometry_score=0.42,
        l2_is_stale=False,
        used_fallback_hlc=False,
        missing_htf=False,
        missing_l3=False,
    )


def test_emit_log_is_single_json_line_with_required_keys(caplog):
    logger = logging.getLogger("test_emit_one_json")
    caplog.set_level(logging.INFO, logger="test_emit_one_json")

    payload = {
        "signal_id": "sid-1",
        "kind": "breakout",
        "side": 1,
        "symbol": "BTCUSDT",
        "ts": 1234567890000,
        "level_key": "L:100.0",
        "raw_score": 2.0,
        "final_score": 1.2,
        "confidence": 65.0,
    }
    parts = {"conf_factor01": 0.6}

    log_signal_one_json_unified(logger, payload=payload, ctx=_ctx(), parts=parts, conf_factor=0.6, event="emit")

    assert len(caplog.records) == 1
    obj = json.loads(caplog.records[0].message)
    assert obj["event"] == "emit"
    assert obj["signal_id"] == "sid-1"
    assert obj["kind"] == "breakout"
    assert obj["veto"] == 0
    assert "dq" in obj and isinstance(obj["dq"], dict)
    assert "conf_factor" in obj


def test_veto_log_includes_reason_code_and_u16(caplog):
    logger = logging.getLogger("test_veto_one_json")
    caplog.set_level(logging.INFO, logger="test_veto_one_json")

    rc = rr.normalize_reason(reason="VETO_WALL_NEAR", reason_code="")[1]
    u16 = rr.reason_code_to_u16(rc)

    payload = {
        "signal_id": None,
        "kind": "breakout",
        "side": 1,
        "symbol": "BTCUSDT",
        "ts": 1234567890000,
        "level_key": "L:100.0",
        "raw_score": 2.0,
        "final_score": 0.0,
        "confidence": 0.0,
    }

    log_signal_one_json_unified(
        logger,
        payload=payload,
        ctx=_ctx(),
        parts={"l2_score01": 0.0},
        veto=True,
        veto_reason_code=rc,
        veto_reason_u16=int(u16),
        conf_factor=0.0,
        event="veto",
    )

    assert len(caplog.records) == 1
    obj = json.loads(caplog.records[0].message)
    assert obj["event"] == "veto"
    assert obj["veto"] == 1
    assert obj["veto_reason_code"] == rc
    assert obj["veto_reason_u16"] == int(u16)
    # Ensure single-line compact JSON (no pretty spaces/newlines)
    assert "\n" not in caplog.records[0].message
