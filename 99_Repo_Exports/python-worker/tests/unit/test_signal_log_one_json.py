import json
import logging
from types import SimpleNamespace

from common.signal_log_one_json import build_signal_one_json_obj, log_signal_one_json


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.msgs = []

    def emit(self, record: logging.LogRecord) -> None:
        self.msgs.append(record.getMessage())


def test_build_signal_one_json_obj_schema_stable():
    ctx = SimpleNamespace(
        spread_bps=2.5,
        obi_avg=1.2,
        microprice_shift_bps_20=-0.4,
        cancel_to_trade_bid_5s=0.9,
        taker_rate_ema=0.12,
        market_regime_score=0.3,
        geometry_score=0.8,
        data_quality_flags=["hlc_fallback"],
        ts_ms=10_000,
        l2_ts_ms=9_000,
    )
    payload = {
        "kind": "breakout",
        "side": 1,
        "symbol": "BTCUSDT",
        "ts": 10_000,
        "signal_id": "sid1",
        "raw_score": 2.0,
        "final_score": 1.0,
        "level_key": "L1",
    }
    obj = build_signal_one_json_obj(
        payload=payload,
        ctx=ctx,
        parts={},
        emitted=True,
        emit_ok=True,
        conf_factor01=0.5,
    )
    # stable keys (a subset sanity check)
    for k in (
        "type",
        "signal_id",
        "symbol",
        "kind",
        "side",
        "ts",
        "raw_score",
        "conf_factor01",
        "final_score",
        "spread_bps",
        "taker_rate",
        "regime_score",
        "geometry_score",
        "l2_is_stale",
        "used_fallback_hlc",
        "missing_htf",
        "missing_l3",
    ):
        assert k in obj


def test_log_signal_one_json_skips_when_info_disabled():
    # If INFO is disabled, we must not touch ctx at all.
    class BoomCtx:
        def __getattr__(self, name):
            raise AssertionError("ctx must not be accessed when INFO disabled")

    logger = logging.getLogger("t-signal-log-skip")
    logger.setLevel(logging.WARNING)  # INFO disabled
    h = _CaptureHandler()
    logger.handlers[:] = [h]

    log_signal_one_json(
        logger,
        payload={"kind": "breakout", "symbol": "BTCUSDT", "ts": 1, "signal_id": "x"},
        ctx=BoomCtx(),
        emitted=True,
        emit_ok=True,
        conf_factor01=0.5,
    )
    assert h.msgs == []


def test_log_signal_one_json_emits_single_line_json():
    logger = logging.getLogger("t-signal-log-json")
    logger.setLevel(logging.INFO)
    h = _CaptureHandler()
    logger.handlers[:] = [h]

    ctx = SimpleNamespace(spread_bps=1.0)
    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 1, "signal_id": "x", "raw_score": 1.0, "final_score": 0.5}

    log_signal_one_json(
        logger,
        payload=payload,
        ctx=ctx,
        emitted=True,
        emit_ok=True,
        conf_factor01=0.5,
    )
    assert len(h.msgs) == 1
    s = h.msgs[0]
    assert "\n" not in s
    obj = json.loads(s)
    assert obj["signal_id"] == "x"
    assert obj["kind"] == "breakout"
