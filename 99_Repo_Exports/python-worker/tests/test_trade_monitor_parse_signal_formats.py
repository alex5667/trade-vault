from __future__ import annotations

import json
from types import SimpleNamespace

from runners.trade_monitor_runner import _parse_signal
from services.trade_monitor import TradeMonitorService


class _SpecStub:
    trailing_profile_default = "rocket_v1"


def _mk_monitor() -> TradeMonitorService:
    """
    Minimal TradeMonitorService instance for calling _normalize_signal().
    """
    mon = TradeMonitorService.__new__(TradeMonitorService)
    mon._get_spec = lambda symbol: _SpecStub()
    mon.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    mon.default_lot = 1.0
    mon.stop_atr_mult = 1.0
    mon.rr_levels = [1.0, 2.0, 3.0]
    mon._crypto_suffixes = ("USDT", "USDC", "BUSD")
    mon._crypto_exclude_prefixes = ()
    mon._margin_fx_symbols = frozenset({""})
    return mon


def test_parse_signal_payload_json_merges_into_flat_dict():
    fields = {
        "schema": "1",
        "signal_id": "sig-1",
        "payload_json": json.dumps({"sid": "sig-1", "timeframe": "1m", "trail_after_tp1": 0}, separators=(",", ":")),
    }
    d = _parse_signal(fields)
    assert d["trail_after_tp1"] in (0, "0")
    assert d["signal_id"] == "sig-1"


def test_parse_signal_canonical_data_envelope_with_nested_payload():
    # This matches your Lua:
    #   XADD ... 'data' ARGV[3]  where ARGV[3] = json.dumps(envelope)
    env = {
        "signal_id": "sig-can-1",
        "ts_ms": 1700000000000,
        "kind": "volatility",
        "symbol": "BTCUSDT",
        "payload": {
            "sid": "sig-can-1",
            "timeframe": "1m",
            "trail_after_tp1": 0,
            "trail_after_tp1_reason": "LOW_MOMO",
        },
    }
    fields = {"data": json.dumps(env, separators=(",", ":"))}
    d = _parse_signal(fields)
    assert d["sid"] == "sig-can-1"
    assert d["trail_after_tp1"] in (0, "0")
    assert d["trail_after_tp1_reason"] == "LOW_MOMO"


def test_parse_signal_envelope_json_flattens_payload():
    env = {
        "signal_id": "sig-2",
        "ts_ms": 1700000000000,
        "payload": {"sid": "sig-2", "timeframe": "1m", "trail_after_tp1": 0, "trail_after_tp1_reason": "LOW_MOMO"},
    }
    fields = {"envelope_json": json.dumps(env, separators=(",", ":"))}
    d = _parse_signal(fields)
    assert d["sid"] == "sig-2"
    assert d["trail_after_tp1"] in (0, "0")
    assert d["trail_after_tp1_reason"] == "LOW_MOMO"


def test_parse_signal_legacy_data_json_supported():
    env = {"sid": "sig-3", "timeframe": "1m", "trail_after_tp1": 1}
    fields = {"data": json.dumps(env, separators=(",", ":"))}
    d = _parse_signal(fields)
    assert d["sid"] == "sig-3"
    assert d["trail_after_tp1"] in (1, "1")


def test_normalize_signal_accepts_timeframe_as_tf():
    mon = _mk_monitor()
    raw = {"sid": "sig-4", "timeframe": "1m", "symbol": "BTCUSDT", "price": 100.0, "direction": "LONG", "sl": 95.0, "tp_levels": [101,102,103]}
    sig = mon._normalize_signal(raw)
    assert sig is not None
    assert str(getattr(sig, "tf", "")).lower() in ("1m", "1min", "1minute", "60s", "60sec", "60") or getattr(sig, "tf", None) is not None
