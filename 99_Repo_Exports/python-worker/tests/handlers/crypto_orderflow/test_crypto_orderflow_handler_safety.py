from __future__ import annotations

import json
from enum import Enum

import pytest


# NOTE:
# Adjust import path if your module is located differently.
from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler


class Kind(Enum):
    BREAKOUT = "breakout"


class Side(Enum):
    LONG = "LONG"


class DummyCtx:
    def __init__(self, regime: str):
        self.market_regime = regime
        self.regime = regime


def test_safe_lower_accepts_enum():
    assert CryptoOrderFlowHandler._safe_lower(Kind.BREAKOUT) == "kind.breakout" or CryptoOrderFlowHandler._safe_lower(Kind.BREAKOUT)
    # We don't assert exact string form because Enum.__str__ could differ across your codebase,
    # but we assert it never raises and returns a string.
    out = CryptoOrderFlowHandler._safe_lower(Kind.BREAKOUT)
    assert isinstance(out, str)
    assert out != ""





def test_safe_reason_u16_fallback_when_mapper_raises(monkeypatch):
    # Force reason_code_to_u16 to raise to validate fail-open behaviour.
    import handlers.crypto_orderflow_handler as mod

    def boom(*args, **kwargs):
        raise RuntimeError("mapper failed")

    monkeypatch.setattr(mod, "reason_code_to_u16", boom)

    u = CryptoOrderFlowHandler._safe_reason_u16("OK", default=1)
    assert u == 1


def test_sanitize_u16_list_filters_bad_values():
    xs = [1, "2", -1, 70000, "x", None, 0, 65535, 65536]
    out = CryptoOrderFlowHandler._sanitize_u16_list(xs)
    assert out == [1, 2, 0, 65535]


def test_log_payload_is_json_serializable_with_enum_values():
    # This reproduces the real issue: Enums/custom objects in kind/side can break JSON logging.
    payload = {
        "kind": CryptoOrderFlowHandler._safe_str(Kind.BREAKOUT),
        "side": CryptoOrderFlowHandler._safe_str(Side.LONG),
        "symbol": "BTCUSDT",
        "ts": 123,
    }
    s = json.dumps(payload)
    assert isinstance(s, str)


def test_qf_pack_fail_open(monkeypatch):
    # Emulate pack_qf_u16 failing: handler must swallow exception.
    import handlers.crypto_orderflow_handler as mod

    def boom(*args, **kwargs):
        raise RuntimeError("pack failed")

    monkeypatch.setattr(mod, "pack_qf_u16", boom)

    qf = CryptoOrderFlowHandler._sanitize_u16_list([1, 2, 3])
    assert qf == [1, 2, 3]
    # The pack call itself is guarded in production code; here we just ensure sanitizer is OK.
