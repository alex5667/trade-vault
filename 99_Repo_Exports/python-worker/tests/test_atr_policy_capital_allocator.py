from __future__ import annotations

import os
from contextlib import contextmanager

from services.atr_policy_capital_allocator import (
    _cert_mult,
    _rollout_mult,
    _relu,
    _clip,
    _scope_key
)

def test_cert_mult():
    assert _cert_mult("passed") == 1.0
    assert _cert_mult("failed") == 0.0
    assert _cert_mult("stale") == 0.0
    assert _cert_mult("unknown") == 0.7
    assert _cert_mult("") == 0.7
    assert _cert_mult(None) == 0.7

def test_rollout_mult():
    assert _rollout_mult("shadow") == 0.0
    assert _rollout_mult("canary_5") == 0.35
    assert _rollout_mult("canary_25") == 0.70
    assert _rollout_mult("live_100") == 1.00
    assert _rollout_mult("frozen") == 0.0
    assert _rollout_mult("rolled_back") == 0.0
    assert _rollout_mult("unknown") == 0.0

def test_relu_clip():
    assert _relu(5.0) == 5.0
    assert _relu(-1.0) == 0.0
    assert _clip(5.0, 0.0, 10.0) == 5.0
    assert _clip(15.0, 0.0, 10.0) == 10.0
    assert _clip(-5.0, 0.0, 10.0) == 0.0

def test_scope_key():
    row = {
        "source": "CryptoOrderFlow",
        "symbol": "BTCUSDT",
        "scenario": "trend",
        "regime": "bull",
        "risk_horizon_bucket": "1h",
        "atr_policy_ver": 4
    }
    key = _scope_key(row, "stop_ttl")
    assert key == "policy:CryptoOrderFlow:BTCUSDT:trend:bull:1h:stop_ttl:4"

def test_scope_key_missing_fields():
    row = {"symbol": "ETHUSDT"}
    key = _scope_key(row, "trailing")
    assert key == "policy:CryptoOrderFlow:ETHUSDT:unknown:unknown:unknown:trailing:0"
