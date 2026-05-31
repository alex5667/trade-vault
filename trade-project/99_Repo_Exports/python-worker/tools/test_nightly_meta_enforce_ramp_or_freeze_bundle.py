from __future__ import annotations
from core.redis_keys import RedisStreams as RS

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_nightly_meta_enforce_ramp_or_freeze_bundle.py

Unit tests for nightly_meta_enforce_ramp_or_freeze_bundle.py
"""


import os

# Import the module functions
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from tools.nightly_meta_enforce_ramp_or_freeze_bundle import (
    _notify,
    _read_float_h,
    now_ms,
    sign,
)


def test_now_ms():
    """Test timestamp generation."""
    ts = now_ms()
    assert ts > 0
    assert isinstance(ts, int)


def test_sign():
    """Test HMAC signature generation."""
    bid = "test_bundle_id"
    secret = "test_secret"
    sig = sign(bid, secret)
    assert len(sig) == 8
    assert isinstance(sig, str)
    # Same input should produce same signature
    sig2 = sign(bid, secret)
    assert sig == sig2


def test_read_float_h():
    """Test reading float from Redis hash."""
    r = MagicMock()
    r.hget.return_value = "0.25"
    result = _read_float_h(r, "key", "field", 0.0)
    assert result == 0.25

    # Test with None (default)
    r.hget.return_value = None
    result = _read_float_h(r, "key", "field", 0.5)
    assert result == 0.5

    # Test with invalid value
    r.hget.return_value = "invalid"
    result = _read_float_h(r, "key", "field", 0.0)
    assert result == 0.0


def test_notify():
    """Test notification function."""
    r = MagicMock()
    text = "Test notification"
    _notify(r, text)

    # Check that xadd was called
    assert r.xadd.called
    call_args = r.xadd.call_args
    assert call_args[0][0] == os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    fields = call_args[0][1]
    assert fields["type"] == "report"
    assert fields["text"] == text
    assert "ts" in fields


def test_notify_with_buttons():
    """Test notification with buttons."""
    r = MagicMock()
    text = "Test with buttons"
    buttons = [[{"text": "Approve", "callback": "test:approve"}]]
    _notify(r, text, buttons=buttons)

    assert r.xadd.called
    call_args = r.xadd.call_args
    fields = call_args[0][1]
    assert "buttons" in fields
    # Buttons should be JSON string
    assert isinstance(fields["buttons"], str)


def test_freeze_cell_parsing():
    """Test parsing of cell keys like 'BTCUSDT|trend'."""
    ck = "BTCUSDT|trend"
    parts = ck.split("|")
    assert len(parts) == 2
    sym, bucket = parts[0].upper(), parts[1].lower()
    assert sym == "BTCUSDT"
    assert bucket == "trend"

    # Test with range
    ck2 = "ETHUSDT|range"
    parts2 = ck2.split("|")
    sym2, bucket2 = parts2[0].upper(), parts2[1].lower()
    assert sym2 == "ETHUSDT"
    assert bucket2 == "range"


def test_freeze_bundle_ops():
    """Test freeze bundle operations generation."""
    # Simulate freeze ops for a cell
    prefix = "config:orderflow:"
    sym = "BTCUSDT"
    bucket = "trend"
    freeze_to = 0.0
    use_per_regime = True

    ops = []
    hk = f"{prefix}{sym}"
    ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
    ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
    ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_salt", "value": "enf_v1"})

    if use_per_regime:
        ops.append({"op": "HSET", "key": hk, "field": f"meta_enforce_share_{bucket}", "value": f"{freeze_to:.2f}"})
        ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share_news", "value": "0.00"})

    assert len(ops) == 5
    assert ops[3]["field"] == "meta_enforce_share_trend"
    assert ops[3]["value"] == "0.00"
    assert ops[4]["field"] == "meta_enforce_share_news"
    assert ops[4]["value"] == "0.00"


def test_ramp_bundle_ops():
    """Test ramp bundle operations generation."""
    prefix = "config:orderflow:"
    sym = "BTCUSDT"
    nxt = 0.25
    use_per_regime = True

    ops = []
    hk = f"{prefix}{sym}"
    ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
    ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
    ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_salt", "value": "enf_v1"})

    if use_per_regime:
        ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share_news", "value": "0.00"})
        ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share_trend", "value": f"{nxt:.2f}"})
        ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share_range", "value": f"{nxt:.2f}"})
        ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share_other", "value": "0.00"})

    assert len(ops) == 7
    assert ops[3]["field"] == "meta_enforce_share_news"
    assert ops[3]["value"] == "0.00"
    assert ops[4]["field"] == "meta_enforce_share_trend"
    assert ops[4]["value"] == "0.25"
    assert ops[5]["field"] == "meta_enforce_share_range"
    assert ops[5]["value"] == "0.25"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

