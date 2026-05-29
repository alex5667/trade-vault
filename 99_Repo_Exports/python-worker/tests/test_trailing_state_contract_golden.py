"""Contract regression tests for the events:trailing:state audit stream."""

import json
import time
import os
from unittest.mock import MagicMock
from pathlib import Path

import pytest

from services.trailing_state_writer import _normalize_row
from orderflow_services.trailing_state_autocal_v1 import _read_audit_stream, _SymbolStats

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "trailing_state_golden_payload.json"

@pytest.fixture
def golden_payload() -> dict:
    with open(FIXTURE_PATH, "r") as f:
        return json.load(f)

def test_trailing_state_writer_golden_contract(golden_payload):
    """Verify that TrailingStateWriter parses the golden payload correctly,
    including the new price/watermark/ATR fields.
    """
    row, reason = _normalize_row(golden_payload)
    assert reason == ""
    assert row is not None

    # Check required fields
    assert row["sid"] == "of:BTCUSDT:1700000000000:LONG"
    assert row["symbol"] == "BTCUSDT"
    assert row["ts_ms"] == 1700000123456

    # Check optional analytical fields (contract additions)
    assert row["price"] == 65000.5
    assert row["old_sl"] == 64900.0
    assert row["new_sl"] == 64950.0
    assert row["high_watermark"] == 65100.0
    assert row["low_watermark"] == 64800.0
    assert row["atr_value"] == 150.25
    assert row["atr_mult"] == 1.5

def test_trailing_state_autocal_golden_contract(golden_payload):
    """Verify that TrailingStateAutocalV1 ignores extra fields and correctly
    computes delta_bps without crashing on new fields.
    """
    golden_payload["ts_ms"] = str(int(time.time() * 1000))
    
    r_mock = MagicMock()
    # Mock xread to return the golden payload
    r_mock.xread.return_value = [
        (
            "events:trailing:state",
            [
                (f"{golden_payload['ts_ms']}-0", golden_payload)
            ]
        )
    ]

    bins = {}
    window_ms = 43_200_000
    
    cursor, n_ingested = _read_audit_stream(r_mock, "0-0", bins, window_ms)
    
    assert n_ingested == 1
    assert "BTCUSDT" in bins
    
    stats: _SymbolStats = bins["BTCUSDT"]
    assert stats.n == 1
    
    sample = stats.buf[0]
    assert sample.symbol == "BTCUSDT"
    assert sample.event_type == "other"  # 'transition' maps to 'other'
    
    # Delta BPS calculation: abs(64950 - 64900) / 65000.5 * 10000 = 7.6922
    expected_delta = abs(64950.0 - 64900.0) / 65000.5 * 10000
    assert abs(sample.delta_bps - expected_delta) < 0.01

def test_trailing_state_autocal_sl_move_golden(golden_payload):
    """Verify that Autocal counts sl_move properly."""
    golden_payload_sl_move = golden_payload.copy()
    golden_payload_sl_move["event_type"] = "sl_move"
    golden_payload_sl_move["ts_ms"] = str(int(time.time() * 1000))
    
    r_mock = MagicMock()
    r_mock.xread.return_value = [
        (
            "events:trailing:state",
            [
                (f"{golden_payload_sl_move['ts_ms']}-1", golden_payload_sl_move)
            ]
        )
    ]

    bins = {}
    cursor, n = _read_audit_stream(r_mock, "0-0", bins, 43_200_000)
    assert n == 1
    
    stats = bins["BTCUSDT"]
    assert stats.n_sl_moves() == 1
    assert stats.n_with_delta() == 1
    assert stats.median_delta_bps() > 0
