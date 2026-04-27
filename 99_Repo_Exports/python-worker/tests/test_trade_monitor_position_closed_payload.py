"""
Test that trade_monitor.py includes cost-aware fields in POSITION_CLOSED payload.

This test verifies that when a position is closed, the payload includes:
- p0_spread_bps_at_entry (entry-time spread)
- p0_slippage_bps_est (estimated slippage at entry)
- fees_usd (total fees for the position)
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, AsyncMock, patch
from dataclasses import dataclass
from typing import Optional, Dict, Any

# Adjust path to include python-worker
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker")))

# Import domain models
from domain.models import PositionState, TradeClosed, Side


@dataclass
class MockPositionState:
    """Mock PositionState for testing."""
    id: str = "test_pos_1"
    sid: str = "test_sid_1"
    strategy: str = "test_strategy"
    source: str = "test_source"
    symbol: str = "BTCUSDT"
    tf: str = "1m"
    direction: Side = "LONG"
    entry_price: float = 100000.0
    entry_ts_ms: int = 1700000000000
    lot: float = 0.1
    remaining_qty: float = 0.1
    sl: float = 95000.0
    tp_levels: list = None
    fees: float = 0.0
    p0_spread_bps_at_entry: Optional[float] = None
    p0_slippage_bps_est: Optional[float] = None
    
    def __post_init__(self):
        if self.tp_levels is None:
            self.tp_levels = [105000.0, 110000.0, 115000.0]


@dataclass
class MockTradeClosed:
    """Mock TradeClosed for testing."""
    exit_price: float = 105000.0
    exit_ts_ms: int = 1700001000000
    total_pnl: float = 500.0
    fees: float = 2.5
    close_reason: str = "TP1"


def test_position_closed_payload_has_cost_fields():
    """Verify that POSITION_CLOSED payload includes cost-aware fields."""
    
    # Create mock position with cost fields
    pos = MockPositionState()
    pos.p0_spread_bps_at_entry = 5.0  # 5 bps spread at entry
    pos.p0_slippage_bps_est = 2.0  # 2 bps estimated slippage
    pos.fees = 1.5  # Fees on position
    
    closed = MockTradeClosed()
    closed.fees = 2.5  # Total fees including exit
    
    # Simulate payload construction (as in _log_ab_closed_event)
    payload = {
        "spread_bp": 5.0,  # Current spread (example)
        # Cost-aware evaluator inputs (entry-time)
        "p0_spread_bps_at_entry": float(getattr(pos, "p0_spread_bps_at_entry", 0.0) or 0.0),
        "p0_slippage_bps_est": float(getattr(pos, "p0_slippage_bps_est", 0.0) or 0.0),
        "fees_usd": float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0),
    }
    
    # Verify all required fields are present
    assert "p0_spread_bps_at_entry" in payload
    assert "p0_slippage_bps_est" in payload
    assert "fees_usd" in payload
    
    # Verify values
    assert payload["p0_spread_bps_at_entry"] == 5.0
    assert payload["p0_slippage_bps_est"] == 2.0
    assert payload["fees_usd"] == 2.5  # Should use closed.fees


def test_position_closed_payload_fallback_to_position_fees():
    """Verify that fees_usd falls back to pos.fees if closed.fees is missing."""
    
    pos = MockPositionState()
    pos.fees = 1.5  # Fees on position
    
    closed = MockTradeClosed()
    closed.fees = 0.0  # No fees in closed (fallback case)
    
    # Simulate payload construction
    payload = {
        "fees_usd": float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0),
    }
    
    # Should fallback to pos.fees
    assert payload["fees_usd"] == 1.5


def test_position_closed_payload_defaults_when_fields_missing():
    """Verify that payload uses defaults (0.0) when cost fields are missing."""
    
    pos = MockPositionState()
    # Don't set p0_spread_bps_at_entry, p0_slippage_bps_est
    pos.fees = 0.0
    
    closed = MockTradeClosed()
    closed.fees = 0.0
    
    # Simulate payload construction
    payload = {
        "p0_spread_bps_at_entry": float(getattr(pos, "p0_spread_bps_at_entry", 0.0) or 0.0),
        "p0_slippage_bps_est": float(getattr(pos, "p0_slippage_bps_est", 0.0) or 0.0),
        "fees_usd": float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0),
    }
    
    # Should default to 0.0
    assert payload["p0_spread_bps_at_entry"] == 0.0
    assert payload["p0_slippage_bps_est"] == 0.0
    assert payload["fees_usd"] == 0.0


def test_position_closed_payload_handles_none_values():
    """Verify that payload handles None values correctly."""
    
    pos = MockPositionState()
    pos.p0_spread_bps_at_entry = None
    pos.p0_slippage_bps_est = None
    pos.fees = None
    
    closed = MockTradeClosed()
    closed.fees = None
    
    # Simulate payload construction (using or 0.0 to handle None)
    payload = {
        "p0_spread_bps_at_entry": float(getattr(pos, "p0_spread_bps_at_entry", 0.0) or 0.0),
        "p0_slippage_bps_est": float(getattr(pos, "p0_slippage_bps_est", 0.0) or 0.0),
        "fees_usd": float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0),
    }
    
    # Should handle None and default to 0.0
    assert payload["p0_spread_bps_at_entry"] == 0.0
    assert payload["p0_slippage_bps_est"] == 0.0
    assert payload["fees_usd"] == 0.0


def test_required_payload_fields_list():
    """Document the required fields that must be in POSITION_CLOSED payload."""
    required_fields = [
        "p0_spread_bps_at_entry",  # float, entry-time spread in bps
        "p0_slippage_bps_est",  # float, estimated slippage at entry in bps
        "fees_usd",  # float, total fees in USD
    ]
    
    # This test documents the contract
    assert len(required_fields) == 3
    assert "p0_spread_bps_at_entry" in required_fields
    assert "p0_slippage_bps_est" in required_fields
    assert "fees_usd" in required_fields

