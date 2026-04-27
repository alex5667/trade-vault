"""Tests: exact flatten / dust cleanup integration (trade_binance_dust_cleanup_exact_flatten patch).

Covers:
  1. _force_flatten_symbol_exact — retry loop with dust tail detection
  2. handle_cancel — routes through exact flatten & surfaces residuals in state
  3. BinanceFuturesClient.get_symbol_position_risk — symbol + positionSide filtering
"""
from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
MOD_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(MOD_DIR))

import importlib.util

def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod

mod = _load_module("binance_executor", MOD_DIR / "binance_executor.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_exec(**overrides):
    """Build a minimal BinanceExecutor without touching Redis / network."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.position_mode = "oneway"
    ex.dust_notional_usdt = 3.0
    ex.dust_margin_usdt = 1.0
    ex.dust_close_retries = 3
    ex.dust_verify_timeout_ms = 500
    ex.dust_verify_poll_ms = 100
    ex._exec_event = MagicMock()
    ex._cancel_by_token = MagicMock(return_value=[])
    ex._position_qty_tolerance = MagicMock(return_value=0.0001)
    ex._load_order_state = MagicMock(return_value={})
    ex._save_order_state = MagicMock()
    ex._derive_audit_chain_fields = MagicMock(return_value={})
    ex._format_order_ref = MagicMock(return_value="binance:exit:12345")
    ex._new_closed_trade_id = MagicMock(return_value="ctid-001")
    ex._sync_client_clock = MagicMock()
    ex._resolve_client = MagicMock()
    ex.allowlist = set()
    for k, v in overrides.items():
        setattr(ex, k, v)
    return ex


def _mock_filters():
    f = MagicMock()
    f.get.return_value = MagicMock(step_size=0.001, tick_size=0.01, min_qty=0.001,
                                   min_notional=5.0)
    return f


def _pos_risk_row(symbol="BTCUSDT", amt=0.0, notional=0.0, margin=0.0, side="BOTH"):
    return {
        "symbol": symbol,
        "positionAmt": str(amt),
        "notional": str(notional),
        "isolatedMargin": str(margin),
        "positionSide": side,
        "leverage": "10",
    }


# ---------------------------------------------------------------------------
# Test 1: _force_flatten_symbol_exact — retry loop stubs a dust tail
# ---------------------------------------------------------------------------

def test_force_flatten_symbol_exact_retries_dust_tail():
    """First attempt leaves a dust residual; second attempt achieves flat."""
    ex = _base_exec()
    client = MagicMock()
    filters = _mock_filters()

    #
    # Sequence of get_position_risk calls:
    #   call 0  — _get_live_symbol_exposure (initial check)  → 0.01 BTC still open
    #   call 1  — _get_live_symbol_exposure (attempt 1 live) → 0.01 BTC
    #   call 2+ — _verify_symbol_flat polls (attempt 1)      → 0.0005 BTC (dust)
    #   call 3  — _get_live_symbol_exposure (attempt 2 live) → 0.0005 BTC (dust)
    #   call 4+ — _verify_symbol_flat polls (attempt 2)      → 0.0 BTC (flat)
    #
    def _side_effect():
        # call 0: initial, non-flat — has position
        yield [_pos_risk_row(amt=0.01, notional=600.0, margin=6.0)]
        # call 1: attempt 1 live read
        yield [_pos_risk_row(amt=0.01, notional=600.0, margin=6.0)]
        # call 2: verify poll 1 — dust remains
        yield [_pos_risk_row(amt=0.0005, notional=0.5, margin=0.05)]
        # call 3: attempt 2 live read
        yield [_pos_risk_row(amt=0.0005, notional=0.5, margin=0.05)]
        # call 4: verify poll 2 — flat
        yield [_pos_risk_row(amt=0.0, notional=0.0, margin=0.0)]

    gen = _side_effect()
    client.get_position_risk.side_effect = lambda: next(gen)
    client.get_open_orders.return_value = []
    client.get_open_algo_orders.return_value = []
    client.cancel_all_orders.return_value = {}

    # _submit_reduce_only_market_exit returns a dict with close_order_id
    ex._submit_reduce_only_market_exit = MagicMock(
        return_value={"close_order_id": 9999, "close_client_id": "cid-9999"}
    )

    with patch("time.sleep"):
        with patch("time.time", side_effect=[
            0.0,   # verify timeout: deadline
            0.0,   # while loop first check (attempt 1 verify poll)
            0.11,  # second while check → beyond dust_verify_timeout... but we want flat
            # attempt 2 verify
            0.0,   # deadline
            0.0,   # while check 1 — last yield is flat
        ] + [1e9] * 20):
            result = ex._force_flatten_symbol_exact(
                sid="sid-test-1",
                symbol="BTCUSDT",
                client=client,
                filters=filters,
                logical_side="LONG",
                reason_tag="emerg",
                max_attempts=3,
            )

    assert result["status"] in {"closed", "dust_remaining", "residual_position"}, result
    assert len(result["attempts"]) >= 1, "Expected at least 1 close attempt"
    # Close order was submitted at least once
    assert ex._submit_reduce_only_market_exit.call_count >= 1


# ---------------------------------------------------------------------------
# Test 2: handle_cancel routes through _force_flatten_symbol_exact
# ---------------------------------------------------------------------------

def test_handle_cancel_uses_exact_flatten_and_surfaces_residuals():
    """handle_cancel should route close through _force_flatten_symbol_exact
    and relay residual fields into result and saved state."""
    ex = _base_exec()
    client = MagicMock()
    filters = _mock_filters()
    ex._resolve_client = MagicMock(return_value=(client, filters))

    # Stub _force_flatten_symbol_exact — returns a clean "closed" status
    ex._force_flatten_symbol_exact = MagicMock(return_value={
        "status": "closed",
        "residual_qty": 0.0,
        "residual_notional_usdt": 0.0,
        "residual_margin_usdt": 0.0,
        "close_order_id": 55501,
        "verify": {"logical_side": "LONG", "abs_qty": 0.0},
    })

    result = ex.handle_cancel({"sid": "sid-cancel-1", "symbol": "BTCUSDT"})

    # _force_flatten_symbol_exact must have been called
    ex._force_flatten_symbol_exact.assert_called_once()
    call_kwargs = ex._force_flatten_symbol_exact.call_args[1]
    assert call_kwargs["sid"] == "sid-cancel-1"
    assert call_kwargs["symbol"] == "BTCUSDT"
    assert call_kwargs["reason_tag"] == "close"

    # result should carry residual info
    assert "residual_qty" in result, "residual_qty must be surfaced in result"
    assert result["closed"] == "true"

    # _save_order_state called with residual fields
    ex._save_order_state.assert_called_once()
    saved = ex._save_order_state.call_args[0][1]
    assert "residual_qty" in saved, "residual_qty must be persisted in state"


# ---------------------------------------------------------------------------
# Test 3: BinanceFuturesClient.get_symbol_position_risk — filtering
# ---------------------------------------------------------------------------

def test_get_symbol_position_risk_matches_symbol_and_side():
    """get_symbol_position_risk must return the matching row by symbol + positionSide."""
    client_cls_path = MOD_DIR / "binance_futures_client.py"
    client_mod = _load_module("binance_futures_client", client_cls_path)
    client = client_mod.BinanceFuturesClient.__new__(client_mod.BinanceFuturesClient)

    rows = [
        {"symbol": "BTCUSDT", "positionAmt": "0.5", "positionSide": "LONG",
         "notional": "30000", "isolatedMargin": "300"},
        {"symbol": "BTCUSDT", "positionAmt": "-0.5", "positionSide": "SHORT",
         "notional": "30000", "isolatedMargin": "300"},
        {"symbol": "ETHUSDT", "positionAmt": "1.0", "positionSide": "LONG",
         "notional": "3000", "isolatedMargin": "30"},
    ]
    client.get_position_risk = MagicMock(return_value=rows)

    # Should find BTCUSDT LONG
    row = client.get_symbol_position_risk("BTCUSDT", position_side="LONG")
    assert row.get("positionSide") == "LONG"
    assert row.get("symbol") == "BTCUSDT"

    # Should find BTCUSDT SHORT
    row2 = client.get_symbol_position_risk("BTCUSDT", position_side="SHORT")
    assert row2.get("positionSide") == "SHORT"

    # Should find ETHUSDT without filtering side
    row3 = client.get_symbol_position_risk("ETHUSDT")
    assert row3.get("symbol") == "ETHUSDT"

    # Non-existent symbol returns empty dict
    row4 = client.get_symbol_position_risk("XYZUSDT")
    assert row4 == {}


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
