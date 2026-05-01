from __future__ import annotations
"""Tests for P0–P4 gap closures in binance_futures_client.py and binance_executor.py.

Covers:
  - _validate_plain_order_contract: hedge/oneway, closePosition, reduceOnly
  - _validate_algo_order_contract: closePosition/reduceOnly, workingType,
                                    trailing callbackRate, activatePrice
  - replace_untriggered_algo_order: cancel-then-post flow
  - leverage tier resolver: _symbol_tier / _resolve_symbol_leverage
  - resize target normalization: _normalize_resize_target
  - structured state contract: _structured_order_contract / _causal_timestamps
"""

from utils.time_utils import get_ny_time_millis

import hashlib
import os
import time
import json
import pytest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Import targets (handle both package and standalone paths)
# ---------------------------------------------------------------------------

try:
    from services.binance_futures_client import (
        AlgoOrderRef,
        PlainOrderRef,
        _float_or_none,
        _require_positive,
        _truthy,
        _validate_algo_order_contract,
        _validate_plain_order_contract,
        _validate_working_type,
        BinanceFuturesClient,
    )
    from services.binance_executor import BinanceExecutor
except ImportError:
    from binance_futures_client import (
        AlgoOrderRef,
        PlainOrderRef,
        _float_or_none,
        _require_positive,
        _truthy,
        _validate_algo_order_contract,
        _validate_plain_order_contract,
        _validate_working_type,
        BinanceFuturesClient,
    )
    from binance_executor import BinanceExecutor


# ===========================================================================
# Validator — plain order
# ===========================================================================

class TestPlainOrderContract:
    """P0: _validate_plain_order_contract."""

    def test_valid_market_oneway(self):
        _validate_plain_order_contract(
            {"type": "MARKET", "quantity": 0.01, "side": "BUY"},
            position_mode="oneway",
        )

    def test_valid_limit_oneway(self):
        _validate_plain_order_contract(
            {"type": "LIMIT", "quantity": 0.01, "price": 30000, "timeInForce": "GTC"},
            position_mode="oneway",
        )

    def test_missing_order_type_raises(self):
        with pytest.raises(ValueError, match="missing_order_type"):
            _validate_plain_order_contract({"quantity": 0.01}, position_mode="oneway")

    def test_close_position_raises_on_plain_order(self):
        with pytest.raises(ValueError, match="closePosition_not_supported"):
            _validate_plain_order_contract(
                {"type": "MARKET", "quantity": 0.01, "closePosition": True},
                position_mode="oneway",
            )

    def test_requires_position_side_in_hedge(self):
        with pytest.raises(ValueError, match="positionSide_required_in_hedge"):
            _validate_plain_order_contract(
                {"type": "MARKET", "quantity": 0.01},
                position_mode="hedge",
            )

    def test_valid_hedge_with_position_side(self):
        _validate_plain_order_contract(
            {"type": "MARKET", "quantity": 0.01, "positionSide": "LONG"},
            position_mode="hedge",
        )

    def test_reduce_only_forbidden_in_hedge(self):
        with pytest.raises(ValueError, match="reduceOnly_forbidden"):
            _validate_plain_order_contract(
                {"type": "MARKET", "quantity": 0.01, "positionSide": "LONG", "reduceOnly": True},
                position_mode="hedge",
            )

    def test_quantity_required(self):
        with pytest.raises(ValueError, match="quantity_required"):
            _validate_plain_order_contract({"type": "MARKET"}, position_mode="oneway")

    def test_limit_requires_price_and_tif(self):
        with pytest.raises(ValueError, match="must_be_positive|timeInForce"):
            _validate_plain_order_contract(
                {"type": "LIMIT", "quantity": 0.01, "price": 0, "timeInForce": "GTC"},
                position_mode="oneway",
            )


# ===========================================================================
# Validator — algo order
# ===========================================================================

class TestAlgoOrderContract:
    """P0: _validate_algo_order_contract.""",

    def test_valid_stop_market(self):
        res = _validate_algo_order_contract(
            {"type": "STOP_MARKET", "quantity": 0.01, "triggerPrice": 29000, "side": "SELL"},
            position_mode="oneway",
        ),
        assert res["workingType"] in ("MARK_PRICE", "CONTRACT_PRICE")

    def test_close_position_with_quantity_raises(self):
        """algo_closePosition_incompatible_with_quantity.""",
        with pytest.raises(ValueError, match="algo_closePosition_incompatible_with_quantity"):
            _validate_algo_order_contract(
                {
                    "type": "STOP_MARKET",
                    "quantity": 0.01,
                    "triggerPrice": 29000,
                    "closePosition": True,
                },
                position_mode="oneway",
            )

    def test_close_position_with_reduce_only_raises(self):
        """algo_closePosition_incompatible_with_reduceOnly."""
        with pytest.raises(ValueError, match="algo_closePosition_incompatible_with_reduceOnly"):
            _validate_algo_order_contract(
                {
                    "type": "STOP_MARKET",
                    "triggerPrice": 29000,
                    "closePosition": True,
                    "reduceOnly": True,
                },
                position_mode="oneway",
            )

    def test_bad_activate_price_raises(self):
        """Trailing activatePrice must be positive when present."""
        with pytest.raises(ValueError, match="activatePrice_must_be_positive"):
            _validate_algo_order_contract(
                {
                    "type": "TRAILING_STOP_MARKET",
                    "quantity": 0.01,
                    "callbackRate": 0.5,
                    "activatePrice": -1,
                },
                position_mode="oneway",
            )

    def test_bad_callback_rate_raises(self):
        with pytest.raises(ValueError, match="callbackRate_out_of_range"):
            _validate_algo_order_contract(
                {
                    "type": "TRAILING_STOP_MARKET",
                    "quantity": 0.01,
                    "callbackRate": 99.0,  # > 10.0
                },
                position_mode="oneway",
            )

    def test_normalises_working_type(self):
        res = _validate_algo_order_contract(
            {
                "type": "STOP_MARKET",
                "quantity": 0.01,
                "triggerPrice": 29000,
                "workingType": "mark_price",  # lowercase → normalised
            },
            position_mode="oneway",
        )
        assert res["workingType"] == "MARK_PRICE"

    def test_invalid_working_type_raises(self):
        with pytest.raises(ValueError, match="invalid_workingType"):
            _validate_algo_order_contract(
                {
                    "type": "STOP_MARKET",
                    "quantity": 0.01,
                    "triggerPrice": 29000,
                    "workingType": "SPOT_PRICE",
                },
                position_mode="oneway",
            )

    def test_hedge_requires_position_side(self):
        with pytest.raises(ValueError, match="positionSide_required_in_hedge"):
            _validate_algo_order_contract(
                {"type": "STOP_MARKET", "quantity": 0.01, "triggerPrice": 29000},
                position_mode="hedge",
            )


# ===========================================================================
# replace_untriggered_algo_order
# ===========================================================================

class TestReplaceUntriggeredAlgoOrder:
    """P0: cancel-then-post replace."""

    def _make_client(self, status="NEW"):
        """Return a mock BinanceFuturesClient with controllable behavior."""
        client = MagicMock(spec=BinanceFuturesClient)
        client.query_algo_order.return_value = {"status": status, "algoId": 9001}
        client.cancel_algo_order.return_value = {"result": "OK"}
        client.post_algo_order.return_value = {"algoId": 9002}
        # Let position_mode return oneway so validator passes
        client.position_mode.return_value = "oneway"
        return client

    def test_cancel_then_post_on_new_order(self):
        client = self._make_client(status="NEW")
        # We call the method through the actual class so validators run
        # (post_algo_order is mocked, so validator won't actually call Binance)
        with patch.object(
            BinanceFuturesClient, "__init__", lambda *a, **kw: None
        ):
            inst = BinanceFuturesClient.__new__(BinanceFuturesClient)
            inst.query_algo_order = client.query_algo_order
            inst.cancel_algo_order = client.cancel_algo_order
            inst.post_algo_order = client.post_algo_order
            inst.position_mode = client.position_mode

            new_params = {
                "type": "STOP_MARKET",
                "quantity": 0.01,
                "triggerPrice": 28000,
                "side": "BUY",
            }
            result = inst.replace_untriggered_algo_order(
                "BTCUSDT", algo_id=9001, new_params=new_params
            )
        client.cancel_algo_order.assert_called_once()
        client.post_algo_order.assert_called_once()
        assert result == {"algoId": 9002}

    def test_triggered_order_raises(self):
        client = self._make_client(status="FILLED")
        with patch.object(
            BinanceFuturesClient, "__init__", lambda *a, **kw: None
        ):
            inst = BinanceFuturesClient.__new__(BinanceFuturesClient)
            inst.query_algo_order = client.query_algo_order
            inst.cancel_algo_order = client.cancel_algo_order
            inst.post_algo_order = client.post_algo_order
            inst.position_mode = client.position_mode

            with pytest.raises(RuntimeError, match="not_replaceable"):
                inst.replace_untriggered_algo_order(
                    "BTCUSDT",
                    algo_id=9001,
                    new_params={"type": "STOP_MARKET", "quantity": 0.01, "triggerPrice": 28000},
                )


# ===========================================================================
# Leverage tier resolver
# ===========================================================================

class TestLeverageTierResolver:
    """P0: _symbol_tier / _resolve_symbol_leverage on BinanceExecutor."""

    def _make_executor(self, env_overrides=None):
        r = MagicMock()
        prod = MagicMock(spec=BinanceFuturesClient)
        with patch.dict(os.environ, env_overrides or {}, clear=False):
            ex = BinanceExecutor(redis_client=r, prod_client=prod)
        return ex

    def test_btcusdt_is_tier_a(self):
        ex = self._make_executor()
        assert ex._symbol_tier("BTCUSDT") == "A"

    def test_solusdt_is_tier_b(self):
        ex = self._make_executor()
        assert ex._symbol_tier("SOLUSDT") == "B"

    def test_pepe_is_tier_c(self):
        ex = self._make_executor()
        assert ex._symbol_tier("PEPEUSDT") == "C"

    def test_tier_a_uses_tier_env(self):
        ex = self._make_executor()
        with patch.dict(os.environ, {"BINANCE_LEVERAGE_TIER_A": "15"}, clear=False):
            lev = ex._resolve_symbol_leverage("BTCUSDT")
        assert lev == 15

    def test_per_symbol_override_takes_precedence(self):
        ex = self._make_executor()
        with patch.dict(os.environ, {"BINANCE_LEVERAGE_BTCUSDT": "8"}, clear=False):
            lev = ex._resolve_symbol_leverage("BTCUSDT")
        assert lev == 8

    def test_tier_c_default_is_5(self):
        ex = self._make_executor()
        lev = ex._resolve_symbol_leverage("PEPEUSDT")
        assert lev == 5

    def test_default_leverage_is_20_not_100(self):
        ex = self._make_executor()
        assert ex.default_leverage == 20


# ===========================================================================
# Resize target normalization
# ===========================================================================

class TestResizeTargetNormalization:
    """P0: _normalize_resize_target on BinanceExecutor."""

    def _make_executor(self):
        r = MagicMock()
        prod = MagicMock(spec=BinanceFuturesClient)
        ex = BinanceExecutor(redis_client=r, prod_client=prod)
        return ex

    def test_delta_qty_positive(self):
        ex = self._make_executor()
        mode, delta, target = ex._normalize_resize_target(
            1.0, {"resize_mode": "delta_qty", "delta_qty": 0.5}
        )
        assert mode == "delta_qty"
        assert abs(delta - 0.5) < 1e-9
        assert abs(target - 1.5) < 1e-9

    def test_delta_qty_negative(self):
        ex = self._make_executor()
        mode, delta, target = ex._normalize_resize_target(
            1.0, {"resize_mode": "delta_qty", "delta_qty": -0.3}
        )
        assert mode == "delta_qty"
        assert abs(delta - (-0.3)) < 1e-9
        assert abs(target - 0.7) < 1e-9

    def test_target_qty_mode(self):
        ex = self._make_executor()
        mode, delta, target = ex._normalize_resize_target(
            1.0, {"resize_mode": "target_qty", "target_qty": 2.0}
        )
        assert mode == "target_qty"
        assert abs(delta - 1.0) < 1e-9
        assert abs(target - 2.0) < 1e-9

    def test_infers_target_qty_from_target_qty_field(self):
        ex = self._make_executor()
        # No resize_mode field, but target_qty present → infer target_qty mode
        mode, delta, target = ex._normalize_resize_target(1.0, {"target_qty": 0.5})
        assert mode == "target_qty"
        assert abs(target - 0.5) < 1e-9

    def test_invalid_mode_raises(self):
        ex = self._make_executor()
        with pytest.raises(ValueError, match="unsupported_resize_mode"):
            ex._normalize_resize_target(1.0, {"resize_mode": "bad_mode"})


# ===========================================================================
# Structured state contract
# ===========================================================================

class TestStructuredOrderContract:
    """P0: _structured_order_contract and _causal_timestamps."""

    def _make_executor(self):
        r = MagicMock()
        prod = MagicMock(spec=BinanceFuturesClient)
        return BinanceExecutor(redis_client=r, prod_client=prod)

    def test_structured_contract_with_entry_and_sl(self):
        ex = self._make_executor()
        entry = PlainOrderRef(
            order_id=1001, client_order_id="entry-cid", type="MARKET", side="BUY", position_side=None
        )
        sl = AlgoOrderRef(
            algo_id=2001, client_algo_id="sl-cid", type="STOP_MARKET", working_type="MARK_PRICE"
        )
        contract = ex._structured_order_contract(sid="test-sid", entry_ref=entry, sl_ref=sl)
        assert contract["sid"] == "test-sid"
        assert contract["entry"]["order_id"] == 1001
        assert contract["protective"]["sl_algo_id"] == 2001

    def test_structured_contract_with_tps(self):
        ex = self._make_executor()
        tps = [
            AlgoOrderRef(algo_id=3001, client_algo_id="tp1", type="TAKE_PROFIT_MARKET", working_type="MARK_PRICE"),
            AlgoOrderRef(algo_id=3002, client_algo_id="tp2", type="TAKE_PROFIT_MARKET", working_type="MARK_PRICE")]
        contract = ex._structured_order_contract(sid="test-sid", tp_refs=tps)
        assert contract["protective"]["tp_algo_ids"] == [3001, 3002]

    def test_trailing_included_in_contract(self):
        ex = self._make_executor()
        trail = AlgoOrderRef(algo_id=4001, client_algo_id="trail-cid", type="TRAILING_STOP_MARKET", working_type="MARK_PRICE")
        contract = ex._structured_order_contract(sid="test-sid", trail_ref=trail)
        assert contract["trailing"]["trail_algo_id"] == 4001

    def test_causal_timestamps_from_payload(self):
        ex = self._make_executor()
        payload = {"ts_event_ms": 1700000000000, "ts_queue_ms": 1700000000100}
        ts = ex._causal_timestamps(payload)
        assert ts["ts_event_ms"] == 1700000000000
        assert ts["ts_queue_ms"] == 1700000000100
        assert "ts_exec_start_ms" in ts

    def test_causal_timestamps_fallback_to_now(self):
        ex = self._make_executor()
        ts = ex._causal_timestamps({})
        now = get_ny_time_millis()
        assert abs(ts["ts_event_ms"] - now) < 5000  # within 5 seconds
        assert abs(ts["ts_exec_start_ms"] - now) < 5000
