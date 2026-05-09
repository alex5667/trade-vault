"""P4: BinanceFuturesClient canonical contract tests.

Tests:
  1. inspect_protection_set flags orders with mismatched trigger prices even
     when the clientAlgoId is present (stale SL/TP detection).
  2. reconcile_protection_by_sid returns the richer P12/P4 contract shape
     with sid, symbol, by_client_algo_id, missing, expect_sl, expected_tp_count.
"""
import importlib.util
import sys
from pathlib import Path

_root = Path(__file__).resolve()
for _p in _root.parents:
    if (_p / "python-worker" / "services").is_dir():
        root = _p / "python-worker"
        break
else:
    root = Path(__file__).resolve().parents[2]

if str(root) not in sys.path:
    sys.path.insert(0, str(root))

mod_path = root / "services" / "binance_futures_client.py"
spec = importlib.util.spec_from_file_location("services.binance_futures_client_p4", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_inspect_protection_set_flags_mismatched_prices_even_when_ids_exist():
    """P4: SL at 95 (expected 94) and TP2 at 120 (expected 121) → is_complete=False.

    The clientAlgoIds ARE present in the open-orders list, so 'missing' is empty.
    But price mismatches produce 'mismatched' = ['sl', 'tp2'].
    """
    client = mod.BinanceFuturesClient.__new__(mod.BinanceFuturesClient)
    sid = "sid-mismatch-1"
    client.get_open_algo_orders = lambda symbol: [
        {"clientAlgoId": client._build_client_algo_id(sid, "sl"),  "triggerPrice": "95.0"},
        {"clientAlgoId": client._build_client_algo_id(sid, "tp1"), "triggerPrice": "110.0"},
        {"clientAlgoId": client._build_client_algo_id(sid, "tp2"), "triggerPrice": "120.0"},
    ]
    out = client.inspect_protection_set(
        "BTCUSDT",
        sid=sid,
        expected_tp_count=2,
        expect_sl=True,
        expected_sl_price=94.0,      # on-exchange price 95 ≠ 94 → mismatched
        expected_tp_prices=[110.0, 121.0],  # tp2 on-exchange 120 ≠ 121 → mismatched
    )
    assert out["is_complete"] is False, "Expected is_complete=False due to price mismatches"
    assert set(out["mismatched"]) == {"sl", "tp2"}, f"mismatched={out['mismatched']}"
    assert out["missing"] == [], f"missing should be empty, got {out['missing']}"


def test_inspect_protection_set_complete_when_all_prices_match():
    """P4: All prices within tolerance → is_complete=True, no mismatches."""
    client = mod.BinanceFuturesClient.__new__(mod.BinanceFuturesClient)
    sid = "sid-match-1"
    client.get_open_algo_orders = lambda symbol: [
        {"clientAlgoId": client._build_client_algo_id(sid, "sl"),  "triggerPrice": "94.0"},
        {"clientAlgoId": client._build_client_algo_id(sid, "tp1"), "triggerPrice": "110.0"},
    ]
    out = client.inspect_protection_set(
        "BTCUSDT",
        sid=sid,
        expect_sl=True,
        expected_sl_price=94.0,
        expected_tp_prices=[110.0],
    )
    assert out["is_complete"] is True
    assert out["missing"] == []
    assert out["mismatched"] == []


def test_reconcile_protection_by_sid_returns_canonical_contract_shape():
    """P4/P12: reconcile_protection_by_sid returns richer contract with sid/symbol/by_client_algo_id."""
    client = mod.BinanceFuturesClient.__new__(mod.BinanceFuturesClient)
    sid = "sid-canonical-1"
    client.get_open_algo_orders = lambda symbol: [
        {"clientAlgoId": client._build_client_algo_id(sid, "sl"),  "triggerPrice": "95.0"},
        {"clientAlgoId": client._build_client_algo_id(sid, "tp1"), "triggerPrice": "110.0"},
    ]
    out = client.reconcile_protection_by_sid("BTCUSDT", sid)
    assert out["sid"] == sid
    assert out["symbol"] == "BTCUSDT"
    assert "by_client_algo_id" in out
    assert "missing" in out
    assert "expect_sl" in out
    assert "expected_tp_count" in out
