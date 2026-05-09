"""P4: BinanceExecutor _verify_protection_on_exchange mismatch tests.

Verifies that _verify_protection_on_exchange correctly propagates the
is_complete=False / mismatched signals from the client's inspect_protection_set
when a stale protection contract is detected (clientAlgoId present but
trigger price outdated).

Note: _verify_protection_on_exchange accepts payload: Dict and state: Dict
(not bare expect_sl / tps), so tests must supply those accordingly.
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

mod_path = root / "services" / "binance_executor.py"
spec = importlib.util.spec_from_file_location("services.binance_executor_p4", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def _make_cid(sid: str, tag: str) -> str:
    """Match BinanceFuturesClient._build_client_algo_id logic for test fixtures."""
    import hashlib
    token = hashlib.sha1(str(sid).encode()).hexdigest()[:8]
    base = str(sid).replace(" ", "").replace(":", "-")
    base = base[: max(6, 36 - (len(tag) + len(token) + 2))]
    cid = f"{base}-{token}-{tag}"
    return cid[:36]


class DummyClientMismatched:
    """Stub client that reports tp1 as mismatched (stale trigger price)."""

    def inspect_protection_set(self, symbol, **kwargs):
        sid = kwargs.get("sid", "")
        tp1_cid = _make_cid(sid, "tp1")
        return {
            "sid": sid,
            "symbol": symbol,
            "is_complete": False,           # stale price → incomplete
            "missing": [],                  # id exists, just price wrong
            "mismatched": ["tp1"],          # P4: stale trigger price
            "tp_by_index": {1: {"algoId": 21, "clientAlgoId": tp1_cid, "triggerPrice": "111.0"}},
            "by_client_algo_id": {tp1_cid: {"algoId": 21, "triggerPrice": "111.0"}},
            "expect_sl": False,
            "expected_tp_count": 1,
        }


class DummyClientComplete:
    """Stub client that reports all protection present and prices correct."""

    def inspect_protection_set(self, symbol, **kwargs):
        return {
            "sid": kwargs.get("sid", ""),
            "symbol": symbol,
            "is_complete": True,
            "missing": [],
            "mismatched": [],
            "tp_by_index": {},
            "by_client_algo_id": {},
            "expect_sl": False,
            "expected_tp_count": 0,
        }


def test_verify_protection_on_exchange_rejects_stale_prices():
    """_verify_protection_on_exchange must propagate is_complete=False + mismatched from client."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    sid = "sid-stale-p4"
    # payload / state carry the expected sl/tp prices
    payload = {"sid": sid, "symbol": "BTCUSDT", "tp1": 110.0}
    state: dict = {}
    out = ex._verify_protection_on_exchange(
        sid=sid,
        symbol="BTCUSDT",
        payload=payload,
        state=state,
        client=DummyClientMismatched(),
    )
    assert out["is_complete"] is False, f"Expected is_complete=False, got {out}"
    assert out.get("mismatched") == ["tp1"], (
        f"Expected mismatched=['tp1'], got {out.get('mismatched')}"
    )


def test_verify_protection_on_exchange_complete_when_no_mismatch():
    """_verify_protection_on_exchange returns is_complete=True when client reports OK."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    sid = "sid-ok-p4"
    payload = {"sid": sid, "symbol": "BTCUSDT"}
    state: dict = {}
    out = ex._verify_protection_on_exchange(
        sid=sid,
        symbol="BTCUSDT",
        payload=payload,
        state=state,
        client=DummyClientComplete(),
    )
    assert out["is_complete"] is True
    assert out.get("mismatched", []) == []
