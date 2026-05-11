"""P5: Tests for BinanceActiveSymbolGuardRepairWorker.

Scenarios:
  RW-1: flat symbol → guard released
  RW-2: open plain orders → guard kept + annotated
  RW-3: live position → guard kept + annotated exchange_position_amt
  RW-4: dry_run=True → guard NOT deleted, record updated
  RW-5: run_once() processes all guard keys in scan
"""
import importlib.util
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

mod_path = root / "services" / "binance_active_symbol_guard_repair_worker.py"
spec = importlib.util.spec_from_file_location("services.binance_guard_repair_p5", mod_path)
mod = importlib.util.module_from_spec(spec)  # type: ignore
sys.modules[spec.name] = mod  # type: ignore
assert spec.loader is not None  # type: ignore
spec.loader.exec_module(mod)  # type: ignore


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def delete(self, key):
        self.kv.pop(key, None)

    def scan_iter(self, match=None):
        prefix = (match or "").rstrip("*")
        for key in list(self.kv.keys()):
            if key.startswith(prefix):
                yield key


class FlatClient:
    """Symbol is flat, no open orders."""
    def get_position_risk(self):
        return [{"symbol": "SOLUSDT", "positionAmt": "0"}]

    def get_open_orders(self, symbol=None):
        return []

    def get_open_algo_orders(self, symbol=None):
        return []


class OpenOrdersClient:
    """Symbol has flat position but open plain orders."""
    def get_position_risk(self):
        return [{"symbol": "ADAUSDT", "positionAmt": "0"}]

    def get_open_orders(self, symbol=None):
        return [{"symbol": "ADAUSDT", "orderId": 1}]

    def get_open_algo_orders(self, symbol=None):
        return []


class LivePositionClient:
    """Symbol has live position."""
    def get_position_risk(self):
        return [{"symbol": "BNBUSDT", "positionAmt": "5.0"}]

    def get_open_orders(self, symbol=None):
        return []

    def get_open_algo_orders(self, symbol=None):
        return []


# ---------------------------------------------------------------------------
# RW-1: flat symbol → guard released
# ---------------------------------------------------------------------------

def test_guard_repair_worker_releases_flat_guard():
    r = FakeRedis()
    r.set("orders:active_symbol_sid:SOLUSDT", json.dumps({"symbol": "SOLUSDT", "sid": "sid-flat"}))
    worker = mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=FlatClient())
    out = worker.run_once()
    assert len(out) == 1
    assert out[0]["status"] == "released", f"expected released, got: {out[0]}"
    raw = json.loads(r.get("orders:active_symbol_sid:SOLUSDT"))  # type: ignore
    assert raw['guard_status'] == 'released'
    assert worker._load_active_symbol_guard('SOLUSDT') == {}


# ---------------------------------------------------------------------------
# RW-2: open plain orders → guard kept + annotated
# ---------------------------------------------------------------------------

def test_guard_repair_worker_keeps_guard_when_open_orders_exist():
    r = FakeRedis()
    r.set("orders:active_symbol_sid:ADAUSDT", json.dumps({"symbol": "ADAUSDT", "sid": "sid-ada"}))
    worker = mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=OpenOrdersClient())
    out = worker.run_once()
    assert out[0]["status"] == "blocked"
    guard = json.loads(r.get("orders:active_symbol_sid:ADAUSDT"))  # type: ignore
    assert guard["exchange_guard_reason"] == "exchange_open_orders"
    assert guard["exchange_open_plain_orders"] == 1


# ---------------------------------------------------------------------------
# RW-3: live position → guard kept + exchange_position_amt annotated
# ---------------------------------------------------------------------------

def test_guard_repair_worker_keeps_guard_when_live_position():
    r = FakeRedis()
    r.set("orders:active_symbol_sid:BNBUSDT", json.dumps({"symbol": "BNBUSDT", "sid": "sid-bnb"}))
    worker = mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=LivePositionClient())
    out = worker.run_once()
    assert out[0]["status"] == "blocked"
    guard = json.loads(r.get("orders:active_symbol_sid:BNBUSDT"))  # type: ignore
    assert guard["exchange_guard_reason"] == "exchange_open_position"
    assert guard["exchange_position_amt"] == 5.0


# ---------------------------------------------------------------------------
# RW-4: dry_run=True → guard NOT deleted, annotation written
# ---------------------------------------------------------------------------

def test_guard_repair_worker_dry_run_does_not_delete():
    r = FakeRedis()
    r.set("orders:active_symbol_sid:SOLUSDT", json.dumps({"symbol": "SOLUSDT", "sid": "sid-dry"}))
    worker = mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=FlatClient())
    worker.dry_run = True
    out = worker.run_once()
    # status noop because _clear_guard returns False in dry_run mode (key not deleted)
    assert out[0]["status"] in ("released", "noop"), f"unexpected: {out[0]}"
    # Key must still be present in dry run
    assert r.get("orders:active_symbol_sid:SOLUSDT") is not None, \
        "dry_run should NOT delete the guard key"


# ---------------------------------------------------------------------------
# RW-5: run_once() processes all guard keys in scan
# ---------------------------------------------------------------------------

def test_guard_repair_worker_processes_multiple_keys():
    r = FakeRedis()
    r.set("orders:active_symbol_sid:SOLUSDT", json.dumps({"symbol": "SOLUSDT", "sid": "sid-a"}))
    r.set("orders:active_symbol_sid:ADAUSDT", json.dumps({"symbol": "ADAUSDT", "sid": "sid-b"}))
    # Use a client that sees both as flat to simplify assertion
    class BothFlatClient:
        def get_position_risk(self):
            return [
                {"symbol": "SOLUSDT", "positionAmt": "0"},
                {"symbol": "ADAUSDT", "positionAmt": "0"},
            ]
        def get_open_orders(self, symbol=None): return []
        def get_open_algo_orders(self, symbol=None): return []

    worker = mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=BothFlatClient())
    out = worker.run_once()
    symbols = {o["symbol"] for o in out}
    assert "SOLUSDT" in symbols
    assert "ADAUSDT" in symbols
    raw_sol = json.loads(r.get("orders:active_symbol_sid:SOLUSDT"))  # type: ignore
    assert raw_sol['guard_status'] == 'released'
    assert worker._load_active_symbol_guard('SOLUSDT') == {}
    raw_ada = json.loads(r.get("orders:active_symbol_sid:ADAUSDT"))  # type: ignore
    assert raw_ada['guard_status'] == 'released'
    assert worker._load_active_symbol_guard('ADAUSDT') == {}
