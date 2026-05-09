"""P5: Tests for BinanceExecutor active-symbol guard exchange-truth release.

Scenarios:
  P5-1: guard released when exchange confirms flat + no orders
  P5-2: guard stays and annotates key when exchange shows live position
  P5-3: guard stays when exchange shows open plain orders only
  P5-4: guard stays and annotates exchange_check_error when API fails
  P5-5: no exchange check when exchange_truth_release disabled (legacy path)
"""
import importlib.util
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

mod_path = root / "services" / "binance_executor.py"
spec = importlib.util.spec_from_file_location("services.binance_executor_p5", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def delete(self, key):
        self.kv.pop(key, None)


class FlatClient:
    """Binance client stub: symbol is completely flat."""
    def get_position_risk(self):
        return [{"symbol": "BTCUSDT", "positionAmt": "0"}]

    def get_open_orders(self, symbol=None):
        return []

    def get_open_algo_orders(self, symbol=None):
        return []


class LivePositionClient:
    """Binance client stub: ETHUSDT has live position."""
    def get_position_risk(self):
        return [{"symbol": "ETHUSDT", "positionAmt": "2"}]

    def get_open_orders(self, symbol=None):
        return []

    def get_open_algo_orders(self, symbol=None):
        return []


class OpenOrdersOnlyClient:
    """Binance client stub: SOLUSDT has no position but has open orders."""
    def get_position_risk(self):
        return [{"symbol": "SOLUSDT", "positionAmt": "0"}]

    def get_open_orders(self, symbol=None):
        return [{"symbol": "SOLUSDT", "orderId": 99}]

    def get_open_algo_orders(self, symbol=None):
        return []


class BrokenClient:
    """Binance client stub: all API calls raise."""
    def get_position_risk(self):
        raise RuntimeError("network error")

    def get_open_orders(self, symbol=None):
        raise RuntimeError("network error")

    def get_open_algo_orders(self, symbol=None):
        raise RuntimeError("network error")


def _mk_exec(r, exchange_truth_release=True):
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = r
    ex.exec_single_active_position_per_symbol = True
    ex.exec_single_active_position_release_on_terminal = True
    ex.exec_single_active_position_stale_timeout_ms = 900000
    ex.exec_single_active_position_exchange_truth_release = exchange_truth_release
    ex.exec_single_active_position_guard_repair_enable = True
    ex.exec_single_active_position_require_flat_no_orders = True
    ex.exec_active_symbol_user_stream_stale_ms = 30000
    ex.active_symbol_key_prefix = "orders:active_symbol_sid:"
    ex.active_symbol_guard_tombstone_ttl_sec = 120
    ex.user_stream_status_key = "orders:user_stream:status"
    ex.exec_journal_primary = True
    ex.exec_state_derived_view = True
    ex.exec_inline_state_projection = False
    ex.state_key_prefix = "orders:state:"
    ex.state_ttl = 86400
    return ex


# ---------------------------------------------------------------------------
# P5-1: guard released when exchange confirms flat + no orders
# ---------------------------------------------------------------------------

def test_guard_released_when_exchange_flat_and_no_orders():
    """When exchange shows positionAmt=0 and no orders, guard is cleared immediately."""
    r = FakeRedis()
    r.set("orders:active_symbol_sid:BTCUSDT", json.dumps({
        "symbol": "BTCUSDT",
        "sid": "sid-old",
        "fsm_state": "PROTECTED",
        "updated_at_ms": 1700000000000,
    }))
    r.set("orders:user_stream:status", json.dumps({
        "connected": False,
        "updated_at_ms": 1700000000000,
    }))
    ex = _mk_exec(r)
    ex._load_order_state = lambda sid: {
        "sid": sid, "fsm_state": "EXIT_FILLED", "status": "closed", "closed": True,
    }
    # Should NOT raise — guard should be cleared
    ex._guard_single_active_symbol_open(sid="sid-new", symbol="BTCUSDT", client=FlatClient())
    raw = json.loads(r.get("orders:active_symbol_sid:BTCUSDT"))
    assert raw['guard_status'] == 'released'
    assert ex._load_active_symbol_guard('BTCUSDT') == {}


# ---------------------------------------------------------------------------
# P5-2: guard stays and annotates key when exchange shows live position
# ---------------------------------------------------------------------------

def test_guard_stays_and_annotates_key_when_exchange_shows_live_position():
    """When exchange shows live position, guard must NOT be released."""
    r = FakeRedis()
    r.set("orders:active_symbol_sid:ETHUSDT", json.dumps({
        "symbol": "ETHUSDT",
        "sid": "sid-old",
        "fsm_state": "EXIT_FILLED",
        "updated_at_ms": 1700000000000,
    }))
    r.set("orders:user_stream:status", json.dumps({
        "connected": False,
        "updated_at_ms": 1700000000000,
    }))
    ex = _mk_exec(r)
    ex._load_order_state = lambda sid: {
        "sid": sid, "fsm_state": "EXIT_FILLED", "status": "closed", "closed": True,
    }
    try:
        ex._guard_single_active_symbol_open(sid="sid-new", symbol="ETHUSDT", client=LivePositionClient())
        assert False, "expected OpenBlockedByActiveSymbolError"
    except mod.OpenBlockedByActiveSymbolError as err:
        assert err.details["blocked_by_sid"] == "sid-old"
        assert err.details["exchange_position_amt"] == 2.0
        # Guard key must be annotated with exchange snapshot
        guard = json.loads(r.get("orders:active_symbol_sid:ETHUSDT"))
        assert guard["exchange_guard_reason"] == "exchange_open_position"
        assert guard["exchange_position_amt"] == 2.0


# ---------------------------------------------------------------------------
# P5-3: guard stays when exchange shows open plain orders (position flat)
# ---------------------------------------------------------------------------

def test_guard_stays_when_exchange_shows_open_orders():
    """Even if positionAmt is 0, open orders must prevent guard release."""
    r = FakeRedis()
    r.set("orders:active_symbol_sid:SOLUSDT", json.dumps({
        "symbol": "SOLUSDT",
        "sid": "sid-old",
        "fsm_state": "EXIT_FILLED",
        "updated_at_ms": 1700000000000,
    }))
    r.set("orders:user_stream:status", json.dumps({"connected": True, "last_event_ms": 1700000000000}))
    ex = _mk_exec(r)
    ex._load_order_state = lambda sid: {
        "sid": sid, "fsm_state": "EXIT_FILLED", "status": "closed", "closed": True,
    }
    try:
        ex._guard_single_active_symbol_open(sid="sid-new", symbol="SOLUSDT", client=OpenOrdersOnlyClient())
        assert False, "expected OpenBlockedByActiveSymbolError"
    except mod.OpenBlockedByActiveSymbolError as err:
        assert err.details["exchange_open_plain_orders"] == 1
        guard = json.loads(r.get("orders:active_symbol_sid:SOLUSDT"))
        assert guard["exchange_guard_reason"] == "exchange_open_orders"


# ---------------------------------------------------------------------------
# P5-4: guard stays and annotates exchange_check_error when API fails
# ---------------------------------------------------------------------------

def test_guard_stays_and_marks_error_when_exchange_api_fails():
    """When Binance API raises, guard is conservatively kept and annotated."""
    r = FakeRedis()
    r.set("orders:active_symbol_sid:BTCUSDT", json.dumps({
        "symbol": "BTCUSDT",
        "sid": "sid-old",
        "fsm_state": "PROTECTED",
        "updated_at_ms": 1700000000000,
    }))
    r.set("orders:user_stream:status", json.dumps({"connected": True, "last_event_ms": 1700000000000}))
    ex = _mk_exec(r)
    ex._load_order_state = lambda sid: {"sid": sid, "fsm_state": "PROTECTED"}
    try:
        ex._guard_single_active_symbol_open(sid="sid-new", symbol="BTCUSDT", client=BrokenClient())
        assert False, "expected OpenBlockedByActiveSymbolError"
    except mod.OpenBlockedByActiveSymbolError as err:
        assert len(err.details["exchange_truth_errors"]) > 0
        guard = json.loads(r.get("orders:active_symbol_sid:BTCUSDT"))
        assert guard["exchange_guard_reason"] == "exchange_check_error"


# ---------------------------------------------------------------------------
# P5-5: legacy path (exchange_truth_release disabled)
# ---------------------------------------------------------------------------

def test_legacy_terminal_release_when_exchange_truth_disabled():
    """When exchange-truth release is off, terminal FSM state alone clears guard."""
    r = FakeRedis()
    r.set("orders:active_symbol_sid:BTCUSDT", json.dumps({
        "symbol": "BTCUSDT",
        "sid": "sid-old",
        "fsm_state": "PROTECTED",
        "updated_at_ms": 1700000000000,
    }))
    ex = _mk_exec(r, exchange_truth_release=False)
    ex._load_order_state = lambda sid: {
        "sid": sid, "fsm_state": "EXIT_FILLED", "status": "closed", "closed": True,
    }
    # Should NOT raise (terminal state clears guard without exchange check)
    ex._guard_single_active_symbol_open(sid="sid-new", symbol="BTCUSDT")
    raw = json.loads(r.get("orders:active_symbol_sid:BTCUSDT"))
    assert raw['guard_status'] == 'released'
    assert ex._load_active_symbol_guard('BTCUSDT') == {}
