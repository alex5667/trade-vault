"""Tests for P5 execution journal contract extension.

Verifies that ExecutionJournalSink correctly writes chain fields
(signal_id, execution_plan_id, entry_order_ref, exit_order_ref,
closed_trade_id, entry_policy, exit_policy) and that record_watchdog_event
works correctly.  Uses a minimal stub DB connection so no real Postgres is needed.
"""
from pathlib import Path
import importlib.util
import sys

# Load execution_journal without requiring the full package
mod_path = Path(__file__).resolve().parent.parent / 'services' / 'execution_journal.py'
spec = importlib.util.spec_from_file_location('execution_journal_p5', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


# ---------------------------------------------------------------------------
# Minimal stub DB connection
# ---------------------------------------------------------------------------

class _Cursor:
    def __init__(self, executed):
        self.executed = executed
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execute(self, sql, params):
        self.executed.append((sql, params))


class _Conn:
    def __init__(self, executed):
        self.executed = executed
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def cursor(self):
        return _Cursor(self.executed)


def _sink(executed):
    return mod.ExecutionJournalSink(
        dsn='postgres://x',
        connect_factory=lambda dsn: _Conn(executed)
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_upsert_order_snapshot_contains_chain_columns():
    """upsert_order_snapshot must write signal_id, closed_trade_id etc. in INSERT."""
    executed = []
    ok = _sink(executed).upsert_order_snapshot({
        'sid': 's1', 'symbol': 'BTCUSDT',
        'signal_id': 'sig-1', 'execution_plan_id': 'plan-1',
        'entry_order_ref': 'binance|entry|oid=1|cid=abc',
        'exit_order_ref': 'binance|exit|oid=2',
        'closed_trade_id': 'closed:s1:abc123',
        'entry_policy': 'ENTRY_MARKET_OR_SHORT_IOC',
        'exit_policy': 'SL_STOP_MARKET__TP_MARKET__TRAIL_OPTIONAL',
    })
    assert ok is True
    sql, params = executed[0]
    assert 'entry_policy' in sql, "entry_policy must be in INSERT"
    assert 'closed_trade_id' in sql, "closed_trade_id must be in INSERT"
    assert 'COALESCE' in sql, "COALESCE should protect chain refs on conflict"
    assert params[8] == 'sig-1',  f"signal_id param position wrong: {params}"
    assert params[9] == 'plan-1', f"execution_plan_id param position wrong: {params}"
    assert params[10] == 'binance|entry|oid=1|cid=abc'
    assert params[11] == 'binance|exit|oid=2'
    assert params[12] == 'closed:s1:abc123'


def test_record_watchdog_event_inserts_to_correct_table():
    """record_watchdog_event must write to execution_watchdog_events."""
    executed = []
    ok = _sink(executed).record_watchdog_event({
        'sid': 's1', 'symbol': 'BTCUSDT',
        'tp_level': 1, 'tp_state': 'TP1_TRIGGERED',
        'event_type': 'tp_watchdog',
    })
    assert ok is True
    sql, params = executed[0]
    assert 'execution_watchdog_events' in sql
    assert params[4] == 1, "tp_level must be params[4]"
    assert 'TP1_TRIGGERED' in params[5]


def test_record_event_includes_signal_chain_fields():
    """record_event must write signal_id and execution_plan_id."""
    executed = []
    ok = _sink(executed).record_event({
        'sid': 's1', 'symbol': 'BTCUSDT',
        'signal_id': 'sig-x', 'execution_plan_id': 'plan-x',
        'event_type': 'open',
    })
    assert ok is True
    sql, params = executed[0]
    assert 'signal_id' in sql
    assert params[2] == 'sig-x'
    assert params[3] == 'plan-x'


def test_helper_functions():
    """_s/_i/_optional_text/_first_text helpers must behave correctly."""
    assert mod._s(None) == ''
    assert mod._s('foo') == 'foo'
    assert mod._i(None) == 0
    assert mod._i('42') == 42
    assert mod._optional_text('') is None
    assert mod._optional_text('  hello  ') == 'hello'
    assert mod._first_text({'a': '', 'b': 'val'}, 'a', 'b') == 'val'
    assert mod._first_text({}, 'a', 'b') is None


def test_upsert_order_snapshot_fallback_signal_id_from_decision_id():
    """When signal_id is absent but decision_id is present, it must be used as fallback."""
    executed = []
    ok = _sink(executed).upsert_order_snapshot({
        'sid': 's2', 'symbol': 'ETHUSDT',
        'decision_id': 'dec-999',
    })
    assert ok is True
    _, params = executed[0]
    # signal_id should fall back to decision_id
    assert params[8] == 'dec-999'
    assert params[9] == 'dec-999'
