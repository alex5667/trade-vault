"""Tests for services/execution_journal.py — P3/P4 SQL execution journal sink."""
from pathlib import Path
import importlib.util
import sys

# Load the module directly to avoid import chain dependencies
mod_path = (Path(__file__).parent.parent / "services" / "execution_journal.py").resolve()
spec = importlib.util.spec_from_file_location("execution_journal", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

# Disable the shared pool so FakeConn/connect_factory fallback path is used in all tests.
# (analytics_db may not be importable in isolated test environments.)
mod._get_shared_conn = None  # type: ignore[attr-defined]


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.log.append((sql, list(params)))


class FakeConn:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        pass  # no-op in tests

    def close(self):
        pass  # no-op in tests


def _factory(log):
    return lambda dsn, **kw: FakeConn(log)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_sink_disabled_if_no_dsn():
    """With no DSN, the sink must be disabled and all methods return False."""
    sink = mod.ExecutionJournalSink(dsn="", connect_factory=None)
    assert not sink.enabled
    assert sink.record_event({"sid": "x"}) is False
    assert sink.upsert_order_snapshot({"sid": "x"}) is False
    assert sink.upsert_protection_refs({"sid": "x"}) is False


def test_record_event_executes_correct_sql():
    """record_event must INSERT into execution_order_events with correct params.

    SQL param order (updated after P5):
      (sid, symbol, signal_id, execution_plan_id, event_type, event_ts_ms, payload_jsonb)
    """
    log = []
    sink = mod.ExecutionJournalSink(dsn="postgres://unit-test", connect_factory=_factory(log))
    result = sink.record_event({
        "sid": "sid-1", "symbol": "BTCUSDT",
        "event_type": "state_transition", "ts_ms": 1_700_000_000_000
    })
    assert result is True
    assert len(log) == 1
    sql, params = log[0]
    assert "execution_order_events" in sql
    assert params[0] == "sid-1"       # sid
    assert params[1] == "BTCUSDT"     # symbol
    assert params[4] == "state_transition"  # event_type (index 4, after signal_id, exec_plan_id)


def test_upsert_order_snapshot_executes_upsert_sql():
    """upsert_order_snapshot must use ON CONFLICT DO UPDATE for execution_orders."""
    log = []
    sink = mod.ExecutionJournalSink(dsn="postgres://unit-test", connect_factory=_factory(log))
    result = sink.upsert_order_snapshot({
        "sid": "sid-1", "symbol": "BTCUSDT",
        "fsm_state": "PROTECTED", "action": "open", "ts_ms": 2
    })
    assert result is True
    assert len(log) == 1
    sql, params = log[0]
    assert "execution_orders" in sql
    assert "ON CONFLICT" in sql
    assert params[0] == "sid-1"


def test_upsert_protection_refs_executes_correct_sql():
    """upsert_protection_refs must insert into execution_protection_refs."""
    log = []
    sink = mod.ExecutionJournalSink(dsn="postgres://unit-test", connect_factory=_factory(log))
    result = sink.upsert_protection_refs({
        "sid": "sid-1", "symbol": "BTCUSDT",
        "sl_algo_id": 999, "tp1_algo_id": 1001
    })
    assert result is True
    assert "execution_protection_refs" in log[0][0]


def test_record_event_and_snapshot_round_trip():
    """Two writes in sequence must produce two SQL calls."""
    log = []
    sink = mod.ExecutionJournalSink(dsn="postgres://unit-test", connect_factory=_factory(log))
    sink.record_event({"sid": "sid-2", "symbol": "ETHUSDT", "event_type": "fill", "ts_ms": 10})
    sink.upsert_order_snapshot({"sid": "sid-2", "symbol": "ETHUSDT", "fsm_state": "EXIT_FILLED", "ts_ms": 11})
    assert len(log) == 2
    assert "execution_order_events" in log[0][0]
    assert "execution_orders" in log[1][0]


def test_connection_failure_returns_false():
    """If the connection factory raises, record_event must return False (fail-open)."""
    def bad_factory(dsn):
        raise ConnectionError("cannot connect")

    sink = mod.ExecutionJournalSink(dsn="postgres://bad", connect_factory=bad_factory)
    assert sink.record_event({"sid": "x"}) is False


def test_upsert_refs_skipped_when_no_sid():
    """upsert_protection_refs must return False immediately when sid is empty."""
    log = []
    sink = mod.ExecutionJournalSink(dsn="postgres://unit-test", connect_factory=_factory(log))
    result = sink.upsert_protection_refs({"symbol": "BTCUSDT"})
    assert result is False
    assert len(log) == 0
