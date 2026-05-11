"""Tests for risk_audit_sql: verifies the SQL sink writes to both tables (P4.4/P4.5)."""
import importlib.util
import sys
from pathlib import Path

# Load the module standalone so tests work without the full services package
mod_path = Path(__file__).resolve().parent / 'risk_audit_sql.py'
spec = importlib.util.spec_from_file_location('risk_audit_sql', mod_path)
mod = importlib.util.module_from_spec(spec)  # type: ignore
sys.modules[spec.name] = mod  # type: ignore
spec.loader.exec_module(mod)  # type: ignore


class _Cur:
    """Minimal psycopg cursor stub that records execute calls."""
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Conn:
    """Minimal psycopg connection stub."""
    def __init__(self):
        self.cur = _Cur()

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _make_sink():
    sink = mod.RiskAuditSqlSink(dsn='postgres://x', enabled=True)
    fake = _Conn()
    sink._connect = lambda: fake
    return sink, fake


class _Tier:
    name = 'A'


class _Decision:
    level = 'ALLOW'
    allow_trade_publish = True
    adjusted_notional_usd = 100.0
    leverage_cap = 10.0
    risk_multiplier = 1.0
    reasons = ['ok']
    snapshot = {'decision_latency_ms': 3, 'clamp_ratio': 1.0}
    tier_policy = _Tier()
    effective_execution_policy = 'SAFETY_FIRST'


class _Input:
    symbol = 'BTCUSDT'
    cluster = 'majors'
    requested_notional_usd = 100.0


def test_record_decision_writes_two_tables():
    """record_decision() must issue exactly 2 SQL execute calls (one per table)."""
    sink, fake = _make_sink()
    ok = sink.record_decision(
        decision_id='d1',
        signal={'symbol': 'BTCUSDT', 'sid': 's1'},
        risk_input=_Input(),
        risk_decision=_Decision(),
    )
    assert ok is True
    assert len(fake.cur.calls) == 2


def test_record_decision_disabled_returns_false():
    """When sink is disabled (no DSN), record_decision returns False without writing."""
    sink = mod.RiskAuditSqlSink(dsn='', enabled=False)
    ok = sink.record_decision(
        decision_id='d2',
        signal={'symbol': 'BTCUSDT'},
        risk_input=_Input(),
        risk_decision=_Decision(),
    )
    assert ok is False


def test_from_env_disabled_when_no_dsn(monkeypatch):
    """from_env() must disable the sink when RISK_AUDIT_SQL_DSN is empty."""
    monkeypatch.delenv('RISK_AUDIT_SQL_DSN', raising=False)
    monkeypatch.delenv('EXECUTION_JOURNAL_DSN', raising=False)
    sink = mod.RiskAuditSqlSink.from_env()
    assert sink.enabled is False


def test_record_decision_bad_conn_returns_false():
    """If _connect returns None, record_decision returns False gracefully."""
    sink = mod.RiskAuditSqlSink(dsn='postgres://x', enabled=True)
    sink._connect = lambda: None
    ok = sink.record_decision(
        decision_id='d3',
        signal={'symbol': 'BTCUSDT'},
        risk_input=_Input(),
        risk_decision=_Decision(),
    )
    assert ok is False
