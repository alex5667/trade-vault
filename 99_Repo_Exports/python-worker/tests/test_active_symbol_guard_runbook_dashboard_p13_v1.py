from __future__ import annotations
"""P13: Tests for ActiveSymbolGuardRunbookExecutor dashboard features:
- runbook_dashboard
- active_holds  
- active_acks
- audit_history (filtered)
- linked_tickets
"""
from utils.time_utils import get_ny_time_millis

import json
import time
from collections import namedtuple
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

try:
    from services.active_symbol_guard_runbook import ActiveSymbolGuardRunbookExecutor
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
except Exception:
    from active_symbol_guard_runbook import ActiveSymbolGuardRunbookExecutor  # type: ignore
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore


def _ms_now() -> int:
    return get_ny_time_millis()


def _make_redis(keys: dict | None = None, stream_entries: list | None = None):
    r = MagicMock()
    _keys = keys or {}
    _entries = stream_entries or []

    def _get(key):
        v = _keys.get(str(key))
        return json.dumps(v).encode() if v is not None else None

    r.get = MagicMock(side_effect=_get)
    r.set = MagicMock(return_value=True)
    r.delete = MagicMock(return_value=1)
    r.xadd = MagicMock()
    r.xrevrange = MagicMock(return_value=_entries)
    r.scan_iter = MagicMock(return_value=list(_keys.keys()))
    r.keys = MagicMock(return_value=list(_keys.keys()))
    return r


def _make_executor(r, keys: dict | None = None, stream_entries: list | None = None):
    store = MagicMock()
    store.load_view = MagicMock(return_value={'sid': 'sid-1', 'symbol': 'BTCUSDT'})
    store.load_raw = MagicMock(return_value={'sid': 'sid-1', 'symbol': 'BTCUSDT'})
    diag = MagicMock(spec=ActiveSymbolGuardDiagnostics)
    policy = MagicMock(spec=ActiveSymbolGuardIncidentPolicyEngine)
    exc = ActiveSymbolGuardRunbookExecutor(r, diagnostics=diag, policy=policy, client=None)
    exc.store = store
    return exc


# ---------------------------------------------------------------------------
# Tests: active_holds
# ---------------------------------------------------------------------------

class TestActiveHolds:
    def test_returns_only_active_holds(self):
        now = _ms_now()
        hold_prefix = 'orders:active_symbol_guard:hold:symbol:'
        keys = {
            f'{hold_prefix}BTCUSDT': {'symbol': 'BTCUSDT', 'hold_status': 'active', 'ticket': 'TKT-1', 'expires_at_ms': now + 3600_000, 'updated_at_ms': now},
            f'{hold_prefix}ETHUSDT': {'symbol': 'ETHUSDT', 'hold_status': 'active', 'ticket': 'TKT-2', 'expires_at_ms': now - 1000, 'updated_at_ms': now},  # expired
        }
        r = _make_redis(keys=keys)
        exc = _make_executor(r)
        holds = exc.active_holds()
        syms = [h['symbol'] for h in holds]
        assert 'BTCUSDT' in syms
        assert 'ETHUSDT' not in syms

    def test_hold_limit_respected(self):
        now = _ms_now()
        pfx = 'orders:active_symbol_guard:hold:symbol:'
        keys = {f'{pfx}SYM{i}': {'symbol': f'SYM{i}', 'hold_status': 'active', 'expires_at_ms': now + 3600_000} for i in range(20)}
        r = _make_redis(keys=keys)
        exc = _make_executor(r)
        holds = exc.active_holds(limit=5)
        assert len(holds) <= 5


# ---------------------------------------------------------------------------
# Tests: active_acks
# ---------------------------------------------------------------------------

class TestActiveAcks:
    def test_returns_only_active_acks(self):
        now = _ms_now()
        pfx = 'orders:active_symbol_guard:incident:ack:'
        keys = {
            f'{pfx}fp1': {'fingerprint': 'fp1', 'expires_at_ms': now + 3600_000},
            f'{pfx}fp2': {'fingerprint': 'fp2', 'expires_at_ms': now - 1000},  # expired
        }
        r = _make_redis(keys=keys)
        exc = _make_executor(r)
        acks = exc.active_acks()
        fps = [a['fingerprint'] for a in acks]
        assert 'fp1' in fps
        assert 'fp2' not in fps


# ---------------------------------------------------------------------------
# Tests: audit_history
# ---------------------------------------------------------------------------

class TestAuditHistory:
    def _stream_entry(self, *, action='apply_hold_symbol', symbol='BTCUSDT', sid='sid-1', ticket='TKT-A', operator='alice', ts_ms: int | None = None):
        ts = ts_ms or _ms_now()
        fields = {
            b'action': action.encode(),
            b'symbol': symbol.encode(),
            b'sid': sid.encode(),
            b'ticket': ticket.encode(),
            b'operator': operator.encode(),
            b'ts_ms': str(ts).encode(),
            b'payload': b'{}',
        }
        entry_id = f'{ts}-0'.encode()
        return (entry_id, fields)

    def test_empty_stream_returns_empty(self):
        r = _make_redis(stream_entries=[])
        exc = _make_executor(r)
        assert exc.audit_history() == []

    def test_filter_by_symbol(self):
        entries = [
            self._stream_entry(symbol='BTCUSDT', action='apply_hold_symbol'),
            self._stream_entry(symbol='ETHUSDT', action='apply_hold_symbol'),
        ]
        r = _make_redis(stream_entries=entries)
        exc = _make_executor(r)
        result = exc.audit_history(symbol='BTCUSDT')
        assert all(d.get('symbol') == 'BTCUSDT' for d in result)

    def test_filter_by_ticket(self):
        entries = [
            self._stream_entry(ticket='TKT-X'),
            self._stream_entry(ticket='TKT-Y'),
        ]
        r = _make_redis(stream_entries=entries)
        exc = _make_executor(r)
        result = exc.audit_history(ticket='TKT-X')
        assert len(result) == 1
        assert result[0].get('ticket') == 'TKT-X'

    def test_filter_by_operator(self):
        entries = [
            self._stream_entry(operator='alice'),
            self._stream_entry(operator='bob'),
        ]
        r = _make_redis(stream_entries=entries)
        exc = _make_executor(r)
        result = exc.audit_history(operator='alice')
        assert all(d.get('operator') == 'alice' for d in result)

    def test_limit_respected(self):
        entries = [self._stream_entry() for _ in range(10)]
        r = _make_redis(stream_entries=entries)
        exc = _make_executor(r)
        result = exc.audit_history(limit=3)
        assert len(result) <= 3


# ---------------------------------------------------------------------------
# Tests: linked_tickets
# ---------------------------------------------------------------------------

class TestLinkedTickets:
    def test_returns_ticket_counts(self):
        now = _ms_now()
        entries = []
        for tkt in ['TKT-A', 'TKT-A', 'TKT-B']:
            fields = {b'ticket': tkt.encode(), b'symbol': b'BTCUSDT', b'action': b'apply_hold_symbol', b'ts_ms': str(now).encode(), b'payload': b'{}'}
            entries.append((str(now).encode(), fields))
        r = _make_redis(stream_entries=entries)
        exc = _make_executor(r)
        result = exc.linked_tickets(symbol='BTCUSDT')
        tkts = {item['ticket']: item['count'] for item in result}
        assert tkts.get('TKT-A') == 2
        assert tkts.get('TKT-B') == 1


# ---------------------------------------------------------------------------
# Tests: runbook_dashboard
# ---------------------------------------------------------------------------

class TestRunbookDashboard:
    def test_dashboard_structure(self):
        r = _make_redis()
        exc = _make_executor(r)
        dash = exc.runbook_dashboard()
        assert 'active_holds' in dash
        assert 'active_acks' in dash
        assert 'recent_audit' in dash
        assert 'top_operators' in dash
        assert 'top_tickets' in dash
        assert 'counts' in dash
        assert isinstance(dash['generated_at_ms'], int)

    def test_dashboard_counts(self):
        now = _ms_now()
        pfx_h = 'orders:active_symbol_guard:hold:symbol:'
        pfx_a = 'orders:active_symbol_guard:incident:ack:'
        keys = {
            f'{pfx_h}BTCUSDT': {'symbol': 'BTCUSDT', 'hold_status': 'active', 'expires_at_ms': now + 3600_000},
            f'{pfx_a}fp1': {'fingerprint': 'fp1', 'expires_at_ms': now + 3600_000},
        }
        r = _make_redis(keys=keys)
        exc = _make_executor(r)
        dash = exc.runbook_dashboard()
        assert dash['counts']['active_holds'] == 1
        assert dash['counts']['active_acks'] == 1
