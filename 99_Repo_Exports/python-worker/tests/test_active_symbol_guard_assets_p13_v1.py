from __future__ import annotations

"""P13: General asset tests — metrics availability, diagnostics enrichment, CLI args."""

import json
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Metrics exist
# ---------------------------------------------------------------------------

class TestP13MetricsExist:
    def test_runbook_state_total_exported(self):
        try:
            from services.execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL
        except Exception:
            from execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL  # type: ignore
        assert EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL is not None

    def test_renew_reminder_total_exported(self):
        try:
            from services.execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL
        except Exception:
            from execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL  # type: ignore
        assert EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL is not None

    def test_runbook_action_total_still_exported(self):
        try:
            from services.execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL
        except Exception:
            from execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL  # type: ignore
        assert EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL is not None

    def test_runbook_audit_total_still_exported(self):
        try:
            from services.execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL
        except Exception:
            from execution_metrics import EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL  # type: ignore
        assert EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL is not None


# ---------------------------------------------------------------------------
# Diagnostics: operator_dashboard structure
# ---------------------------------------------------------------------------

class TestDiagnosticsP13:
    def _make_diag(self, keys: dict | None = None, stream_entries: list | None = None):
        try:
            from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
        except Exception:
            from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore

        r = MagicMock()
        _keys = keys or {}
        _entries = stream_entries or []

        def _get(key):
            v = _keys.get(str(key))
            return json.dumps(v).encode() if v is not None else None

        r.get = MagicMock(side_effect=_get)
        r.scan_iter = MagicMock(return_value=list(_keys.keys()))
        r.keys = MagicMock(return_value=list(_keys.keys()))
        r.xrevrange = MagicMock(return_value=_entries)

        store = MagicMock()
        store.list_symbols.return_value = []
        store.top_conflict_symbols.return_value = []
        store.top_resurrection_symbols.return_value = []
        store.get_conflict_counts.return_value = {}
        store.get_resurrection_counts.return_value = {}
        store.get_latest_conflict_meta.return_value = {}
        store.get_latest_resurrection_meta.return_value = {}
        store.get_symbol_timeline.return_value = []
        store.rolling_hot_symbols.return_value = []
        store.reset_window_hot_metric = MagicMock()

        diag = ActiveSymbolGuardDiagnostics(r)
        diag.store = store
        return diag

    def test_operator_dashboard_structure(self):
        diag = self._make_diag()
        dash = diag.operator_dashboard()
        assert 'active_holds' in dash
        assert 'active_acks' in dash
        assert 'recent_audit' in dash
        assert 'top_operators' in dash
        assert 'top_tickets' in dash
        assert 'generated_at_ms' in dash

    def test_runbook_history_empty_stream(self):
        diag = self._make_diag()
        assert diag.runbook_history() == []

    def test_linked_tickets_empty(self):
        diag = self._make_diag()
        assert diag.linked_tickets() == []

    def test_snapshot_has_runbook_dashboard_summary(self):
        diag = self._make_diag()
        snap = diag.snapshot()
        assert 'runbook_dashboard_summary' in snap
        rds = snap['runbook_dashboard_summary']
        assert 'active_holds' in rds
        assert 'active_acks' in rds

    def test_debug_symbol_has_runbook_key(self):
        diag = self._make_diag()
        try:
            pass
        except Exception:
            pass  # type: ignore
        diag.store.load_raw = MagicMock(return_value={})
        result = diag.debug_symbol('BTCUSDT')
        assert 'runbook' in result
        assert 'hold' in result['runbook']
        assert 'ticket_history' in result['runbook']

    def test_debug_sid_has_runbook_key(self):
        diag = self._make_diag()
        diag.store.load_raw = MagicMock(return_value={})
        result = diag.debug_sid('sid-test')
        assert 'runbook' in result


# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------

class TestCLIArgs:
    def test_dashboard_arg_present(self):
        try:
            from services.active_symbol_guard_cli import _build_arg_parser
        except Exception:
            from active_symbol_guard_cli import _build_arg_parser  # type: ignore
        p = _build_arg_parser()
        # Parse --dashboard, should not raise
        args = p.parse_args(['--dashboard'])
        assert bool(args.dashboard)

    def test_ticket_history_arg_present(self):
        try:
            from services.active_symbol_guard_cli import _build_arg_parser
        except Exception:
            from active_symbol_guard_cli import _build_arg_parser  # type: ignore
        p = _build_arg_parser()
        args = p.parse_args(['--ticket-history', 'TKT-001'])
        assert args.ticket_history == 'TKT-001'
