from __future__ import annotations
"""P13: Tests for ActiveSymbolGuardIncidentNotifier with hold/ack-aware candidate selection
and decision field in run_once output.
"""
from utils.time_utils import get_ny_time_millis

import json
import time
from unittest.mock import MagicMock, patch

import pytest

try:
    from services.active_symbol_guard_incident_notifier import ActiveSymbolGuardIncidentNotifier
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
except Exception:
    from active_symbol_guard_incident_notifier import ActiveSymbolGuardIncidentNotifier  # type: ignore
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore


def _ms_now() -> int:
    return get_ny_time_millis()


def _make_notifier(
    snapshot: dict | None = None,
    dashboard: dict | None = None,
    triage_decision: str = 'notify',
):
    r = MagicMock()
    r.xadd = MagicMock()

    diag = MagicMock(spec=ActiveSymbolGuardDiagnostics)
    diag.snapshot.return_value = snapshot or {
        'guards': [], 'cas_conflict_hot_symbols': [], 'resurrection_hot_symbols': [],
        'heatmap': {'top_hot_symbols': {'5m': [], '1h': []}},
    }
    diag.operator_dashboard.return_value = dashboard or {
        'active_holds': [], 'active_acks': [],
    }

    policy = MagicMock(spec=ActiveSymbolGuardIncidentPolicyEngine)
    policy.triage_symbol.return_value = {
        'summary': {'symbol': 'BTCUSDT', 'severity': 'warning'},
        'policy': {'should_notify': triage_decision == 'notify', 'decision': triage_decision, 'fingerprint': 'fp1'},
        'telegram_text': '[test]',
    }
    policy.telegram_stream_fields.return_value = {'type': 'report', 'text': '[test]', 'decision': triage_decision}

    notifier = ActiveSymbolGuardIncidentNotifier(r, diag, policy)
    return notifier, diag, policy, r


class TestCandidateSymbolsP13:
    def test_active_holds_added_as_candidates(self):
        """Symbols with active holds from operator_dashboard are always evaluated."""
        dashboard = {
            'active_holds': [{'symbol': 'SOLUSDT'}],
            'active_acks': [],
        }
        notifier, diag, policy, r = _make_notifier(dashboard=dashboard)
        symbols = notifier._candidate_symbols()
        assert 'SOLUSDT' in symbols

    def test_active_acks_added_as_candidates(self):
        """Symbols with active acks from operator_dashboard are always evaluated."""
        dashboard = {
            'active_holds': [],
            'active_acks': [{'symbol': 'BNBUSDT', 'fingerprint': 'fp1'}],
        }
        notifier, diag, policy, r = _make_notifier(dashboard=dashboard)
        symbols = notifier._candidate_symbols()
        assert 'BNBUSDT' in symbols

    def test_released_tombstone_in_candidates(self):
        """released_tombstone classification (new in P13) is included in candidates."""
        snapshot = {
            'guards': [{'symbol': 'XRPUSDT', 'classification': 'released_tombstone'}],
            'cas_conflict_hot_symbols': [],
            'resurrection_hot_symbols': [],
            'heatmap': {'top_hot_symbols': {'5m': [], '1h': []}},
        }
        notifier, diag, policy, r = _make_notifier(snapshot=snapshot)
        symbols = notifier._candidate_symbols()
        assert 'XRPUSDT' in symbols

    def test_empty_candidates_when_all_clear(self):
        """When nothing is wrong, candidate list is empty."""
        notifier, diag, policy, r = _make_notifier()
        symbols = notifier._candidate_symbols()
        assert symbols == []


class TestRunOnceP13:
    def test_sent_includes_decision_field(self):
        """P13: each sent entry must include the decision field."""
        snapshot = {
            'guards': [{'symbol': 'BTCUSDT', 'classification': 'stale_tombstone'}],
            'cas_conflict_hot_symbols': [],
            'resurrection_hot_symbols': [],
            'heatmap': {'top_hot_symbols': {'5m': [], '1h': []}},
        }
        notifier, diag, policy, r = _make_notifier(snapshot=snapshot, triage_decision='notify')
        result = notifier.run_once()
        assert 'BTCUSDT' in [s['symbol'] for s in result.get('sent', [])]
        for item in result.get('sent', []):
            assert 'decision' in item

    def test_renew_reminder_decision_causes_notify(self):
        """P13: renew_reminder decision should_notify=True so it's captured in sent."""
        snapshot = {
            'guards': [{'symbol': 'BTCUSDT', 'classification': 'stale_tombstone'}],
            'cas_conflict_hot_symbols': [], 'resurrection_hot_symbols': [],
            'heatmap': {'top_hot_symbols': {'5m': [], '1h': []}},
        }
        notifier, diag, policy, r = _make_notifier(snapshot=snapshot, triage_decision='renew_reminder')
        policy.triage_symbol.return_value['policy']['should_notify'] = True
        result = notifier.run_once()
        sent = result.get('sent', [])
        assert any(s.get('decision') == 'renew_reminder' for s in sent)

    def test_acked_decision_goes_to_skipped(self):
        """P13: acked decision should_notify=False so it's captured in skipped."""
        snapshot = {
            'guards': [{'symbol': 'BTCUSDT', 'classification': 'stale_tombstone'}],
            'cas_conflict_hot_symbols': [], 'resurrection_hot_symbols': [],
            'heatmap': {'top_hot_symbols': {'5m': [], '1h': []}},
        }
        notifier, diag, policy, r = _make_notifier(snapshot=snapshot, triage_decision='acked')
        policy.triage_symbol.return_value['policy']['should_notify'] = False
        result = notifier.run_once()
        skipped = result.get('skipped', [])
        assert any(s.get('decision') == 'acked' for s in skipped)
