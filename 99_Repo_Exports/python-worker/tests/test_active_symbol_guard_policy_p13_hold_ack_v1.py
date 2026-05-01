from __future__ import annotations
"""P13: Tests for hold-aware triage and ack-aware suppression / renew-reminder
in ActiveSymbolGuardIncidentPolicyEngine.
"""
from utils.time_utils import get_ny_time_millis

import json
import time
from unittest.mock import MagicMock, patch

import pytest

try:
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
except Exception:
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ms_now() -> int:
    return get_ny_time_millis()


def _make_redis(*, hold_doc: dict | None = None, ack_doc: dict | None = None, suppress_doc: dict | None = None, dedupe_doc: dict | None = None):
    """Build a mock Redis client with configurable key returns."""
    r = MagicMock()

    def _get_side_effect(key: str):
        key = str(key)
        if hold_doc is not None and 'hold:symbol:' in key:
            return json.dumps(hold_doc).encode()
        if ack_doc is not None and 'incident:ack:' in key:
            return json.dumps(ack_doc).encode()
        if suppress_doc is not None and 'incident:suppress:' in key:
            return json.dumps(suppress_doc).encode()
        if dedupe_doc is not None and 'incident:dedupe:' in key:
            return json.dumps(dedupe_doc).encode()
        return None

    r.get = MagicMock(side_effect=_get_side_effect)
    r.xadd = MagicMock()
    r.set = MagicMock()
    r.scan_iter = MagicMock(return_value=[])
    r.xrevrange = MagicMock(return_value=[])
    return r


def _make_diag(r) -> ActiveSymbolGuardDiagnostics:
    diag = MagicMock(spec=ActiveSymbolGuardDiagnostics)
    return diag


def _make_bundle(symbol: str = 'BTCUSDT', sid: str = 'sid-1', classification: str = 'stale_tombstone', hot_5m: int = 6) -> dict:
    return {
        'summary': {'symbol': symbol, 'sid': sid, 'classification': classification, 'severity': 'info', 'score': 0, 'hotness': {'5m': hot_5m, '1h': 0}, 'race_chain_count': 0},
        'exchange_truth': {'has_live_position': True, 'has_open_orders': False, 'is_reliable': True, 'is_flat': False},
        'suspicious_writer_race_chains': [],
    }


def _engine(r, **kwargs) -> ActiveSymbolGuardIncidentPolicyEngine:
    diag = _make_diag(r)
    return ActiveSymbolGuardIncidentPolicyEngine(r, diag, **kwargs)


# ---------------------------------------------------------------------------
# Tests: hold state
# ---------------------------------------------------------------------------

class TestHoldAwareTriage:
    def test_no_hold_decision_notify(self):
        """When no hold is active, triage proceeds normally."""
        r = _make_redis()
        eng = _engine(r)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        policy = result['policy']
        decision = policy['decision']
        # without dedupe/suppress, should be notify
        assert decision == 'notify'
        assert bool(policy['should_notify'])

    def test_active_hold_does_not_suppress(self):
        """Active hold is counted in summary but does NOT itself suppress; it only enriches the runbook actions."""
        hold_doc = {
            'symbol': 'BTCUSDT',
            'hold_status': 'active',
            'ticket': 'TKT-001',
            'operator': 'alice',
            'expires_at_ms': _ms_now() + 3600_000,
        }
        r = _make_redis(hold_doc=hold_doc)
        eng = _engine(r)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        summary = result['summary']
        # hold_active should be annotated in summary
        assert bool(summary.get('hold_active'))
        assert summary.get('hold_ticket') == 'TKT-001'
        # hold alone doesn't suppress; decision is still notify
        assert result['policy']['decision'] == 'notify'

    def test_expired_hold_not_active(self):
        """An expired hold is treated as not active."""
        hold_doc = {
            'symbol': 'BTCUSDT',
            'hold_status': 'active',
            'ticket': 'TKT-OLD',
            'operator': 'alice',
            'expires_at_ms': _ms_now() - 1000,  # expired
        }
        r = _make_redis(hold_doc=hold_doc)
        eng = _engine(r)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        assert not bool(result['summary'].get('hold_active'))

    def test_revoke_action_offered_when_hold_active(self):
        """When hold is active, runbook actions include revoke_hold instead of hold_symbol."""
        hold_doc = {'symbol': 'BTCUSDT', 'hold_status': 'active', 'ticket': 'TKT-002', 'operator': 'bob', 'expires_at_ms': _ms_now() + 3600_000}
        r = _make_redis(hold_doc=hold_doc)
        eng = _engine(r)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        actions = {a['action'] for a in result['policy']['runbook_actions']}
        assert 'revoke_hold' in actions
        assert 'hold_symbol' not in actions


# ---------------------------------------------------------------------------
# Tests: ack-aware suppression
# ---------------------------------------------------------------------------

class TestAckAwareSuppression:
    def test_active_ack_suppresses_to_acked(self):
        """Active ack with remaining TTL results in decision=acked."""
        ack_doc = {
            'fingerprint': 'fp1',
            'symbol': 'BTCUSDT',
            'ticket': 'TKT-ACK',
            'acked_by': 'charlie',
            'expires_at_ms': _ms_now() + 7200_000,  # 2h remaining
        }
        r = _make_redis(ack_doc=ack_doc)
        eng = _engine(r, ack_renew_reminder_sec=300)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        assert result['policy']['decision'] == 'acked'
        assert not bool(result['policy']['should_notify'])

    def test_ack_near_expiry_triggers_renew_reminder(self):
        """Active ack expiring within reminder window results in decision=renew_reminder with notify=True."""
        ack_doc = {
            'fingerprint': 'fp1',
            'symbol': 'BTCUSDT',
            'ticket': 'TKT-ACK',
            'acked_by': 'charlie',
            'expires_at_ms': _ms_now() + 60_000,   # 60s remaining
        }
        r = _make_redis(ack_doc=ack_doc)
        eng = _engine(r, ack_renew_reminder_sec=300)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        assert result['policy']['decision'] == 'renew_reminder'
        assert bool(result['policy']['should_notify'])
        # telegram_text should include reminder context
        assert 'reminder' in str(result.get('telegram_text') or '').lower()

    def test_expired_ack_falls_through_to_notify(self):
        """Once ack expires, incident re-surfaces as notify."""
        ack_doc = {
            'fingerprint': 'fp1',
            'expires_at_ms': _ms_now() - 1000,  # expired
        }
        r = _make_redis(ack_doc=ack_doc)
        eng = _engine(r, ack_renew_reminder_sec=300)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        assert result['policy']['decision'] == 'notify'

    def test_renew_action_offered_when_ack_active(self):
        """When ack is active, runbook should offer renew instead of first-time ack."""
        ack_doc = {
            'fingerprint': 'fp1',
            'symbol': 'BTCUSDT',
            'expires_at_ms': _ms_now() + 3600_000,
        }
        r = _make_redis(ack_doc=ack_doc)
        eng = _engine(r, ack_renew_reminder_sec=300)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        actions = {a['action'] for a in result['policy']['runbook_actions']}
        assert 'renew_ack' in actions
        assert 'ack' not in actions

    def test_suppression_takes_priority_over_ack(self):
        """Symbol suppression has higher priority than ack-aware logic."""
        ack_doc = {'fingerprint': 'fp1', 'expires_at_ms': _ms_now() + 7200_000}
        suppress_doc = {'symbol': 'BTCUSDT', 'reason': 'manual'}
        r = _make_redis(ack_doc=ack_doc, suppress_doc=suppress_doc)
        eng = _engine(r, ack_renew_reminder_sec=300)
        bundle = _make_bundle()
        result = eng.triage_bundle(bundle)
        assert result['policy']['decision'] == 'suppressed'

    def test_decision_field_in_telegram_stream_fields(self):
        """telegram_stream_fields must include the decision field (P13 requirement)."""
        r = _make_redis()
        eng = _engine(r)
        bundle = _make_bundle()
        triaged = eng.triage_bundle(bundle)
        fields = eng.telegram_stream_fields(triaged)
        assert 'decision' in fields
        assert str(fields['decision']) == str(triaged['policy']['decision'])


# ---------------------------------------------------------------------------
# Tests: metric emission
# ---------------------------------------------------------------------------

class TestMetricEmission:
    @patch('services.active_symbol_guard_incident_policy.EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL')
    def test_renew_metric_emitted_on_reminder(self, mock_metric):
        ack_doc = {'fingerprint': 'fp1', 'expires_at_ms': _ms_now() + 60_000}
        r = _make_redis(ack_doc=ack_doc)
        eng = _engine(r, ack_renew_reminder_sec=300)
        bundle = _make_bundle()
        eng.triage_bundle(bundle)
        assert mock_metric.labels.called
