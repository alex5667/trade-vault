from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional, Sequence

try:  # pragma: no cover
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.execution_metrics import (
        EXECUTION_ACTIVE_SYMBOL_GUARD_INCIDENT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_NOTIFY_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_SUPPRESSION_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL,
    )
except Exception:  # pragma: no cover
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from execution_metrics import (  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_INCIDENT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_NOTIFY_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_SUPPRESSION_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL,
    )


def _ms_now() -> int:
    return get_ny_time_millis()


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


class ActiveSymbolGuardIncidentPolicyEngine:
    """Policy/triage layer for active-symbol incidents.

    Responsibilities:
    - convert diagnostics bundles into normalized severity and runbook actions
    - apply dedupe/suppression policy in Redis
    - emit notifier payloads that can be used by Telegram/HTTP/UI
    """

    def __init__(
        self,
        redis_client: Any,
        diagnostics: ActiveSymbolGuardDiagnostics,
        *,
        incident_prefix: str = 'orders:active_symbol_guard:incident:last:',
        dedupe_prefix: str = 'orders:active_symbol_guard:incident:dedupe:',
        suppress_prefix: str = 'orders:active_symbol_guard:incident:suppress:',
        hold_key_prefix: str = 'orders:active_symbol_guard:hold:symbol:',
        escalation_key_prefix: str = 'orders:active_symbol_guard:incident:ack:',
        dedupe_ttl_info_sec: int | None = None,
        dedupe_ttl_warning_sec: int | None = None,
        dedupe_ttl_critical_sec: int | None = None,
        ack_renew_reminder_sec: int | None = None,
    ) -> None:
        self.r = redis_client
        self.diagnostics = diagnostics
        self.incident_prefix = str(incident_prefix or 'orders:active_symbol_guard:incident:last:')
        self.dedupe_prefix = str(dedupe_prefix or 'orders:active_symbol_guard:incident:dedupe:')
        self.suppress_prefix = str(suppress_prefix or 'orders:active_symbol_guard:incident:suppress:')
        self.hold_key_prefix = str(hold_key_prefix or 'orders:active_symbol_guard:hold:symbol:')
        self.escalation_key_prefix = str(escalation_key_prefix or 'orders:active_symbol_guard:incident:ack:')
        self.dedupe_ttl_info_sec = max(int(dedupe_ttl_info_sec or os.getenv('ACTIVE_SYMBOL_GUARD_INCIDENT_DEDUPE_INFO_SEC', '300')), 1)
        self.dedupe_ttl_warning_sec = max(int(dedupe_ttl_warning_sec or os.getenv('ACTIVE_SYMBOL_GUARD_INCIDENT_DEDUPE_WARNING_SEC', '900')), 1)
        self.dedupe_ttl_critical_sec = max(int(dedupe_ttl_critical_sec or os.getenv('ACTIVE_SYMBOL_GUARD_INCIDENT_DEDUPE_CRITICAL_SEC', '1800')), 1)
        self.default_symbol_suppress_sec = max(int(os.getenv('ACTIVE_SYMBOL_GUARD_SYMBOL_SUPPRESS_SEC', '1800')), 1)
        self.default_fingerprint_suppress_sec = max(int(os.getenv('ACTIVE_SYMBOL_GUARD_FINGERPRINT_SUPPRESS_SEC', '900')), 1)
        # P13: ack-renew reminder window (constructor kwarg takes priority over env var)
        self.ack_renew_reminder_sec = max(int(ack_renew_reminder_sec or os.getenv('ACTIVE_SYMBOL_GUARD_ACK_RENEW_REMINDER_SEC', '300')), 1)

    def _severity_ttl_sec(self, severity: str) -> int:
        sev = str(severity or 'info').lower()
        if sev == 'critical':
            return max(int(self.dedupe_ttl_critical_sec), 60)
        if sev == 'warning':
            return max(int(self.dedupe_ttl_warning_sec), 60)
        return max(int(self.dedupe_ttl_info_sec), 60)

    def _metric_notify(self, severity: str, channel: str, result: str) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_NOTIFY_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_NOTIFY_TOTAL.labels(
                    severity=str(severity or ''), channel=str(channel or ''), result=str(result or '')
                ).inc()
        except Exception:
            pass

    def _suppression_metric(self, scope: str, result: str) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_SUPPRESSION_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_SUPPRESSION_TOTAL.labels(scope=str(scope or ''), result=str(result or '')).inc()
        except Exception:
            pass

    def _metric_incident(self, severity: str, classification: str, decision: str) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_INCIDENT_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_INCIDENT_TOTAL.labels(
                    severity=str(severity or ''), classification=str(classification or ''), decision=str(decision or '')
                ).inc()
        except Exception:
            pass

    def _renew_metric(self, severity: str, result: str) -> None:
        """Increment renew-reminder counter when ack entries are nearing expiry."""
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RENEW_REMINDER_TOTAL.labels(severity=str(severity or ''), result=str(result or '')).inc()
        except Exception:
            pass

    def _hold_state(self, symbol: str) -> Dict[str, Any]:
        """Return current manual hold for a symbol, with is_active flag."""
        symbol = str(symbol or '').strip().upper()
        if not symbol:
            return {}
        doc = self._load_json_key(f'{self.hold_key_prefix}{symbol}')
        if not doc:
            return {}
        exp = _i(doc.get('expires_at_ms'), 0)
        doc['is_active'] = bool(str(doc.get('hold_status') or 'active') == 'active' and (exp <= 0 or exp > _ms_now()))
        return doc

    def _ack_state(self, fingerprint: str) -> Dict[str, Any]:
        """Return current escalation ack for a fingerprint, with is_active / needs_renew_reminder flags."""
        fp = str(fingerprint or '').strip()
        if not fp:
            return {}
        doc = self._load_json_key(f'{self.escalation_key_prefix}{fp}')
        if not doc:
            return {}
        exp = _i(doc.get('expires_at_ms'), 0)
        now = _ms_now()
        doc['is_active'] = bool(exp <= 0 or exp > now)
        doc['remaining_sec'] = max(int((exp - now) / 1000), 0) if exp > 0 else 0
        doc['needs_renew_reminder'] = bool(
            doc.get('is_active') and exp > 0 and (exp - now) <= int(self.ack_renew_reminder_sec * 1000)
        )
        return doc

    def _fingerprint(self, summary: Dict[str, Any], exchange_truth: Dict[str, Any], race_chains: Sequence[Dict[str, Any]]) -> str:
        symbol = str(summary.get('symbol') or '').strip().upper()
        classification = str(summary.get('classification') or '')
        severity = str(summary.get('severity') or '')
        hot_5m = _i((summary.get('hotness') or {}).get('5m'), 0)
        hot_bucket = '5+' if hot_5m >= 5 else '3+' if hot_5m >= 3 else '1+' if hot_5m >= 1 else '0'
        race_types = sorted({str((item or {}).get('chain_type') or '') for item in (race_chains or []) if str((item or {}).get('chain_type') or '')})
        shape = {
            'symbol': symbol,
            'classification': classification,
            'severity': severity,
            'hot_bucket': hot_bucket,
            'race_types': race_types,
            'exchange_has_live_position': bool((exchange_truth or {}).get('has_live_position')),
            'exchange_has_open_orders': bool((exchange_truth or {}).get('has_open_orders')),
            'exchange_reliable': bool((exchange_truth or {}).get('is_reliable')),
        }
        raw = json.dumps(shape, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        return hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]

    def _score_bundle(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        summary = dict(bundle.get('summary') or {})
        exchange_truth = dict(bundle.get('exchange_truth') or {})
        classification = str(summary.get('classification') or bundle.get('classification') or '')
        hotness = dict(summary.get('hotness') or {})
        hot_5m = _i(hotness.get('5m'), 0)
        hot_1h = _i(hotness.get('1h'), 0)
        race_chains = list(bundle.get('suspicious_writer_race_chains') or [])
        race_types = [str((item or {}).get('chain_type') or '') for item in race_chains]
        score = 0
        reasons: List[str] = []

        base_points = {
            'active': 5,
            'pending_release': 30,
            'released_tombstone': 12,
            'stale_tombstone': 45,
            'unknown': 8,
            'missing_symbol': 20,
        }
        score += int(base_points.get(classification, 0))
        if classification in base_points:
            reasons.append(f'class:{classification}+{base_points.get(classification, 0)}')

        if hot_5m > 0:
            add = min(hot_5m * 8, 32)
            score += int(add)
            reasons.append(f'hot5m+{add}')
        if hot_1h > 0:
            add = min(hot_1h * 2, 16)
            score += int(add)
            reasons.append(f'hot1h+{add}')
        if race_chains:
            add = min(len(race_chains) * 10, 30)
            score += int(add)
            reasons.append(f'race+{add}')
        if 'multi_writer_conflict_burst' in race_types:
            score += 15
            reasons.append('multi_writer_conflict_burst+15')
        if 'release_then_conflict' in race_types:
            score += 20
            reasons.append('release_then_conflict+20')
        if 'resurrection_attempt' in race_types:
            score += 35
            reasons.append('resurrection_attempt+35')
        if bool(exchange_truth.get('has_live_position')) and classification in {'released_tombstone', 'stale_tombstone'}:
            score += 60
            reasons.append('live_position_while_nonblocking+60')
        elif bool(exchange_truth.get('has_live_position')) and classification == 'pending_release':
            score += 20
            reasons.append('live_position_pending_release+20')
        if bool(exchange_truth.get('has_open_orders')) and classification in {'released_tombstone', 'stale_tombstone'}:
            score += 25
            reasons.append('open_orders_while_nonblocking+25')
        if exchange_truth and not bool(exchange_truth.get('is_reliable')):
            score += 5
            reasons.append('exchange_unreliable+5')

        severity = 'info'
        if score >= 80:
            severity = 'critical'
        elif score >= 35:
            severity = 'warning'
        return {
            'score': int(score),
            'severity': severity,
            'reasons': reasons,
        }

    def _runbook_actions(self, bundle: Dict[str, Any], score_info: Dict[str, Any], hold_state: Dict[str, Any], ack_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        summary = dict(bundle.get('summary') or {})
        symbol = str(summary.get('symbol') or '').strip().upper()
        sid = str(summary.get('sid') or '').strip()
        classification = str(summary.get('classification') or '')
        exchange_truth = dict(bundle.get('exchange_truth') or {})
        severity = str(score_info.get('severity') or 'info')
        base_url = os.getenv('ACTIVE_SYMBOL_GUARD_EXPORTER_BASE_URL', 'http://127.0.0.1:8788').rstrip('/')
        actions: List[Dict[str, Any]] = [
            {
                'action': 'inspect',
                'kind': 'read_only',
                'endpoint': f'{base_url}/api/active-symbol-guard/incident/symbol/{symbol}',
                'symbol': symbol,
                'sid': sid,
                'enabled': bool(symbol),
            }
        ]
        # P13: hold-aware — if hold is active, offer revoke; else offer apply
        if bool(hold_state.get('is_active')):
            actions.append({'action': 'revoke_hold', 'kind': 'runbook', 'symbol': symbol, 'sid': sid, 'enabled': True, 'ticket': str(hold_state.get('ticket') or '')})
        else:
            actions.append({'action': 'hold_symbol', 'kind': 'runbook', 'symbol': symbol, 'sid': sid, 'enabled': bool(symbol and severity in {'warning', 'critical'})})
        force_release_enabled = bool(symbol and classification in {'pending_release', 'released_tombstone', 'stale_tombstone'} and bool(exchange_truth.get('is_flat')))
        actions.append({
            'action': 'force_release', 'kind': 'runbook', 'symbol': symbol, 'sid': sid,
            'enabled': force_release_enabled, 'expected_sid': sid,
        })
        # P13: ack-aware — if ack is active, offer renew; else offer ack
        if bool(ack_state.get('is_active')):
            actions.append({'action': 'renew_ack', 'kind': 'runbook', 'symbol': symbol, 'sid': sid, 'enabled': bool(ack_state.get('needs_renew_reminder')), 'fingerprint': str(ack_state.get('fingerprint') or '')})
        else:
            actions.append({'action': 'ack', 'kind': 'runbook', 'symbol': symbol, 'sid': sid, 'enabled': bool(symbol and severity in {'warning', 'critical'})})
        actions.append({
            'action': 'escalate', 'kind': 'notification', 'symbol': symbol, 'sid': sid,
            'enabled': bool(symbol and (severity == 'critical' or int(score_info.get('score') or 0) >= 65)),
            'target': os.getenv('ACTIVE_SYMBOL_GUARD_ESCALATION_TARGET', 'telegram:oncall'),
        })
        return actions

    def _dedupe_key(self, fingerprint: str) -> str:
        return f'{self.dedupe_prefix}{fingerprint}'

    def _symbol_suppress_key(self, symbol: str) -> str:
        return f'{self.suppress_prefix}symbol:{str(symbol or "").strip().upper()}'

    def _fingerprint_suppress_key(self, fingerprint: str) -> str:
        return f'{self.suppress_prefix}fingerprint:{fingerprint}'

    def set_symbol_suppression(self, symbol: str, *, ttl_sec: Optional[int] = None, reason: str = 'manual') -> Dict[str, Any]:
        symbol = str(symbol or '').strip().upper()
        ttl = max(int(ttl_sec or self.default_symbol_suppress_sec), 1)
        doc = {'symbol': symbol, 'reason': str(reason or ''), 'created_at_ms': _ms_now(), 'ttl_sec': ttl}
        self.r.set(self._symbol_suppress_key(symbol), json.dumps(doc, ensure_ascii=False), ex=ttl)
        return doc

    def set_fingerprint_suppression(self, fingerprint: str, *, ttl_sec: Optional[int] = None, reason: str = 'manual') -> Dict[str, Any]:
        fp = str(fingerprint or '').strip()
        ttl = max(int(ttl_sec or self.default_fingerprint_suppress_sec), 1)
        doc = {'fingerprint': fp, 'reason': str(reason or ''), 'created_at_ms': _ms_now(), 'ttl_sec': ttl}
        self.r.set(self._fingerprint_suppress_key(fp), json.dumps(doc, ensure_ascii=False), ex=ttl)
        return doc

    def _load_json_key(self, key: str) -> Dict[str, Any]:
        try:
            raw = self.r.get(key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _suppression_state(self, symbol: str, fingerprint: str) -> Dict[str, Any]:
        sdoc = self._load_json_key(self._symbol_suppress_key(symbol)) if symbol else {}
        fdoc = self._load_json_key(self._fingerprint_suppress_key(fingerprint)) if fingerprint else {}
        return {
            'symbol': sdoc,
            'fingerprint': fdoc,
            'is_suppressed': bool(sdoc or fdoc),
        }

    def _dedupe_state(self, fingerprint: str) -> Dict[str, Any]:
        doc = self._load_json_key(self._dedupe_key(fingerprint)) if fingerprint else {}
        return {'fingerprint': doc, 'is_deduped': bool(doc)}

    def _store_dedupe(self, *, fingerprint: str, payload: Dict[str, Any], ttl_sec: int) -> None:
        self.r.set(self._dedupe_key(fingerprint), json.dumps(payload, ensure_ascii=False, default=str), ex=max(int(ttl_sec), 1))

    def triage_bundle(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        summary = dict(bundle.get('summary') or {})
        symbol = str(summary.get('symbol') or '').strip().upper()
        score_info = self._score_bundle(bundle)
        summary['severity'] = str(score_info.get('severity') or 'info')
        summary['score'] = int(score_info.get('score') or 0)
        exchange_truth = dict(bundle.get('exchange_truth') or {})
        race_chains = list(bundle.get('suspicious_writer_race_chains') or [])
        fingerprint = self._fingerprint(summary, exchange_truth, race_chains)
        # P13: load hold + ack state before deciding suppression/notify path
        hold_state = self._hold_state(symbol)
        ack_state = self._ack_state(fingerprint)
        summary['fingerprint'] = fingerprint
        summary['hold_active'] = bool(hold_state.get('is_active'))
        summary['hold_ticket'] = str(hold_state.get('ticket') or '')
        summary['ack_active'] = bool(ack_state.get('is_active'))
        summary['ack_ticket'] = str(ack_state.get('ticket') or '')
        summary['ack_remaining_sec'] = int(ack_state.get('remaining_sec') or 0)
        bundle['summary'] = summary
        policy = {
            'severity': summary['severity'],
            'score': summary['score'],
            'score_reasons': list(score_info.get('reasons') or []),
            'fingerprint': fingerprint,
            'hold_state': hold_state,
            'ack_state': ack_state,
        }
        policy['runbook_actions'] = self._runbook_actions(bundle, score_info, hold_state, ack_state)
        suppression = self._suppression_state(symbol, fingerprint)
        dedupe = self._dedupe_state(fingerprint)
        policy['suppression'] = suppression
        policy['dedupe'] = dedupe
        decision = 'notify'
        if suppression.get('is_suppressed'):
            decision = 'suppressed'
            self._suppression_metric('symbol_or_fingerprint', 'suppressed')
        elif bool(ack_state.get('is_active')):
            # P13: ack-aware suppression with renew-reminder path
            if bool(ack_state.get('needs_renew_reminder')):
                decision = 'renew_reminder'
                self._renew_metric(summary['severity'], 'due')
            else:
                decision = 'acked'
                self._suppression_metric('ack', 'acked')
        elif dedupe.get('is_deduped'):
            decision = 'deduped'
            self._suppression_metric('fingerprint', 'deduped')
        policy['decision'] = decision
        policy['should_notify'] = decision in {'notify', 'renew_reminder'}
        policy['notify_channels'] = ['telegram_stream', 'http', 'ui'] if policy['should_notify'] else []
        # P13: annotate score reasons with hold/ack context
        if bool(hold_state.get('is_active')):
            policy.setdefault('score_reasons', []).append('manual_hold_active')
        if bool(ack_state.get('is_active')):
            policy.setdefault('score_reasons', []).append('incident_acked')
            if bool(ack_state.get('needs_renew_reminder')):
                policy.setdefault('score_reasons', []).append('ack_needs_renew')
        bundle['policy'] = policy
        bundle['runbook_actions'] = list(policy['runbook_actions'])
        if decision == 'renew_reminder':
            bundle['telegram_text'] = (
                f"{str(bundle.get('telegram_text') or '')}"
                f"\nAck reminder: ticket={ack_state.get('ticket')} remaining_sec={ack_state.get('remaining_sec')}"
            )
        self._metric_incident(summary['severity'], str(summary.get('classification') or ''), decision)
        return bundle

    def triage_symbol(self, symbol: str, *, include_exchange: bool = False) -> Dict[str, Any]:
        return self.triage_bundle(self.diagnostics.incident_bundle_symbol(symbol, include_exchange=include_exchange))

    def triage_sid(self, sid: str, *, include_exchange: bool = False) -> Dict[str, Any]:
        return self.triage_bundle(self.diagnostics.incident_bundle_sid(sid, include_exchange=include_exchange))

    def mark_notified(self, triaged: Dict[str, Any], *, channel: str, result: str = 'sent') -> None:
        summary = dict((triaged or {}).get('summary') or {})
        policy = dict((triaged or {}).get('policy') or {})
        severity = str(summary.get('severity') or 'info')
        classification = str(summary.get('classification') or '')
        fingerprint = str(policy.get('fingerprint') or summary.get('fingerprint') or '')
        symbol = str(summary.get('symbol') or '').strip().upper()
        ttl = self._severity_ttl_sec(severity)
        if fingerprint:
            doc = {
                'fingerprint': fingerprint,
                'symbol': symbol,
                'severity': severity,
                'classification': classification,
                'notified_at_ms': _ms_now(),
                'channel': str(channel or ''),
                'result': str(result or ''),
                'ttl_sec': ttl,
            }
            self._store_dedupe(fingerprint=fingerprint, payload=doc, ttl_sec=ttl)
        self._metric_notify(severity, channel, result)
        if symbol:
            try:
                self.r.set(f'{self.incident_prefix}{symbol}', json.dumps(triaged, ensure_ascii=False, default=str), ex=max(ttl, 60))
            except Exception:
                pass

    def telegram_stream_fields(self, triaged: Dict[str, Any]) -> Dict[str, str]:
        summary = dict((triaged or {}).get('summary') or {})
        policy = dict((triaged or {}).get('policy') or {})
        return {
            'type': 'report',
            'subtype': 'active_symbol_guard_incident',
            'severity': str(summary.get('severity') or 'info'),
            'decision': str(policy.get('decision') or 'notify'),
            'symbol': str(summary.get('symbol') or ''),
            'sid': str(summary.get('sid') or ''),
            'fingerprint': str(policy.get('fingerprint') or summary.get('fingerprint') or ''),
            'text': str((triaged or {}).get('telegram_text') or ''),
            'payload': json.dumps(triaged, ensure_ascii=False, default=str),
            'ts': str(_ms_now()),
        }
