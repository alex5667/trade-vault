from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Iterable
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

try:  # pragma: no cover
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
    from services.active_symbol_guard_semantics import guard_view
    from services.active_symbol_guard_store import ActiveSymbolGuardStore
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution_metrics import (
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL,
    )
except Exception:  # pragma: no cover
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore
    from active_symbol_guard_semantics import guard_view  # type: ignore
    from active_symbol_guard_store import ActiveSymbolGuardStore  # type: ignore
    from binance_futures_client import BinanceFuturesClient  # type: ignore
    from execution_metrics import (  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL,
    )


def _ms_now() -> int:
    return get_ny_time_millis()


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _normalize(obj: Any) -> Any:
    """Recursively decode bytes keys/values from Redis responses."""
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, dict):
        return {str(_normalize(k)): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize(x) for x in obj]
    return obj


class ActiveSymbolGuardRunbookExecutor:
    """Safe execution layer for manual runbook actions.

    Design goals:
    - every action leaves an audit trail with operator/ticket/result
    - destructive action (force_release) is guarded by exchange truth
    - apply/revoke hold is explicit and TTL-bound
    - escalation ack/renew is stored in Redis with renewable TTL

    Audit trail is written to Redis stream: orders:active_symbol_guard:audit
    Required audit fields: operator, action, ticket, symbol, sid, fingerprint, result, reason
    """

    def __init__(
        self,
        redis_client: Any,
        diagnostics: ActiveSymbolGuardDiagnostics | None = None,
        policy: ActiveSymbolGuardIncidentPolicyEngine | None = None,
        store: ActiveSymbolGuardStore | None = None,
        client: BinanceFuturesClient | None = None,
        *,
        hold_ttl_sec: int | None = None,
        escalation_ack_ttl_sec: int | None = None,
        audit_stream: str = 'orders:active_symbol_guard:audit',
        hold_key_prefix: str = 'orders:active_symbol_guard:hold:symbol:',
        escalation_key_prefix: str = 'orders:active_symbol_guard:incident:ack:',
    ) -> None:
        self.r = redis_client
        self.diagnostics = diagnostics
        self.policy = policy
        self.store = store or ActiveSymbolGuardStore(
            redis_client,
            key_prefix='orders:active_symbol_sid:',
            active_ttl_sec=86400,
            tombstone_ttl_sec=int(os.getenv('ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC', '120')),
        )
        self.client = client
        self.hold_ttl_sec = max(int(hold_ttl_sec or os.getenv('ACTIVE_SYMBOL_GUARD_RUNBOOK_HOLD_TTL_SEC', '1800')), 1)
        self.escalation_ack_ttl_sec = max(int(escalation_ack_ttl_sec or os.getenv('ACTIVE_SYMBOL_GUARD_ESCALATION_ACK_TTL_SEC', '1800')), 1)
        self.audit_stream = (audit_stream or 'orders:active_symbol_guard:audit')
        self.hold_key_prefix = (hold_key_prefix or 'orders:active_symbol_guard:hold:symbol:').rstrip(':') + ':'
        self.escalation_key_prefix = (escalation_key_prefix or 'orders:active_symbol_guard:incident:ack:').rstrip(':') + ':'

    def _hold_key(self, symbol: str) -> str:
        return f"{self.hold_key_prefix}{(symbol or '').strip().upper()}"

    def _escalation_key(self, fingerprint: str) -> str:
        return f"{self.escalation_key_prefix}{(fingerprint or '').strip()}"

    def _json_get(self, key: str) -> dict[str, Any]:
        try:
            raw = self.r.get(key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _json_set(self, key: str, doc: dict[str, Any], ttl_sec: int) -> None:
        self.r.set(key, json.dumps(doc, ensure_ascii=False, default=str), ex=max(ttl_sec, 1))

    def _audit(self, **kwargs: Any) -> None:
        """Write one canonical audit record to the Redis stream with all required fields."""
        fields = {
            k: (json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (dict, list)) else str(v))
            for k, v in kwargs.items()
        }
        fields['ts_ms'] = str(_ms_now())
        # Ensure all required canonical fields are always present
        for f in ('operator', 'action', 'ticket', 'symbol', 'sid', 'fingerprint', 'result', 'reason'):
            fields.setdefault(f, '')
        try:
            self.r.xadd(self.audit_stream, fields, maxlen=100000, approximate=True)
        except Exception:
            with contextlib.suppress(Exception):
                self.r.xadd(self.audit_stream, fields, maxlen=50000)
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL.labels(
                    action=(kwargs.get('action') or ''),
                    result=(kwargs.get('result') or ''),
                ).inc()
        except Exception:
            pass

    def _metric(self, *, action: str, result: str) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL.labels(action=(action or ''), result=(result or '')).inc()
        except Exception:
            pass

    def _iter_prefix_keys(self, prefix: str) -> list[str]:
        """Scan Redis for keys matching prefix*, return sorted unique list."""
        out: list[str] = []
        pattern = f"{prefix}*"
        try:
            if hasattr(self.r, 'scan_iter'):
                out.extend([str(_normalize(k)) for k in self.r.scan_iter(pattern, count=10000)])
            elif hasattr(self.r, 'keys'):
                out.extend([str(_normalize(k)) for k in (self.r.keys(pattern) or [])])
        except Exception:
            pass
        return sorted(set([k for k in out if k.startswith(prefix)]))

    def _stream_entries(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return the most recent audit stream entries as normalized dicts."""
        items: Iterable[Any] = []
        try:
            if hasattr(self.r, 'xrevrange'):
                items = self.r.xrevrange(self.audit_stream, count=max(limit, 1)) or []
            elif hasattr(self.r, 'xrange'):
                raw = self.r.xrange(self.audit_stream, count=max(limit, 1)) or []
                items = list(reversed(raw))
        except Exception:
            items = []
        out: list[dict[str, Any]] = []
        for item in items:
            try:
                entry_id, fields = item
                doc = _normalize(fields)
                if not isinstance(doc, dict):
                    continue
                doc['stream_id'] = str(_normalize(entry_id))
                if 'payload' in doc and isinstance(doc.get('payload'), str):
                    try:
                        doc['payload_json'] = json.loads((doc.get('payload') or '{}'))
                    except Exception:
                        doc['payload_json'] = {}
                out.append(doc)
            except Exception:
                continue
        return out

    def hold_state(self, symbol: str) -> dict[str, Any]:
        """Return the current hold document for a symbol, with is_active flag."""
        symbol = (symbol or '').strip().upper()
        doc = self._json_get(self._hold_key(symbol))
        if not doc:
            return {}
        now_ms = _ms_now()
        expires_at_ms = _i(doc.get('expires_at_ms'), 0)
        doc['is_active'] = ((doc.get('hold_status') or 'active') == 'active' and (expires_at_ms <= 0 or expires_at_ms > now_ms))
        return doc

    def active_holds(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return all currently active symbol holds, sorted by most recently updated."""
        out: list[dict[str, Any]] = []
        for key in self._iter_prefix_keys(self.hold_key_prefix):
            doc = self._json_get(key)
            if not doc:
                continue
            symbol = (doc.get('symbol') or key[len(self.hold_key_prefix):] or '').strip().upper()
            doc['symbol'] = symbol
            exp = _i(doc.get('expires_at_ms'), 0)
            doc['is_active'] = ((doc.get('hold_status') or 'active') == 'active' and (exp <= 0 or exp > _ms_now()))
            out.append(doc)
        out = [d for d in out if d.get('is_active')]
        out.sort(key=lambda d: (-_i(d.get('updated_at_ms') or d.get('applied_at_ms'), 0), (d.get('symbol') or '')))
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL.labels(kind='hold', status='active').set(len(out))
        except Exception:
            pass
        return out[:max(limit or 100, 1)]

    def apply_hold_symbol(self, *, symbol: str, operator: str, ticket: str, reason: str = '', ttl_sec: int | None = None) -> dict[str, Any]:
        """Apply a manual hold on a symbol. All new open orders will be blocked while hold is active.

        The hold is TTL-bound (default: ACTIVE_SYMBOL_GUARD_RUNBOOK_HOLD_TTL_SEC env var).
        Every hold is recorded in the audit stream with operator/ticket context.
        """
        if not symbol or not operator or not ticket:
            raise ValueError('symbol/operator/ticket required')
        now_ms = _ms_now()
        ttl = max(ttl_sec or self.hold_ttl_sec, 1)
        symbol = (symbol or '').strip().upper()
        # Load current guard sid so it's captured in the hold doc for traceability
        guard = self.store.load_view(symbol) if hasattr(self.store, 'load_view') else guard_view(self.store.load_raw(symbol))
        doc = {
            'symbol': symbol,
            'sid': (guard.get('sid') or ''),
            'hold_status': 'active',
            'reason': (reason or ''),
            'ticket': (ticket or ''),
            'operator': (operator or ''),
            'applied_at_ms': now_ms,
            'expires_at_ms': now_ms + ttl * 1000,
            'updated_at_ms': now_ms,
        }
        self._json_set(self._hold_key(symbol), doc, ttl)
        self._metric(action='apply_hold_symbol', result='applied')
        self._audit(action='apply_hold_symbol', operator=operator, ticket=ticket, symbol=symbol,
                    sid=(doc.get('sid') or ''), result='applied', reason=reason, payload=doc)
        return {'ok': True, 'action': 'apply_hold_symbol', 'symbol': symbol, 'hold': doc}

    def revoke_hold_symbol(self, *, symbol: str, operator: str, ticket: str, reason: str = '') -> dict[str, Any]:
        """Revoke an existing manual hold on a symbol.

        Deletes the hold key from Redis and records the action in the audit stream.
        Returns result='revoked' if key was present, 'noop' if it was already gone.
        """
        if not symbol or not operator or not ticket:
            raise ValueError('symbol/operator/ticket required')
        symbol = (symbol or '').strip().upper()
        prev = self.hold_state(symbol)
        deleted = (self.r.delete(self._hold_key(symbol)) or 0)
        result = 'revoked' if deleted else 'noop'
        self._metric(action='revoke_hold_symbol', result=result)
        self._audit(action='revoke_hold_symbol', operator=operator, ticket=ticket, symbol=symbol,
                    sid=(prev.get('sid') or ''), result=result, reason=reason, payload={'previous': prev})
        return {'ok': True, 'action': 'revoke_hold_symbol', 'symbol': symbol, 'result': result, 'previous_hold': prev}

    def _exchange_truth(self, symbol: str) -> dict[str, Any]:
        """Get exchange truth for a symbol, preferring diagnostics over direct client calls."""
        symbol = (symbol or '').strip().upper()
        if self.diagnostics is not None:
            try:
                payload = self.diagnostics.debug_symbol(symbol, include_exchange=True)
                truth = dict(payload.get('exchange_truth') or {})
                if truth:
                    return truth
            except Exception:
                pass
        if self.client is not None:
            try:
                pos = self.client.get_position_risk(symbol=symbol)
                plain = self.client.get_open_orders(symbol=symbol)
                algo = self.client.get_open_algo_orders(symbol=symbol)
                position_amt = 0.0
                if isinstance(pos, list) and pos:
                    row = pos[0] if isinstance(pos[0], dict) else {}
                    position_amt = _f(row.get('positionAmt'), 0.0)
                elif isinstance(pos, dict):
                    position_amt = _f(pos.get('positionAmt'), 0.0)
                return {
                    'symbol': symbol,
                    'position_amt': position_amt,
                    'open_plain_orders': len(plain) if isinstance(plain, list) else 0,
                    'open_algo_orders': len(algo) if isinstance(algo, list) else 0,
                    'is_reliable': True,
                }
            except Exception as exc:
                return {'symbol': symbol, 'is_reliable': False, 'error': str(exc)}
        return {'symbol': symbol, 'is_reliable': False, 'error': 'exchange_truth_unavailable'}

    def guarded_force_release(self, *, symbol: str, operator: str, ticket: str, expected_sid: str = '', reason: str = '', dry_run: bool = False) -> dict[str, Any]:
        """Guarded force-release of the active-symbol guard.

        Safety contract:
        - exchange truth must confirm: reliable AND position_amt==0 AND no plain/algo orders
        - if expected_sid is given and doesn't match the current guard sid, the action is blocked
        - dry_run=True allows probing whether release would succeed without actually releasing
        - All outcomes recorded in the audit stream

        force_release is designed as a guarded operator action, NOT an auto-policy action.
        """
        symbol = (symbol or '').strip().upper()
        operator = (operator or '').strip()
        ticket = (ticket or '').strip()
        if not symbol or not operator or not ticket:
            raise ValueError('symbol/operator/ticket required')

        raw = self.store.load_raw(symbol)
        view = guard_view(raw)
        guard_sid = (view.get('sid') or raw.get('sid') or '').strip()
        exchange_truth = self._exchange_truth(symbol)
        reliable = exchange_truth.get('is_reliable')
        position_amt = abs(_f(exchange_truth.get('position_amt'), 0.0))
        open_plain_orders = _i(exchange_truth.get('open_plain_orders'), 0)
        open_algo_orders = _i(exchange_truth.get('open_algo_orders'), 0)
        # Check sid match first — reject if caller expects a specific sid but it differs
        if expected_sid and guard_sid and guard_sid != (expected_sid or '').strip():
            result = {
                'ok': False,
                'action': 'guarded_force_release',
                'symbol': symbol,
                'reason': 'sid_mismatch',
                'guard_sid': guard_sid,
                'expected_sid': (expected_sid or '').strip(),
                'exchange_truth': exchange_truth,
            }
            self._metric(action='guarded_force_release', result='blocked')
            self._audit(operator=operator, action='guarded_force_release', ticket=ticket, symbol=symbol, sid=guard_sid, result='blocked', reason='sid_mismatch', payload=result)
            return result
        # Exchange must be flat: no position, no orders, reliable API response
        safe = (reliable and position_amt == 0.0 and open_plain_orders == 0 and open_algo_orders == 0)
        if not safe:
            result = {
                'ok': False,
                'action': 'guarded_force_release',
                'symbol': symbol,
                'reason': 'exchange_truth_not_safe',
                'guard': view,
                'exchange_truth': exchange_truth,
            }
            self._metric(action='guarded_force_release', result='blocked')
            self._audit(operator=operator, action='guarded_force_release', ticket=ticket, symbol=symbol, sid=guard_sid, result='blocked', reason='exchange_truth_not_safe', payload=result)
            return result
        if dry_run:
            result = {
                'ok': True,
                'action': 'guarded_force_release',
                'symbol': symbol,
                'result': 'dry_run_safe',
                'guard': view,
                'exchange_truth': exchange_truth,
            }
            self._metric(action='guarded_force_release', result='dry_run_safe')
            self._audit(operator=operator, action='guarded_force_release', ticket=ticket, symbol=symbol, sid=guard_sid, result='dry_run_safe', reason=reason, payload=result)
            return result
        # Perform the actual release with full operator attribution in the guard document
        released = self.store.mark_released(
            symbol=symbol,
            expected_sid=(expected_sid or guard_sid),
            release_reason='runbook_force_release',
            writer='runbook',
            extra_patch={
                'runbook_operator': operator,
                'runbook_ticket': ticket,
                'runbook_reason': (reason or ''),
                'guard_release_policy': 'runbook_force_release',
                'guard_release_pending': False,
                'updated_at_ms': _ms_now(),
            },
        )
        result_name = 'released' if released.get('applied') else (released.get('reason') or 'rejected')
        payload = {
            'ok': released.get('applied'),
            'action': 'guarded_force_release',
            'symbol': symbol,
            'sid': guard_sid,
            'result': result_name,
            'released_doc': dict(released.get('doc') or {}),
            'exchange_truth': exchange_truth,
        }
        self._metric(action='guarded_force_release', result=result_name)
        self._audit(operator=operator, action='guarded_force_release', ticket=ticket, symbol=symbol, sid=guard_sid, result=result_name, reason=reason, payload=payload)
        return payload

    def _resolve_triage(self, *, symbol: str = '', sid: str = '', fingerprint: str = '', include_exchange: bool = True) -> dict[str, Any]:
        """Resolve triage payload for given symbol/sid/fingerprint."""
        fp = (fingerprint or '').strip()
        if self.policy is None and self.diagnostics is not None:
            self.policy = ActiveSymbolGuardIncidentPolicyEngine(self.r, self.diagnostics)
        if self.policy is None:
            return {'summary': {'symbol': (symbol or '').upper(), 'sid': (sid or ''), 'severity': 'info'}, 'policy': {'fingerprint': fp}}
        if fp:
            summary = {'symbol': (symbol or '').upper(), 'sid': (sid or ''), 'severity': 'info'}
            return {'summary': summary, 'policy': {'fingerprint': fp}}
        if symbol:
            return self.policy.triage_symbol((symbol or '').upper(), include_exchange=include_exchange)
        if sid:
            return self.policy.triage_sid((sid or ''), include_exchange=include_exchange)
        return {'summary': {'severity': 'info'}, 'policy': {'fingerprint': ''}}

    def escalation_state(self, fingerprint: str) -> dict[str, Any]:
        """Return the current escalation ack document for a fingerprint, with is_active flag."""
        fp = (fingerprint or '').strip()
        if not fp:
            return {}
        doc = self._json_get(self._escalation_key(fp))
        if not doc:
            return {}
        now_ms = _ms_now()
        expires_at_ms = _i(doc.get('expires_at_ms'), 0)
        doc['is_active'] = (expires_at_ms <= 0 or expires_at_ms > now_ms)
        return doc

    def active_acks(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return all currently active escalation acks, sorted by most recently updated."""
        out: list[dict[str, Any]] = []
        for key in self._iter_prefix_keys(self.escalation_key_prefix):
            doc = self._json_get(key)
            if not doc:
                continue
            exp = _i(doc.get('expires_at_ms'), 0)
            doc['is_active'] = (exp <= 0 or exp > _ms_now())
            doc['fingerprint'] = (doc.get('fingerprint') or key[len(self.escalation_key_prefix):] or '')
            if doc['is_active']:
                out.append(doc)
        out.sort(key=lambda d: (-_i(d.get('updated_at_ms') or d.get('acked_at_ms'), 0), (d.get('symbol') or '')))
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL.labels(kind='ack', status='active').set(len(out))
        except Exception:
            pass
        return out[:max(limit or 100, 1)]

    def escalation_ack(self, *, symbol: str = '', sid: str = '', fingerprint: str = '', operator: str, ticket: str, reason: str = '', ttl_sec: int | None = None) -> dict[str, Any]:
        """Acknowledge an escalation by fingerprint (or resolved via symbol/sid).

        The ack is TTL-bound and stored in Redis. Ack/renew is NOT "magic flag auth" —
        it is a TTL-bound document with a full audit trail.
        """
        operator = (operator or '').strip()
        ticket = (ticket or '').strip()
        if not operator or not ticket:
            raise ValueError('operator/ticket required')
        triaged = self._resolve_triage(symbol=symbol, sid=sid, fingerprint=fingerprint, include_exchange=True)
        summary = dict((triaged or {}).get('summary') or {})
        policy = dict((triaged or {}).get('policy') or {})
        fp = str(policy.get('fingerprint') or fingerprint or '').strip()
        if not fp:
            raise ValueError('fingerprint could not be resolved')
        now_ms = _ms_now()
        ttl = max(ttl_sec or self.escalation_ack_ttl_sec, 1)
        current = self.escalation_state(fp)
        doc = dict(current or {})
        doc.update({
            'fingerprint': fp,
            'symbol': (summary.get('symbol') or symbol or '').upper(),
            'sid': (summary.get('sid') or sid or ''),
            'ack_status': 'acked',
            'acked_by': operator,
            'ticket': ticket,
            'ack_reason': (reason or ''),
            'acked_at_ms': _i(doc.get('acked_at_ms'), now_ms) or now_ms,
            'renew_count': _i(doc.get('renew_count'), 0),
            'expires_at_ms': now_ms + ttl * 1000,
            'updated_at_ms': now_ms,
        })
        self._json_set(self._escalation_key(fp), doc, ttl)
        result = {'ok': True, 'action': 'escalation_ack', 'fingerprint': fp, 'state': doc, 'triage': triaged}
        self._metric(action='escalation_ack', result='acked')
        self._audit(operator=operator, action='escalation_ack', ticket=ticket, symbol=(doc.get('symbol') or ''), sid=(doc.get('sid') or ''), fingerprint=fp, result='acked', reason=reason, payload=result)
        return result

    def escalation_renew(self, *, symbol: str = '', sid: str = '', fingerprint: str = '', operator: str, ticket: str, reason: str = '', ttl_sec: int | None = None) -> dict[str, Any]:
        """Renew an existing escalation ack, extending its TTL.

        Requires an existing ack to be present (ack_missing returned otherwise).
        Increments renew_count on each renewal for audit tracking.
        """
        operator = (operator or '').strip()
        ticket = (ticket or '').strip()
        if not operator or not ticket:
            raise ValueError('operator/ticket required')
        triaged = self._resolve_triage(symbol=symbol, sid=sid, fingerprint=fingerprint, include_exchange=True)
        summary = dict((triaged or {}).get('summary') or {})
        policy = dict((triaged or {}).get('policy') or {})
        fp = str(policy.get('fingerprint') or fingerprint or '').strip()
        if not fp:
            raise ValueError('fingerprint could not be resolved')
        current = self.escalation_state(fp)
        if not current:
            result = {'ok': False, 'action': 'escalation_renew', 'fingerprint': fp, 'reason': 'ack_missing'}
            self._metric(action='escalation_renew', result='ack_missing')
            self._audit(operator=operator, action='escalation_renew', ticket=ticket, symbol=(summary.get('symbol') or ''), sid=(summary.get('sid') or ''), fingerprint=fp, result='ack_missing', reason=reason, payload=result)
            return result
        now_ms = _ms_now()
        ttl = max(ttl_sec or self.escalation_ack_ttl_sec, 1)
        doc = dict(current)
        doc.update({
            'fingerprint': fp,
            'symbol': str(doc.get('symbol') or summary.get('symbol') or symbol or '').upper(),
            'sid': str(doc.get('sid') or summary.get('sid') or sid or ''),
            'ack_status': 'acked',
            'renewed_by': operator,
            'renew_ticket': ticket,
            'renew_reason': (reason or ''),
            'renew_count': _i(doc.get('renew_count'), 0) + 1,
            'renewed_at_ms': now_ms,
            'expires_at_ms': now_ms + ttl * 1000,
            'updated_at_ms': now_ms,
        })
        self._json_set(self._escalation_key(fp), doc, ttl)
        result = {'ok': True, 'action': 'escalation_renew', 'fingerprint': fp, 'state': doc, 'triage': triaged}
        self._metric(action='escalation_renew', result='renewed')
        self._audit(operator=operator, action='escalation_renew', ticket=ticket, symbol=(doc.get('symbol') or ''), sid=(doc.get('sid') or ''), fingerprint=fp, result='renewed', reason=reason, payload=result)
        return result

    def audit_history(
        self,
        *,
        symbol: str = '',
        sid: str = '',
        ticket: str = '',
        operator: str = '',
        action: str = '',
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return filtered audit stream entries for symbol/sid/ticket/operator/action."""
        symbol = (symbol or '').strip().upper()
        sid = (sid or '').strip()
        ticket = (ticket or '').strip()
        operator = (operator or '').strip()
        action = (action or '').strip()
        out: list[dict[str, Any]] = []
        for doc in self._stream_entries(limit=max((limit or 50) * 5, 50)):
            payload_json = dict(doc.get('payload_json') or {})
            doc_symbol = str(doc.get('symbol') or payload_json.get('symbol') or '').strip().upper()
            doc_sid = str(doc.get('sid') or payload_json.get('sid') or payload_json.get('state', {}).get('sid') or '').strip()
            doc_ticket = str(doc.get('ticket') or payload_json.get('ticket') or payload_json.get('renew_ticket') or '').strip()
            doc_operator = str(doc.get('operator') or payload_json.get('operator') or payload_json.get('acked_by') or payload_json.get('renewed_by') or '').strip()
            doc_action = (doc.get('action') or '').strip()
            if symbol and doc_symbol != symbol:
                continue
            if sid and doc_sid != sid:
                continue
            if ticket and doc_ticket != ticket:
                continue
            if operator and doc_operator != operator:
                continue
            if action and doc_action != action:
                continue
            doc['symbol'] = doc_symbol
            if doc_sid:
                doc['sid'] = doc_sid
            if doc_ticket:
                doc['ticket'] = doc_ticket
            if doc_operator:
                doc['operator'] = doc_operator
            out.append(doc)
            if len(out) >= max(limit or 50, 1):
                break
        return out

    def linked_tickets(self, *, symbol: str = '', sid: str = '', limit: int = 20) -> list[dict[str, Any]]:
        """Return tickets referenced in the audit stream for a symbol/sid, with counts."""
        counts: Counter[str] = Counter()
        latest: dict[str, dict[str, Any]] = {}
        for doc in self.audit_history(symbol=symbol, sid=sid, limit=max((limit or 20) * 5, 50)):
            ticket = (doc.get('ticket') or '').strip()
            if not ticket:
                continue
            counts[ticket] += 1
            latest[ticket] = {
                'ticket': ticket,
                'last_action': (doc.get('action') or ''),
                'last_operator': (doc.get('operator') or ''),
                'last_ts_ms': _i(doc.get('ts_ms'), 0),
            }
        out = []
        for ticket, count in counts.most_common(max(limit or 20, 1)):
            item = dict(latest.get(ticket) or {})
            item['count'] = count
            out.append(item)
        return out

    def runbook_dashboard(self, *, limit: int = 50) -> dict[str, Any]:
        """Return operator audit dashboard: active holds, active acks, recent audit, top operators/tickets."""
        holds = self.active_holds(limit=limit)
        acks = self.active_acks(limit=limit)
        recent = self.audit_history(limit=limit)
        op_counts: Counter[str] = Counter()
        ticket_counts: Counter[str] = Counter()
        for doc in recent:
            operator = (doc.get('operator') or '').strip()
            ticket = (doc.get('ticket') or '').strip()
            if operator:
                op_counts[operator] += 1
            if ticket:
                ticket_counts[ticket] += 1
        return {
            'generated_at_ms': _ms_now(),
            'active_holds': holds,
            'active_acks': acks,
            'recent_audit': recent,
            'top_operators': [{'operator': op, 'count': cnt} for op, cnt in op_counts.most_common(10)],
            'top_tickets': [{'ticket': tk, 'count': cnt} for tk, cnt in ticket_counts.most_common(10)],
            'counts': {'active_holds': len(holds), 'active_acks': len(acks), 'recent_audit': len(recent)},
        }

    def runbook_state_symbol(self, symbol: str) -> dict[str, Any]:
        """Return a unified runbook state for a symbol.

        Includes: guard state, hold state, triage result, escalation ack state,
        ticket-linked history, runbook audit history.
        """
        symbol = (symbol or '').strip().upper()
        triaged = self._resolve_triage(symbol=symbol, include_exchange=True)
        fingerprint = str(dict((triaged or {}).get('policy') or {}).get('fingerprint') or '').strip()
        return {
            'symbol': symbol,
            'guard': self.store.load_view(symbol) if hasattr(self.store, 'load_view') else guard_view(self.store.load_raw(symbol)),
            'hold': self.hold_state(symbol),
            'triage': triaged,
            'escalation': self.escalation_state(fingerprint),
            'ticket_history': self.linked_tickets(symbol=symbol),
            'runbook_history': self.audit_history(symbol=symbol, limit=50),
        }

    def runbook_state_sid(self, sid: str) -> dict[str, Any]:
        """Return a unified runbook state for a sid.

        Resolves symbol from triage result, then delegates to runbook_state_symbol.
        """
        triaged = self._resolve_triage(sid=sid, include_exchange=True)
        summary = dict((triaged or {}).get('summary') or {})
        symbol = (summary.get('symbol') or '').strip().upper()
        payload = self.runbook_state_symbol(symbol) if symbol else {
            'symbol': '', 'guard': {}, 'hold': {}, 'triage': triaged, 'escalation': {},
            'ticket_history': [], 'runbook_history': [],
        }
        payload['sid'] = (sid or '')
        payload['runbook_history'] = self.audit_history(sid=sid, limit=50)
        payload['ticket_history'] = self.linked_tickets(sid=sid)
        return payload
