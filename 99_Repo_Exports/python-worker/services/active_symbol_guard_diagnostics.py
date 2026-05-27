from __future__ import annotations

import json
import math
import os
from collections import Counter
from collections.abc import Iterable
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover,
    from services.active_symbol_guard_semantics import guard_view
    from services.active_symbol_guard_store import ActiveSymbolGuardStore
    from services.execution_metrics import (
        EXECUTION_ACTIVE_SYMBOL_GUARD_RACE_CHAIN_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_SNAPSHOT_TOTAL,
    )
except Exception:  # pragma: no cover,
    from active_symbol_guard_semantics import guard_view  # type: ignore
    from active_symbol_guard_store import ActiveSymbolGuardStore  # type: ignore
    from execution_metrics import (  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_RACE_CHAIN_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_SNAPSHOT_TOTAL,
    )


def _ms_now() -> int:
    return get_ny_time_millis()


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _normalize(obj: Any) -> Any:
    """Recursively decode bytes from Redis responses."""
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='replace')
    if isinstance(obj, dict):
        return {str(_normalize(k)): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize(x) for x in obj]
    return obj


class ActiveSymbolGuardDiagnostics:
    """Health-report and incident-debug helper for active-symbol guards.

    This is the shared read-only contract for exporter endpoints, CLI tools and
    future runbooks. All readers should go through this helper instead of
    ad-hoc Redis GET + manual field interpretation.
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        client: Any | None = None,
        active_symbol_key_prefix: str = 'orders:active_symbol_sid:',
        state_key_prefix: str = 'orders:state:',
        state_ttl_sec: int = 86400,
        tombstone_ttl_sec: int = 120,
        stale_tombstone_ms: int | None = None,
        hot_symbol_limit: int = 10,
        # P13: runbook audit integration — prefixes mirror RunbookExecutor defaults
        hold_key_prefix: str = 'orders:active_symbol_guard:hold:symbol:',
        escalation_key_prefix: str = 'orders:active_symbol_guard:incident:ack:',
        audit_stream: str = 'orders:active_symbol_guard:audit',
    ) -> None:
        self.r = redis_client
        self.client = client
        self.active_symbol_key_prefix = (active_symbol_key_prefix or 'orders:active_symbol_sid:').rstrip(':') + ':'
        self.state_key_prefix = (state_key_prefix or 'orders:state:').rstrip(':') + ':'
        self.state_ttl_sec = max(int(state_ttl_sec or 86400), 1)
        self.tombstone_ttl_sec = max(int(tombstone_ttl_sec or 120), 1)
        self.stale_tombstone_ms = max(int(stale_tombstone_ms or os.getenv('ACTIVE_SYMBOL_GUARD_STALE_TOMBSTONE_MS', '600000')), 1)
        self.hot_symbol_limit = max(int(hot_symbol_limit or 10), 1)
        self.window_5m_ms = int(os.getenv('ACTIVE_SYMBOL_GUARD_HEATMAP_5M_MS', '300000'))
        self.window_1h_ms = int(os.getenv('ACTIVE_SYMBOL_GUARD_HEATMAP_1H_MS', '3600000'))
        self.timeline_limit = max(int(os.getenv('ACTIVE_SYMBOL_GUARD_TIMELINE_LIMIT', '20')), 1)
        self.hold_key_prefix = (hold_key_prefix or 'orders:active_symbol_guard:hold:symbol:')
        self.escalation_key_prefix = (escalation_key_prefix or 'orders:active_symbol_guard:incident:ack:')
        self.audit_stream = (audit_stream or 'orders:active_symbol_guard:audit')
        self.store = ActiveSymbolGuardStore(
            self.r,
            key_prefix=self.active_symbol_key_prefix,
            active_ttl_sec=self.state_ttl_sec,
            tombstone_ttl_sec=self.tombstone_ttl_sec,
        )

    def _load_json(self, key: str) -> dict[str, Any]:
        try:
            raw = self.r.get(key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _iter_symbols(self) -> list[str]:
        return sorted(self.store.list_symbols())

    def _iter_prefix_keys(self, prefix: str) -> list[str]:
        """Scan Redis keys matching prefix*, return sorted unique list."""
        out: list[str] = []
        pattern = f"{prefix}*"
        try:
            if hasattr(self.r, 'scan_iter'):
                out.extend([str(_normalize(k)) for k in self.r.scan_iter(pattern, count=500)])
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
                items = self.r.xrevrange(self.audit_stream, count=max(int(limit), 1)) or []
            elif hasattr(self.r, 'xrange'):
                raw = self.r.xrange(self.audit_stream, count=max(int(limit), 1)) or []
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
                payload = doc.get('payload')
                if isinstance(payload, str):
                    try:
                        doc['payload_json'] = json.loads(payload)
                    except Exception:
                        doc['payload_json'] = {}
                out.append(doc)
            except Exception:
                continue
        return out

    def _classify(self, view: dict[str, Any]) -> str:
        if view.get('is_released'):
            if int(view.get('tombstone_age_ms') or 0) >= int(self.stale_tombstone_ms):
                return 'stale_tombstone'
            return 'released_tombstone'
        if bool(view.get('guard_release_pending')):
            return 'pending_release'
        if bool(view.get('is_active')):
            return 'active'
        return 'unknown'

    def _compact_doc(self, raw: dict[str, Any], view: dict[str, Any]) -> dict[str, Any]:
        return {
            'symbol': (view.get('symbol') or ''),
            'sid': (view.get('sid') or ''),
            'classification': self._classify(view),
            'status': (view.get('status') or ''),
            'is_blocking': bool(view.get('is_blocking')),
            'guard_release_pending': bool(view.get('guard_release_pending')),
            'state_terminalish': bool(view.get('state_terminalish')),
            'guard_version': int(view.get('guard_version') or 0),
            'guard_writer': (view.get('guard_writer') or ''),
            'guard_release_reason': (view.get('guard_release_reason') or ''),
            'exchange_guard_reason': (raw.get('exchange_guard_reason') or ''),
            'updated_at_ms': int(view.get('updated_at_ms') or 0),
            'released_at_ms': int(view.get('released_at_ms') or 0),
            'tombstone_age_ms': int(view.get('tombstone_age_ms') or 0),
        }

    def _read_exchange_truth(self, symbol: str) -> dict[str, Any]:
        symbol = (symbol or '').strip().upper()
        out: dict[str, Any] = {
            'symbol': symbol,
            'checked_at_ms': _ms_now(),
            'position_amt': 0.0,
            'has_live_position': False,
            'open_plain_orders': 0,
            'open_algo_orders': 0,
            'has_open_orders': False,
            'errors': [],
            'is_reliable': False,
        }
        client = self.client
        if client is None:
            out['errors'] = ['client_unavailable']
            return out
        errors: list[str] = []
        try:
            for pos in client.get_position_risk() or []:
                if str((pos or {}).get('symbol') or '').upper() != symbol:
                    continue
                amt = _f((pos or {}).get('positionAmt'), 0.0)
                out['position_amt'] = amt
                out['has_live_position'] = not math.isclose(float(amt), 0.0, abs_tol=1e-12)
                break
        except Exception as exc:
            errors.append(f'position_risk:{exc.__class__.__name__}')
        try:
            out['open_plain_orders'] = len(list(client.get_open_orders(symbol) or []))
        except Exception as exc:
            errors.append(f'open_orders:{exc.__class__.__name__}')
        try:
            out['open_algo_orders'] = len(list(client.get_open_algo_orders(symbol) or []))
        except Exception as exc:
            errors.append(f'open_algo_orders:{exc.__class__.__name__}')
        out['has_open_orders'] = int(out['open_plain_orders']) > 0 or int(out['open_algo_orders']) > 0
        out['errors'] = errors
        out['is_reliable'] = not errors
        out['is_flat'] = bool(out['is_reliable'] and not out['has_live_position'] and not out['has_open_orders'])
        return out

    def _timeline(self, symbol: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        return self.store.get_symbol_timeline(symbol, limit=int(limit or self.timeline_limit))

    def _windowed_hot_symbols(self) -> dict[str, list[dict[str, Any]]]:
        symbols = self._iter_symbols()
        self.store.reset_window_hot_metric(window_label='5m', symbols=symbols)
        self.store.reset_window_hot_metric(window_label='1h', symbols=symbols)
        hot_5m = self.store.rolling_hot_symbols(window_ms=self.window_5m_ms, limit=self.hot_symbol_limit)
        hot_1h = self.store.rolling_hot_symbols(window_ms=self.window_1h_ms, limit=self.hot_symbol_limit)
        return {
            '5m': hot_5m,
            '1h': hot_1h,
        }

    def _detect_race_chains(self, symbol: str, *, limit: int = 6) -> list[dict[str, Any]]:
        timeline = self._timeline(symbol, limit=max(int(limit or 6) * 3, 12))
        out: list[dict[str, Any]] = []
        for idx in range(1, len(timeline)):
            prev = timeline[idx - 1]
            cur = timeline[idx]
            prev_writer = (prev.get('writer') or '')
            cur_writer = (cur.get('writer') or '')
            prev_type = (prev.get('event_type') or '')
            cur_type = (cur.get('event_type') or '')
            same_symbol = (prev.get('symbol') or '').upper() == (cur.get('symbol') or '').upper()
            if not same_symbol:
                continue
            chain_type = ''
            if prev_type == 'cas_conflict' and cur_type == 'guard_refresh' and prev_writer != cur_writer:
                chain_type = 'conflict_then_other_writer_refresh'
            elif prev_type == 'guard_released' and cur_type == 'cas_conflict':
                chain_type = 'release_then_conflict'
            elif prev_type == 'cas_conflict' and cur_type == 'cas_conflict' and prev_writer != cur_writer:
                chain_type = 'multi_writer_conflict_burst'
            elif prev_type == 'resurrection_attempt':
                chain_type = 'resurrection_attempt'
            if not chain_type:
                continue
            chain = {
                'symbol': symbol,
                'chain_type': chain_type,
                'first': prev,
                'second': cur,
            }
            out.append(chain)
        chains = out[-max(int(limit or 6), 1):]
        counts: dict[str, int] = {}
        for chain in chains:
            ctype = str((chain or {}).get('chain_type') or '')
            counts[ctype] = int(counts.get(ctype) or 0) + 1
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RACE_CHAIN_TOTAL is not None:
                for ctype, count in counts.items():
                    EXECUTION_ACTIVE_SYMBOL_GUARD_RACE_CHAIN_TOTAL.labels(symbol=(symbol or '').upper(), chain_type=ctype).set(int(count))
        except Exception:
            pass
        return chains

    def _runbook_hold_state(self, symbol: str) -> dict[str, Any]:
        """Load current manual hold state for a symbol (read-only helper for diagnostics)."""
        symbol = (symbol or '').strip().upper()
        if not symbol:
            return {}
        doc = self._load_json(f'{self.hold_key_prefix}{symbol}')
        if not doc:
            return {}
        exp = _i(doc.get('expires_at_ms'), 0)
        doc['is_active'] = bool((doc.get('hold_status') or 'active') == 'active' and (exp <= 0 or exp > _ms_now()))
        return doc

    def _runbook_ack_state(self, fingerprint: str) -> dict[str, Any]:
        """Load current escalation ack state for a fingerprint (read-only helper for diagnostics)."""
        fingerprint = (fingerprint or '').strip()
        if not fingerprint:
            return {}
        doc = self._load_json(f'{self.escalation_key_prefix}{fingerprint}')
        if not doc:
            return {}
        exp = _i(doc.get('expires_at_ms'), 0)
        doc['is_active'] = bool(exp <= 0 or exp > _ms_now())
        return doc

    def runbook_history(self, *, symbol: str = '', sid: str = '', ticket: str = '', operator: str = '', limit: int = 50) -> list[dict[str, Any]]:
        """Return filtered audit stream entries for the runbook audit history."""
        symbol = (symbol or '').strip().upper()
        sid = (sid or '').strip()
        ticket = (ticket or '').strip()
        operator = (operator or '').strip()
        out: list[dict[str, Any]] = []
        for doc in self._stream_entries(limit=max(int(limit or 50) * 5, 50)):
            payload_json = dict(doc.get('payload_json') or {})
            doc_symbol = str(doc.get('symbol') or payload_json.get('symbol') or '').strip().upper()
            doc_sid = str(doc.get('sid') or payload_json.get('sid') or payload_json.get('state', {}).get('sid') or '').strip()
            doc_ticket = str(doc.get('ticket') or payload_json.get('ticket') or payload_json.get('renew_ticket') or '').strip()
            doc_operator = str(doc.get('operator') or payload_json.get('operator') or payload_json.get('acked_by') or payload_json.get('renewed_by') or '').strip()
            if symbol and doc_symbol != symbol:
                continue
            if sid and doc_sid != sid:
                continue
            if ticket and doc_ticket != ticket:
                continue
            if operator and doc_operator != operator:
                continue
            doc['symbol'] = doc_symbol
            if doc_sid:
                doc['sid'] = doc_sid
            if doc_ticket:
                doc['ticket'] = doc_ticket
            if doc_operator:
                doc['operator'] = doc_operator
            out.append(doc)
            if len(out) >= max(int(limit or 50), 1):
                break
        return out

    def linked_tickets(self, *, symbol: str = '', sid: str = '', limit: int = 20) -> list[dict[str, Any]]:
        """Return tickets referenced in runbook audit history for a symbol/sid, with occurrence counts."""
        counts: Counter[str] = Counter()
        latest: dict[str, dict[str, Any]] = {}
        for doc in self.runbook_history(symbol=symbol, sid=sid, limit=max(int(limit or 20) * 5, 50)):
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
        for ticket, count in counts.most_common(max(int(limit or 20), 1)):
            item = dict(latest.get(ticket) or {})
            item['count'] = int(count)
            out.append(item)
        return out

    def operator_dashboard(self, *, limit: int = 50) -> dict[str, Any]:
        """Operator audit dashboard: active holds, active acks, recent runbook history, top operators."""
        holds: list[dict[str, Any]] = []
        for key in self._iter_prefix_keys(self.hold_key_prefix):
            doc = self._load_json(key)
            if not doc:
                continue
            doc['symbol'] = str(doc.get('symbol') or key[len(self.hold_key_prefix):] or '').strip().upper()
            exp = _i(doc.get('expires_at_ms'), 0)
            doc['is_active'] = bool((doc.get('hold_status') or 'active') == 'active' and (exp <= 0 or exp > _ms_now()))
            if doc['is_active']:
                holds.append(doc)
        acks: list[dict[str, Any]] = []
        for key in self._iter_prefix_keys(self.escalation_key_prefix):
            doc = self._load_json(key)
            if not doc:
                continue
            exp = _i(doc.get('expires_at_ms'), 0)
            doc['is_active'] = bool(exp <= 0 or exp > _ms_now())
            if doc['is_active']:
                acks.append(doc)
        holds.sort(key=lambda d: (-_i(d.get('updated_at_ms') or d.get('applied_at_ms'), 0), (d.get('symbol') or '')))
        acks.sort(key=lambda d: (-_i(d.get('updated_at_ms') or d.get('acked_at_ms'), 0), (d.get('symbol') or '')))
        history = self.runbook_history(limit=limit)
        op_counts: Counter[str] = Counter()
        ticket_counts: Counter[str] = Counter()
        for doc in history:
            op = (doc.get('operator') or '').strip()
            tk = (doc.get('ticket') or '').strip()
            if op:
                op_counts[op] += 1
            if tk:
                ticket_counts[tk] += 1
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL.labels(kind='hold', status='active').set(len(holds))
                EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_STATE_TOTAL.labels(kind='ack', status='active').set(len(acks))
        except Exception:
            pass
        return {
            'generated_at_ms': _ms_now(),
            'active_holds': holds[:max(int(limit or 50), 1)],
            'active_acks': acks[:max(int(limit or 50), 1)],
            'recent_audit': history,
            'top_operators': [{'operator': op, 'count': cnt} for op, cnt in op_counts.most_common(10)],
            'top_tickets': [{'ticket': tk, 'count': cnt} for tk, cnt in ticket_counts.most_common(10)],
        }

    def _telegram_text(self, *, symbol: str, classification: str, hotness_5m: int, hotness_1h: int, race_chains: list[dict[str, Any]], exchange_truth: dict[str, Any] | None, hold: dict[str, Any] | None = None, ack: dict[str, Any] | None = None) -> str:
        exchange_part = ''
        if isinstance(exchange_truth, dict) and exchange_truth:
            exchange_part = (
                f"\nExchange: pos={exchange_truth.get('position_amt')} plain={exchange_truth.get('open_plain_orders')} algo={exchange_truth.get('open_algo_orders')} reliable={exchange_truth.get('is_reliable')}"
            )
        hold_part = ''
        if isinstance(hold, dict) and hold.get('is_active'):
            hold_part = f"\nHold: active ticket={hold.get('ticket')} operator={hold.get('operator')}"
        ack_part = ''
        if isinstance(ack, dict) and ack.get('is_active'):
            ack_part = f"\nAck: active ticket={ack.get('ticket')} acked_by={ack.get('acked_by')}"
        return (
            f"[active_symbol incident] {symbol}\n"
            f"classification={classification} hot_5m={int(hotness_5m)} hot_1h={int(hotness_1h)} race_chains={len(race_chains)}"
            f"{exchange_part}{hold_part}{ack_part}"
        )

    def incident_bundle_symbol(self, symbol: str, *, include_exchange: bool = False) -> dict[str, Any]:
        symbol = (symbol or '').strip().upper()
        base = self.debug_symbol(symbol, include_exchange=include_exchange)
        timeline = self._timeline(symbol)
        race_chains = self._detect_race_chains(symbol)
        hot = self._windowed_hot_symbols()
        hot_5m = next((int(item.get('count') or 0) for item in hot.get('5m', []) if (item.get('symbol') or '') == symbol), 0)
        hot_1h = next((int(item.get('count') or 0) for item in hot.get('1h', []) if (item.get('symbol') or '') == symbol), 0)
        severity = 'info'
        if race_chains or hot_5m >= 3:
            severity = 'warning'
        if hot_5m >= 5 or any(str((c or {}).get('chain_type') or '') == 'resurrection_attempt' for c in race_chains):
            severity = 'critical'
        exchange_truth = base.get('exchange_truth') if include_exchange else None
        runbook = dict(base.get('runbook') or {})
        hold = dict(runbook.get('hold') or {})
        ack = dict(runbook.get('ack') or {})
        summary = {
            'symbol': symbol,
            'sid': str(base.get('guard_view', {}).get('sid') or ''),
            'classification': (base.get('classification') or ''),
            'severity': severity,
            'hotness': {'5m': int(hot_5m), '1h': int(hot_1h)},
            'race_chain_count': len(race_chains),
            'manual_hold_active': bool(hold.get('is_active')),
        }
        bundle = {
            'summary': summary,
            'guard': base.get('guard_view') or {},
            'state': base.get('state') or {},
            'exchange_truth': exchange_truth or {},
            'last_writer_timeline': timeline,
            'suspicious_writer_race_chains': race_chains,
            'runbook': runbook,
            'ticket_linked_history': list(runbook.get('ticket_history') or []),
            'telegram_text': self._telegram_text(
                symbol=symbol,
                classification=(base.get('classification') or ''),
                hotness_5m=hot_5m,
                hotness_1h=hot_1h,
                race_chains=race_chains,
                exchange_truth=exchange_truth if isinstance(exchange_truth, dict) else None,
                hold=hold if isinstance(hold, dict) else None,
                ack=ack if isinstance(ack, dict) else None,
            ),
            'http_payload': {
                'summary': summary,
                'guard_view': base.get('guard_view') or {},
                'state': base.get('state') or {},
                'timeline': timeline,
                'race_chains': race_chains,
                'runbook': runbook,
            },
            'ui_payload': {
                'card': summary,
                'timeline': timeline,
                'race_chains': race_chains,
                'guard': base.get('guard_view') or {},
                'runbook': runbook,
            },
        }
        return bundle

    def incident_bundle_sid(self, sid: str, *, include_exchange: bool = False) -> dict[str, Any]:
        base = self.debug_sid(sid, include_exchange=include_exchange)
        symbol = (base.get('symbol') or '').strip().upper()
        if symbol:
            bundle = self.incident_bundle_symbol(symbol, include_exchange=include_exchange)
        else:
            bundle = {
                'summary': {'symbol': '', 'sid': (sid or ''), 'classification': 'missing_symbol', 'severity': 'warning', 'hotness': {'5m': 0, '1h': 0}, 'race_chain_count': 0},
                'guard': {}, 'state': base.get('state') or {}, 'exchange_truth': {},
                'last_writer_timeline': [], 'suspicious_writer_race_chains': [],
                'runbook': base.get('runbook') or {},
                'ticket_linked_history': list((base.get('runbook') or {}).get('ticket_history') or []),
                'telegram_text': f'[active_symbol incident] sid={sid} symbol=missing',
                'http_payload': base, 'ui_payload': base,
            }
        bundle['summary']['sid'] = (sid or '')
        return bundle

    def heatmap(self) -> dict[str, Any]:
        hot = self._windowed_hot_symbols()
        return {
            'generated_at_ms': _ms_now(),
            'windows': {
                '5m_ms': int(self.window_5m_ms),
                '1h_ms': int(self.window_1h_ms),
            },
            'top_hot_symbols': hot,
        }

    def snapshot(self) -> dict[str, Any]:
        now_ms = _ms_now()
        docs: list[dict[str, Any]] = []
        breakdown = {
            'active': 0,
            'pending_release': 0,
            'released_tombstone': 0,
            'stale_tombstone': 0,
            'unknown': 0,
        }
        ok = True
        errors: list[str] = []
        try:
            symbols = self._iter_symbols()
        except Exception as exc:
            symbols = []
            ok = False
            errors.append(f'list_symbols:{exc.__class__.__name__}')
        for symbol in symbols:
            raw = self.store.load_raw(symbol)
            view = guard_view(raw, now_ms=now_ms)
            cls = self._classify(view)
            breakdown[cls] = int(breakdown.get(cls) or 0) + 1
            docs.append(self._compact_doc(raw, view))
        hot_conflicts = self.store.top_conflict_symbols(limit=self.hot_symbol_limit)
        hot_resurrections = self.store.top_resurrection_symbols(limit=self.hot_symbol_limit)
        heatmap = self.heatmap()
        # P13: include runbook dashboard summary in snapshot
        runbook = self.operator_dashboard(limit=min(self.hot_symbol_limit, 20))
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_SNAPSHOT_TOTAL is not None:
                for status, count in breakdown.items():
                    EXECUTION_ACTIVE_SYMBOL_GUARD_SNAPSHOT_TOTAL.labels(status=status).set(int(count))
        except Exception:
            pass
        return {
            'ok': ok,
            'ready': ok,
            'degraded': bool(int(breakdown.get('stale_tombstone') or 0) > 0),
            'errors': errors,
            'generated_at_ms': now_ms,
            'stale_tombstone_threshold_ms': int(self.stale_tombstone_ms),
            'total_keys': len(docs),
            'breakdown': breakdown,
            'cas_conflict_hot_symbols': hot_conflicts,
            'resurrection_hot_symbols': hot_resurrections,
            'heatmap': heatmap,
            'runbook_dashboard_summary': {
                'active_holds': len(runbook.get('active_holds') or []),
                'active_acks': len(runbook.get('active_acks') or []),
                'recent_audit': len(runbook.get('recent_audit') or []),
            },
            'guards': docs,
        }

    def debug_symbol(self, symbol: str, *, include_exchange: bool = False) -> dict[str, Any]:
        symbol = (symbol or '').strip().upper()
        raw = self.store.load_raw(symbol)
        view = guard_view(raw)
        sid = str(view.get('sid') or raw.get('sid') or '').strip()
        state = self._load_json(f'{self.state_key_prefix}{sid}') if sid else {}
        # P13: include runbook hold state and ticket history in every debug_symbol response
        ticket_history = self.linked_tickets(symbol=symbol)
        runbook = {
            'hold': self._runbook_hold_state(symbol),
            'ticket_history': ticket_history,
            'history': self.runbook_history(symbol=symbol, limit=50),
        }
        payload: dict[str, Any] = {
            'symbol': symbol,
            'guard_raw': raw,
            'guard_view': view,
            'state': state,
            'classification': self._classify(view),
            'cas_conflict_count': int(self.store.get_conflict_counts().get(symbol) or 0),
            'resurrection_attempt_count': int(self.store.get_resurrection_counts().get(symbol) or 0),
            'latest_conflict': self.store.get_latest_conflict_meta(symbol),
            'latest_resurrection_attempt': self.store.get_latest_resurrection_meta(symbol),
            'runbook': runbook,
        }
        if include_exchange:
            payload['exchange_truth'] = self._read_exchange_truth(symbol)
        return payload

    def debug_sid(self, sid: str, *, include_exchange: bool = False) -> dict[str, Any]:
        sid = (sid or '').strip()
        state = self._load_json(f'{self.state_key_prefix}{sid}') if sid else {}
        symbol = (state.get('symbol') or '').strip().upper()
        guard_raw: dict[str, Any] = {}
        if not symbol:
            for sym in self._iter_symbols():
                raw = self.store.load_raw(sym)
                if (raw.get('sid') or '').strip() == sid:
                    symbol = sym
                    guard_raw = raw
                    break
        if symbol and not guard_raw:
            guard_raw = self.store.load_raw(symbol)
        view = guard_view(guard_raw)
        # P13: attach runbook context so callers get full audit linkage
        ticket_history = self.linked_tickets(sid=sid)
        runbook = {
            'hold': self._runbook_hold_state(symbol),
            'ticket_history': ticket_history,
            'history': self.runbook_history(sid=sid, limit=50),
        }
        payload = {
            'sid': sid,
            'state': state,
            'symbol': symbol,
            'guard_raw': guard_raw,
            'guard_view': view,
            'classification': self._classify(view),
            'runbook': runbook,
        }
        if include_exchange and symbol:
            payload['exchange_truth'] = self._read_exchange_truth(symbol)
        return payload
