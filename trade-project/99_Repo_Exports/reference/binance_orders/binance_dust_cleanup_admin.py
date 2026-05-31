#!/usr/bin/env python3
from __future__ import annotations

"""Manual/admin control plane for Binance dust cleanup sweep.

Purpose
-------
Expose safe operational controls around the dust sweep worker without touching
its cleanup logic:
* add/remove denylist symbols
* clear per-symbol cleanup cooldowns
* inspect current denylist/cooldown state
* emit an audit trail with operator/reason/ticket metadata

All controls write to the same Redis keys already consumed by
`binance_dust_cleanup_worker.py`, so the worker automatically respects them.
"""

import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
TICK_ROOT = os.path.join(REPO_ROOT, 'tick_flow_full')
for _p in (REPO_ROOT, TICK_ROOT):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from services.execution_metrics import (
        EXECUTION_DUST_ADMIN_ACTION_TOTAL,
        EXECUTION_DUST_ADMIN_STATE_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (
            EXECUTION_DUST_ADMIN_ACTION_TOTAL,
            EXECUTION_DUST_ADMIN_STATE_TOTAL,
        )
    except Exception:  # pragma: no cover
        EXECUTION_DUST_ADMIN_ACTION_TOTAL = None  # type: ignore
        EXECUTION_DUST_ADMIN_STATE_TOTAL = None  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_symbol(symbol: str) -> str:
    target = str(symbol or '').upper().strip()
    if not target:
        raise ValueError('symbol_required')
    return target


class BinanceDustCleanupAdmin:
    def __init__(self, *, redis_client: Any = None) -> None:
        if redis_client is not None:
            self.r = redis_client
        elif redis is not None and os.getenv('REDIS_URL'):
            self.r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True)
        else:
            self.r = None
        self.static_denylist = {s.strip().upper() for s in str(os.getenv('BINANCE_DUST_SWEEP_DENYLIST', '')).split(',') if s.strip()}
        self.dynamic_denylist_set_key = os.getenv('BINANCE_DUST_SWEEP_DENYLIST_SET_KEY', 'orders:dust_cleanup:denylist')
        self.dynamic_denylist_prefix = os.getenv('BINANCE_DUST_SWEEP_DENYLIST_PREFIX', 'orders:dust_cleanup:denylist:')
        self.cooldown_prefix = os.getenv('BINANCE_DUST_SWEEP_COOLDOWN_PREFIX', 'orders:dust_cleanup:cooldown:')
        self.audit_stream = os.getenv('BINANCE_DUST_ADMIN_AUDIT_STREAM', 'orders:dust_cleanup:audit')
        self.audit_stream_maxlen = max(0, int(os.getenv('BINANCE_DUST_ADMIN_AUDIT_STREAM_MAXLEN', '10000') or '0')) or None

    def _metric_action(self, action: str, result: str) -> None:
        if EXECUTION_DUST_ADMIN_ACTION_TOTAL is None:
            return
        try:
            EXECUTION_DUST_ADMIN_ACTION_TOTAL.labels(action=str(action), result=str(result)).inc()
        except Exception:
            pass

    def _metric_state(self, kind: str, value: int) -> None:
        if EXECUTION_DUST_ADMIN_STATE_TOTAL is None:
            return
        try:
            EXECUTION_DUST_ADMIN_STATE_TOTAL.labels(kind=str(kind)).set(float(max(0, int(value))))
        except Exception:
            pass

    def _audit(self, *, action: str, symbol: str, operator: str, reason: str, ticket: str, result: str, payload: Optional[Dict[str, Any]] = None) -> None:
        if self.r is None:
            return
        fields = {
            'ts_ms': str(_now_ms()),
            'action': str(action),
            'symbol': str(symbol),
            'operator': str(operator or ''),
            'reason': str(reason or ''),
            'ticket': str(ticket or ''),
            'result': str(result),
            'payload_json': json.dumps(dict(payload or {}), ensure_ascii=False, separators=(',', ':')),
        }
        try:
            kwargs: Dict[str, Any] = {}
            if self.audit_stream_maxlen:
                kwargs = {'maxlen': self.audit_stream_maxlen, 'approximate': True}
            self.r.xadd(self.audit_stream, fields, **kwargs, maxlen=50000)
        except Exception:
            pass

    def _dynamic_denylist_key(self, symbol: str) -> str:
        return f"{self.dynamic_denylist_prefix}{_normalize_symbol(symbol)}"

    def _cooldown_key(self, symbol: str) -> str:
        return f"{self.cooldown_prefix}{_normalize_symbol(symbol)}"

    def _scan_keys(self, prefix: str) -> List[str]:
        if self.r is None:
            return []
        patt = f"{prefix}*"
        try:
            if hasattr(self.r, 'scan_iter'):
                return sorted(str(k) for k in self.r.scan_iter(match=patt))
        except Exception:
            pass
        try:
            if hasattr(self.r, 'keys'):
                return sorted(str(k) for k in self.r.keys(patt))
        except Exception:
            pass
        return []

    def _smembers(self, key: str) -> Set[str]:
        if self.r is None:
            return set()
        try:
            if hasattr(self.r, 'smembers'):
                return {str(v) for v in (self.r.smembers(key) or set())}
        except Exception:
            pass
        return set()

    def _pttl(self, key: str) -> int:
        if self.r is None:
            return -2
        try:
            if hasattr(self.r, 'pttl'):
                return int(self.r.pttl(key) or -2)
        except Exception:
            pass
        return -2

    def _get(self, key: str) -> Optional[str]:
        if self.r is None:
            return None
        try:
            return self.r.get(key)
        except Exception:
            return None

    def _delete(self, key: str) -> None:
        if self.r is None:
            return
        try:
            self.r.delete(key)
        except Exception:
            pass

    def _parse_json_doc(self, raw: Optional[str]) -> Dict[str, Any]:
        if raw in (None, ''):
            return {}
        try:
            return dict(json.loads(str(raw)))
        except Exception:
            return {'raw': raw}

    def _cooldown_doc(self, symbol: str) -> Dict[str, Any]:
        target = _normalize_symbol(symbol)
        key = self._cooldown_key(target)
        raw = self._get(key)
        ttl_ms = self._pttl(key)
        doc = self._parse_json_doc(raw)
        until_ms = int(doc.get('until_ms') or 0)
        remaining_ms = ttl_ms if ttl_ms > 0 else max(0, until_ms - _now_ms())
        return {
            'symbol': target,
            'key': key,
            'exists': raw not in (None, ''),
            'reason': str(doc.get('reason') or ''),
            'until_ms': until_ms,
            'remaining_sec': int((remaining_ms + 999) // 1000) if remaining_ms > 0 else 0,
            'payload': doc,
        }

    def _dynamic_denylist_doc(self, symbol: str) -> Dict[str, Any]:
        target = _normalize_symbol(symbol)
        key = self._dynamic_denylist_key(target)
        raw = self._get(key)
        ttl_ms = self._pttl(key)
        doc = self._parse_json_doc(raw)
        return {
            'symbol': target,
            'key': key,
            'exists': raw not in (None, '', '0', 'false', 'False'),
            'ttl_sec': int((ttl_ms + 999) // 1000) if ttl_ms > 0 else 0,
            'payload': doc,
        }

    def add_denylist_symbol(self, symbol: str, *, operator: str, reason: str, ticket: str, ttl_sec: Optional[int] = None) -> Dict[str, Any]:
        if self.r is None:
            raise RuntimeError('redis_unavailable')
        target = _normalize_symbol(symbol)
        ttl = int(ttl_sec or 0)
        payload = {
            'symbol': target,
            'operator': str(operator or ''),
            'reason': str(reason or ''),
            'ticket': str(ticket or ''),
            'ts_ms': _now_ms(),
            'ttl_sec': ttl,
        }
        try:
            self.r.sadd(self.dynamic_denylist_set_key, target)
            if ttl > 0:
                if hasattr(self.r, 'setex'):
                    self.r.setex(self._dynamic_denylist_key(target), ttl, json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
                else:
                    self.r.set(self._dynamic_denylist_key(target), json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
            else:
                self._delete(self._dynamic_denylist_key(target))
            result = {'ok': True, 'symbol': target, 'ttl_sec': ttl, 'scope': 'dynamic_denylist'}
            self._metric_action('add_denylist', 'ok')
            self._audit(action='add_denylist', symbol=target, operator=operator, reason=reason, ticket=ticket, result='ok', payload=result)
            return result
        except Exception as exc:
            self._metric_action('add_denylist', 'error')
            self._audit(action='add_denylist', symbol=target, operator=operator, reason=reason, ticket=ticket, result='error', payload={'error': str(exc)})
            raise

    def remove_denylist_symbol(self, symbol: str, *, operator: str, reason: str, ticket: str) -> Dict[str, Any]:
        if self.r is None:
            raise RuntimeError('redis_unavailable')
        target = _normalize_symbol(symbol)
        try:
            if hasattr(self.r, 'srem'):
                self.r.srem(self.dynamic_denylist_set_key, target)
            self._delete(self._dynamic_denylist_key(target))
            result = {'ok': True, 'symbol': target, 'scope': 'dynamic_denylist'}
            self._metric_action('remove_denylist', 'ok')
            self._audit(action='remove_denylist', symbol=target, operator=operator, reason=reason, ticket=ticket, result='ok', payload=result)
            return result
        except Exception as exc:
            self._metric_action('remove_denylist', 'error')
            self._audit(action='remove_denylist', symbol=target, operator=operator, reason=reason, ticket=ticket, result='error', payload={'error': str(exc)})
            raise

    def clear_cooldown(self, symbol: str, *, operator: str, reason: str, ticket: str) -> Dict[str, Any]:
        if self.r is None:
            raise RuntimeError('redis_unavailable')
        target = _normalize_symbol(symbol)
        key = self._cooldown_key(target)
        before = self._cooldown_doc(target)
        try:
            self._delete(key)
            result = {'ok': True, 'symbol': target, 'cleared': bool(before.get('exists')), 'previous_remaining_sec': int(before.get('remaining_sec') or 0)}
            self._metric_action('clear_cooldown', 'ok')
            self._audit(action='clear_cooldown', symbol=target, operator=operator, reason=reason, ticket=ticket, result='ok', payload=result)
            return result
        except Exception as exc:
            self._metric_action('clear_cooldown', 'error')
            self._audit(action='clear_cooldown', symbol=target, operator=operator, reason=reason, ticket=ticket, result='error', payload={'error': str(exc)})
            raise

    def symbol_state(self, symbol: str) -> Dict[str, Any]:
        target = _normalize_symbol(symbol)
        dyn_set = target in self._smembers(self.dynamic_denylist_set_key)
        dyn_doc = self._dynamic_denylist_doc(target)
        cooldown = self._cooldown_doc(target)
        out = {
            'symbol': target,
            'static_denylisted': target in self.static_denylist,
            'dynamic_set_member': dyn_set,
            'dynamic_override': dyn_doc,
            'cooldown': cooldown,
            'effective_denylisted': bool(target in self.static_denylist or dyn_set or dyn_doc.get('exists')),
        }
        return out

    def current_state(self) -> Dict[str, Any]:
        dynamic_set = sorted(self._smembers(self.dynamic_denylist_set_key))
        dynamic_docs = [self._dynamic_denylist_doc(k.replace(self.dynamic_denylist_prefix, '', 1)) for k in self._scan_keys(self.dynamic_denylist_prefix)]
        cooldown_docs = [self._cooldown_doc(k.replace(self.cooldown_prefix, '', 1)) for k in self._scan_keys(self.cooldown_prefix)]
        out = {
            'static_denylist': sorted(self.static_denylist),
            'dynamic_denylist_set_key': self.dynamic_denylist_set_key,
            'dynamic_denylist_members': dynamic_set,
            'dynamic_denylist_overrides': dynamic_docs,
            'cooldowns': cooldown_docs,
        }
        self._metric_state('static_denylist', len(out['static_denylist']))
        self._metric_state('dynamic_denylist_members', len(dynamic_set))
        self._metric_state('dynamic_denylist_overrides', len(dynamic_docs))
        self._metric_state('cooldowns', len(cooldown_docs))
        return out

    def recent_audit(self, *, symbol: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        if self.r is None or limit <= 0:
            return []
        rows: List[Dict[str, Any]] = []
        try:
            if hasattr(self.r, 'xrevrange'):
                raw = self.r.xrevrange(self.audit_stream, count=int(limit))
                for item_id, fields in raw:
                    doc = {'id': str(item_id)}
                    doc.update({str(k): v for k, v in dict(fields).items()})
                    if symbol and str(doc.get('symbol') or '').upper() != str(symbol).upper():
                        continue
                    rows.append(doc)
        except Exception:
            return []
        return rows


__all__ = ['BinanceDustCleanupAdmin']
