#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Notification / reminder layer for Binance dust cleanup admin controls.

Responsibilities
----------------
1) Mirror manual admin actions from `orders:dust_cleanup:audit` into the shared
   Telegram notify stream (`notify:telegram` by default).
2) Periodically scan dynamic denylist overrides and cooldown keys to detect:
   - too old denylist entries
   - symbols stuck in a cooldown loop for too long
3) Emit reminder notifications with Redis-backed dedupe so the operator does
   not get paged repeatedly for the same symbol.

This module is intentionally operational and read-only relative to the dust
cleanup worker itself. It does not mutate cleanup state; it only observes and
notifies.
"""

import json
import os
import sys
import time
from typing import Any
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.binance_dust_cleanup_admin_ack import (
        reminder_ack_state,
        should_suppress_reminder,
    )  # P14: ACK suppress/renew layer
except Exception:  # pragma: no cover
    try:
        from binance_dust_cleanup_admin_ack import (  # type: ignore[no-redef]
            reminder_ack_state,
            should_suppress_reminder,
        )
    except Exception:
        should_suppress_reminder = None  # type: ignore[assignment]
        reminder_ack_state = None  # type: ignore[assignment]

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
TICK_ROOT = os.path.join(REPO_ROOT, 'tick_flow_full')
for _p in (REPO_ROOT, TICK_ROOT):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from services.execution_metrics import (
        EXECUTION_DUST_ADMIN_NOTIFY_TOTAL,
        EXECUTION_DUST_ADMIN_OLD_ENTRY_AGE_SEC,
        EXECUTION_DUST_ADMIN_REMINDER_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (
            EXECUTION_DUST_ADMIN_NOTIFY_TOTAL,
            EXECUTION_DUST_ADMIN_OLD_ENTRY_AGE_SEC,
            EXECUTION_DUST_ADMIN_REMINDER_TOTAL,
        )
    except Exception:  # pragma: no cover
        EXECUTION_DUST_ADMIN_NOTIFY_TOTAL = None  # type: ignore
        EXECUTION_DUST_ADMIN_REMINDER_TOTAL = None  # type: ignore
        EXECUTION_DUST_ADMIN_OLD_ENTRY_AGE_SEC = None  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, '1' if default else '0').strip().lower()
    return raw in {'1', 'true', 'yes', 'on', 'y'}


def _normalize_symbol(symbol: str) -> str:
    target = (symbol or '').upper().strip()
    if not target:
        raise ValueError('symbol_required')
    return target


class BinanceDustCleanupAdminNotifier:
    def __init__(
        self,
        *,
        redis_client: Any = None,
        notify_stream: str | None = None,
        audit_stream: str | None = None,
    ) -> None:
        if redis_client is not None:
            self.r = redis_client
        elif redis is not None and os.getenv('REDIS_URL'):
            self.r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True)
        else:
            self.r = None
        self.notify_stream = str(notify_stream or os.getenv('NOTIFY_TELEGRAM_STREAM', RS.NOTIFY_TELEGRAM))
        self.audit_stream = str(audit_stream or os.getenv('BINANCE_DUST_ADMIN_AUDIT_STREAM', 'orders:dust_cleanup:audit'))
        self.audit_cursor_key = os.getenv('BINANCE_DUST_ADMIN_NOTIFY_CURSOR_KEY', 'orders:dust_cleanup:notify:last_id')
        self.dynamic_denylist_set_key = os.getenv('BINANCE_DUST_SWEEP_DENYLIST_SET_KEY', 'orders:dust_cleanup:denylist')
        self.dynamic_denylist_prefix = os.getenv('BINANCE_DUST_SWEEP_DENYLIST_PREFIX', 'orders:dust_cleanup:denylist:')
        self.cooldown_prefix = os.getenv('BINANCE_DUST_SWEEP_COOLDOWN_PREFIX', 'orders:dust_cleanup:cooldown:')
        self.reminder_state_prefix = os.getenv('BINANCE_DUST_ADMIN_REMINDER_STATE_PREFIX', 'orders:dust_cleanup:reminder:state:')
        self.dedupe_prefix = os.getenv('BINANCE_DUST_ADMIN_NOTIFY_DEDUPE_PREFIX', 'orders:dust_cleanup:notify:dedupe:')
        self.notify_maxlen = max(0, int(os.getenv('BINANCE_DUST_ADMIN_NOTIFY_STREAM_MAXLEN', '10000') or '0')) or None
        self.old_denylist_threshold_sec = max(60, int(os.getenv('BINANCE_DUST_ADMIN_OLD_DENYLIST_SEC', '14400') or '14400'))
        self.cooldown_loop_threshold_sec = max(60, int(os.getenv('BINANCE_DUST_ADMIN_COOLDOWN_LOOP_SEC', '1800') or '1800'))
        self.reminder_repeat_sec = max(60, int(os.getenv('BINANCE_DUST_ADMIN_REMINDER_REPEAT_SEC', '3600') or '3600'))
        # P14: renew-window threshold — emit renewal reminder when ACK TTL falls below this
        self.ack_renew_reminder_sec = max(60, int(os.getenv('BINANCE_DUST_ADMIN_ACK_RENEW_REMINDER_SEC', '900') or '900'))
        self.scan_limit = max(1, int(os.getenv('BINANCE_DUST_ADMIN_NOTIFY_SCAN_LIMIT', '200') or '200'))
        self.direct_telegram = _bool_env('BINANCE_DUST_ADMIN_NOTIFY_DIRECT_TELEGRAM', False)
        self._tg = None
        if self.direct_telegram:
            try:
                from services.telegram.telegram_client import TelegramClient  # type: ignore
                self._tg = TelegramClient.from_env()
            except Exception:
                self._tg = None

    # ----- low-level helpers -------------------------------------------------
    def _metric_inc(self, metric: Any, *, labels: dict[str, str]) -> None:
        if metric is None:
            return
        with contextlib.suppress(Exception):
            metric.labels(**labels).inc()

    def _metric_set_age(self, kind: str, symbol: str, age_sec: float) -> None:
        if EXECUTION_DUST_ADMIN_OLD_ENTRY_AGE_SEC is None:
            return
        with contextlib.suppress(Exception):
            EXECUTION_DUST_ADMIN_OLD_ENTRY_AGE_SEC.labels(kind=str(kind), symbol=symbol).set(float(max(0.0, age_sec)))

    def _get(self, key: str) -> str | None:
        if self.r is None:
            return None
        try:
            return self.r.get(key)
        except Exception:
            return None

    def _set(self, key: str, value: str) -> None:
        if self.r is None:
            return
        with contextlib.suppress(Exception):
            self.r.set(key, value)

    def _setex(self, key: str, ttl_sec: int, value: str) -> None:
        if self.r is None:
            return
        try:
            if ttl_sec > 0 and hasattr(self.r, 'setex'):
                self.r.setex(key, ttl_sec, value)
            else:
                self.r.set(key, value)
        except Exception:
            pass

    def _delete(self, key: str) -> None:
        if self.r is None:
            return
        with contextlib.suppress(Exception):
            self.r.delete(key)

    def _pttl(self, key: str) -> int:
        if self.r is None:
            return -2
        try:
            return int(self.r.pttl(key) or -2)
        except Exception:
            return -2

    def _scan_keys(self, prefix: str) -> list[str]:
        if self.r is None:
            return []
        patt = f"{prefix}*"
        try:
            if hasattr(self.r, 'scan_iter'):
                return sorted(str(k) for k in self.r.scan_iter(match=patt, count=500))[: self.scan_limit]
        except Exception:
            pass
        try:
            if hasattr(self.r, 'keys'):
                return sorted(str(k) for k in self.r.keys(patt))[: self.scan_limit]
        except Exception:
            pass
        return []

    def _xrange(self, stream: str, start: str, end: str, count: int) -> list[tuple[str, dict[str, Any]]]:
        if self.r is None:
            return []
        try:
            if hasattr(self.r, 'xrange'):
                rows = self.r.xrange(stream, min=start, max=end, count=count)
                return [(str(i), dict(fields or {})) for i, fields in (rows or [])]
        except TypeError:
            # fakeredis compatibility
            try:
                rows = self.r.xrange(stream, start, end, count)
                return [(str(i), dict(fields or {})) for i, fields in (rows or [])]
            except Exception:
                return []
        except Exception:
            return []
        return []

    def _smembers(self, key: str) -> set[str]:
        if self.r is None:
            return set()
        try:
            if hasattr(self.r, 'smembers'):
                return {str(v) for v in (self.r.smembers(key) or set())}
        except Exception:
            pass
        return set()

    def _xadd_notify(self, text: str, *, symbol: str = '', kind: str = '', payload: dict[str, Any] | None = None, severity: str = 'warn') -> None:
        if self.r is None:
            return
        fields = {
            'text': str(text),
            'source': 'binance_dust_admin_notifier',
            'severity': str(severity),
            'symbol': (symbol or ''),
            'kind': (kind or ''),
            'payload_json': json.dumps(dict(payload or {}), ensure_ascii=False, separators=(',', ':')),
            'ts_ms': str(_now_ms()),
        }
        try:
            kwargs: dict[str, Any] = {}
            if self.notify_maxlen:
                kwargs = {'maxlen': self.notify_maxlen, 'approximate': True}
            self.r.xadd(self.notify_stream, fields, **kwargs, maxlen=50000)
            self._metric_inc(EXECUTION_DUST_ADMIN_NOTIFY_TOTAL, labels={'kind': (kind or 'telegram'), 'result': 'ok'})
        except Exception:
            self._metric_inc(EXECUTION_DUST_ADMIN_NOTIFY_TOTAL, labels={'kind': (kind or 'telegram'), 'result': 'error'})
        if self._tg is not None:
            with contextlib.suppress(Exception):
                self._tg.send_message(text)  # type: ignore

    def _parse_json_doc(self, raw: str | None) -> dict[str, Any]:
        if raw in (None, ''):
            return {}
        try:
            return dict(json.loads(str(raw)))
        except Exception:
            return {'raw': raw}

    def _dedupe_key(self, scope: str, symbol: str) -> str:
        return f"{self.dedupe_prefix}{scope}:{_normalize_symbol(symbol)}"

    def _should_emit_reminder(self, scope: str, symbol: str) -> bool:
        key = self._dedupe_key(scope, symbol)
        existing = self._get(key)
        if existing not in (None, ''):
            return False
        self._setex(key, self.reminder_repeat_sec, str(_now_ms()))
        return True

    def _state_key(self, symbol: str) -> str:
        return f"{self.reminder_state_prefix}{_normalize_symbol(symbol)}"

    def _load_state(self, symbol: str) -> dict[str, Any]:
        return self._parse_json_doc(self._get(self._state_key(symbol)))

    def _save_state(self, symbol: str, doc: dict[str, Any]) -> None:
        self._set(self._state_key(symbol), json.dumps(doc, ensure_ascii=False, separators=(',', ':')))

    def _extract_symbol(self, key: str, prefix: str) -> str:
        return str(key).replace(prefix, '', 1).upper().strip()

    # ----- admin action mirror ----------------------------------------------
    def process_manual_actions_once(self, *, count: int = 100) -> dict[str, Any]:
        last_id = self._get(self.audit_cursor_key) or '0-0'
        rows = self._xrange(self.audit_stream, f'({last_id}', '+', max(1, int(count)))
        processed = 0
        emitted = 0
        new_last = last_id
        for entry_id, fields in rows:
            processed += 1
            new_last = entry_id
            action = (fields.get('action') or '')
            if action not in {'add_denylist', 'remove_denylist', 'clear_cooldown'}:
                continue
            symbol = _normalize_symbol((fields.get('symbol') or ''))
            operator = (fields.get('operator') or '')
            reason = (fields.get('reason') or '')
            ticket = (fields.get('ticket') or '')
            result = (fields.get('result') or '')
            payload = self._parse_json_doc(fields.get('payload_json'))
            text = (
                f"🧹 Dust admin: {action} {symbol}\n"
                f"result={result} operator={operator or '-'} ticket={ticket or '-'}\n"
                f"reason={reason or '-'}"
            )
            self._xadd_notify(
                text,
                symbol=symbol,
                kind=f'manual_{action}',
                severity='info',
                payload={
                    'entry_id': entry_id,
                    'action': action,
                    'symbol': symbol,
                    'operator': operator,
                    'reason': reason,
                    'ticket': ticket,
                    'result': result,
                    'payload': payload,
                },
            )
            emitted += 1
        if new_last != last_id:
            self._set(self.audit_cursor_key, new_last)
        return {'processed': processed, 'emitted': emitted, 'last_id': new_last}

    # ----- reminder scans ----------------------------------------------------
    def _dynamic_override_doc(self, symbol: str) -> dict[str, Any]:
        key = f"{self.dynamic_denylist_prefix}{_normalize_symbol(symbol)}"
        raw = self._get(key)
        ttl_ms = self._pttl(key)
        doc = self._parse_json_doc(raw)
        return {
            'symbol': _normalize_symbol(symbol),
            'key': key,
            'exists': raw not in (None, ''),
            'ttl_ms': ttl_ms,
            'payload': doc,
        }

    def _cooldown_doc(self, symbol: str) -> dict[str, Any]:
        key = f"{self.cooldown_prefix}{_normalize_symbol(symbol)}"
        raw = self._get(key)
        ttl_ms = self._pttl(key)
        doc = self._parse_json_doc(raw)
        until_ms = int(doc.get('until_ms') or 0)
        return {
            'symbol': _normalize_symbol(symbol),
            'key': key,
            'exists': raw not in (None, ''),
            'ttl_ms': ttl_ms,
            'until_ms': until_ms,
            'payload': doc,
        }

    # ----- P14: ACK-aware reminder helpers -----------------------------------
    def _ack_suppression(self, kind: str, symbol: str, fingerprint: str = '') -> dict[str, Any]:
        """Query ACK suppression state; returns {} if the ack module is unavailable."""
        if should_suppress_reminder is None or self.r is None:
            return {"suppressed": False, "reason": "ack_module_unavailable"}
        try:
            return should_suppress_reminder(self.r, kind=kind, symbol=symbol, fingerprint=fingerprint)
        except Exception:
            return {"suppressed": False, "reason": "ack_check_error"}

    def _maybe_emit_renew_reminder(self, kind: str, symbol: str, ack_state: dict[str, Any]) -> None:
        """Emit a renewal reminder when the active ACK TTL is within the renew window."""
        ttl = int(ack_state.get('ttl_sec', -1))
        if ttl < 0 or ttl > self.ack_renew_reminder_sec:
            # TTL is comfortable (or infinite) — no renew reminder needed
            return
        operator = (ack_state.get('operator', ''))
        ticket = (ack_state.get('ticket', ''))
        text = (
            f"⏰ Dust reminder ACK is close to expiry: {symbol}\n"
            f"kind={kind}\n"
            f"operator={operator}\n"
            f"ticket={ticket}\n"
            f"ttl_sec={ttl}"
        )
        dedupe_key = f"{self.dedupe_prefix}renew:{kind}:{symbol}"
        existing = self._get(dedupe_key)
        if existing not in (None, ''):
            return
        # Dedupe renew reminders at half the normal repeat window
        renew_repeat = max(60, self.reminder_repeat_sec // 2)
        self._setex(dedupe_key, renew_repeat, str(_now_ms()))
        self._xadd_notify(text, symbol=symbol, kind=f'ack_renew:{kind}', severity='info',
                          payload={'kind': kind, 'symbol': symbol, 'ttl_sec': ttl, 'operator': operator, 'ticket': ticket})
        try:
            from services.execution_metrics import EXECUTION_DUST_ADMIN_ACK_RENEW_REMINDER_TOTAL  # type: ignore[import]
            if EXECUTION_DUST_ADMIN_ACK_RENEW_REMINDER_TOTAL is not None:
                EXECUTION_DUST_ADMIN_ACK_RENEW_REMINDER_TOTAL.labels(kind=kind, result='sent').inc()
        except Exception:
            pass

    def _emit_old_denylist_reminder(self, symbol: str, age_sec: int, doc: dict[str, Any]) -> bool:
        # P14: check for active operator ACK before emitting reminder
        suppression = self._ack_suppression('old_denylist', symbol)
        if suppression.get('suppressed'):
            ack_state = suppression.get('ack_state') or {}
            self._maybe_emit_renew_reminder('old_denylist', symbol, ack_state)
            self._metric_inc(EXECUTION_DUST_ADMIN_REMINDER_TOTAL, labels={'kind': 'old_denylist', 'result': 'suppressed_ack'})
            return False
        if not self._should_emit_reminder('old_denylist', symbol):
            self._metric_inc(EXECUTION_DUST_ADMIN_REMINDER_TOTAL, labels={'kind': 'old_denylist', 'result': 'deduped'})
            return False
        operator = (doc.get('operator') or '-')
        ticket = (doc.get('ticket') or '-')
        reason = (doc.get('reason') or '-')
        text = (
            f"⚠️ Dust denylist is stale: {symbol}\n"
            f"age={age_sec}s operator={operator} ticket={ticket}\n"
            f"reason={reason}"
        )
        self._xadd_notify(text, symbol=symbol, kind='old_denylist', severity='warn', payload={'symbol': symbol, 'age_sec': age_sec, 'payload': doc})
        self._metric_inc(EXECUTION_DUST_ADMIN_REMINDER_TOTAL, labels={'kind': 'old_denylist', 'result': 'ok'})
        return True

    def _emit_cooldown_loop_reminder(self, symbol: str, loop_age_sec: int, doc: dict[str, Any]) -> bool:
        # P14: check for active operator ACK before emitting reminder
        suppression = self._ack_suppression('cooldown_loop', symbol)
        if suppression.get('suppressed'):
            ack_state = suppression.get('ack_state') or {}
            self._maybe_emit_renew_reminder('cooldown_loop', symbol, ack_state)
            self._metric_inc(EXECUTION_DUST_ADMIN_REMINDER_TOTAL, labels={'kind': 'cooldown_loop', 'result': 'suppressed_ack'})
            return False
        if not self._should_emit_reminder('cooldown_loop', symbol):
            self._metric_inc(EXECUTION_DUST_ADMIN_REMINDER_TOTAL, labels={'kind': 'cooldown_loop', 'result': 'deduped'})
            return False
        reason = (doc.get('reason') or '-')
        text = (
            f"🔁 Dust cooldown loop suspected: {symbol}\n"
            f"loop_age={loop_age_sec}s reason={reason}\n"
            f"The worker keeps seeing cooldown protection on this symbol."
        )
        self._xadd_notify(text, symbol=symbol, kind='cooldown_loop', severity='warn', payload={'symbol': symbol, 'loop_age_sec': loop_age_sec, 'payload': doc})
        self._metric_inc(EXECUTION_DUST_ADMIN_REMINDER_TOTAL, labels={'kind': 'cooldown_loop', 'result': 'ok'})
        return True

    def scan_reminders_once(self) -> dict[str, Any]:
        now_ms = _now_ms()
        denylist_emitted = 0
        cooldown_emitted = 0

        dynamic_symbols = set(self._smembers(self.dynamic_denylist_set_key))
        for key in self._scan_keys(self.dynamic_denylist_prefix):
            dynamic_symbols.add(self._extract_symbol(key, self.dynamic_denylist_prefix))
        seen_dynamic_symbols: set[str] = set()
        for symbol in sorted(dynamic_symbols):
            doc = self._dynamic_override_doc(symbol)
            payload = dict(doc.get('payload') or {})
            ts_ms = int(payload.get('ts_ms') or 0)
            if not ts_ms:
                continue
            seen_dynamic_symbols.add(symbol)
            state = self._load_state(symbol)
            state['denylist_seen'] = True
            self._save_state(symbol, state)
            age_sec = max(0, int((now_ms - ts_ms) / 1000))
            self._metric_set_age('denylist', symbol, float(age_sec))
            if doc.get('exists') and age_sec >= self.old_denylist_threshold_sec:
                if self._emit_old_denylist_reminder(symbol, age_sec, payload):
                    denylist_emitted += 1

        # reset stale age gauges for symbols no longer present in the dynamic denylist
        for key in self._scan_keys(self.reminder_state_prefix):
            symbol = self._extract_symbol(key, self.reminder_state_prefix)
            if symbol in seen_dynamic_symbols:
                continue
            state = self._load_state(symbol)
            if state.get('denylist_seen'):
                state['denylist_seen'] = False
                self._save_state(symbol, state)
                self._metric_set_age('denylist', symbol, 0.0)

        current_cooldown_symbols: set[str] = set()
        for key in self._scan_keys(self.cooldown_prefix):
            symbol = self._extract_symbol(key, self.cooldown_prefix)
            current_cooldown_symbols.add(symbol)
            doc = self._cooldown_doc(symbol)
            payload = dict(doc.get('payload') or {})
            existing_state = self._load_state(symbol)
            first_seen_ms = int(existing_state.get('cooldown_first_seen_ms') or 0) or now_ms
            if not existing_state.get('cooldown_present'):
                first_seen_ms = now_ms
            state = {
                **existing_state,
                'denylist_seen': bool(existing_state.get('denylist_seen')),
                'cooldown_present': True,
                'cooldown_first_seen_ms': first_seen_ms,
                'cooldown_last_seen_ms': now_ms,
            }
            self._save_state(symbol, state)
            loop_age_sec = max(0, int((now_ms - first_seen_ms) / 1000))
            self._metric_set_age('cooldown_loop', symbol, float(loop_age_sec))
            if doc.get('exists') and loop_age_sec >= self.cooldown_loop_threshold_sec:
                if self._emit_cooldown_loop_reminder(symbol, loop_age_sec, payload):
                    cooldown_emitted += 1

        # reset loop tracking for symbols no longer under cooldown
        for key in self._scan_keys(self.reminder_state_prefix):
            symbol = self._extract_symbol(key, self.reminder_state_prefix)
            if symbol in current_cooldown_symbols:
                continue
            state = self._load_state(symbol)
            if not state:
                continue
            state['cooldown_present'] = False
            state['cooldown_first_seen_ms'] = 0
            state['cooldown_last_seen_ms'] = now_ms
            self._save_state(symbol, state)
            self._metric_set_age('cooldown_loop', symbol, 0.0)
        return {
            'denylist_emitted': denylist_emitted,
            'cooldown_emitted': cooldown_emitted,
            'dynamic_symbols': len(dynamic_symbols),
            'cooldown_symbols': len(current_cooldown_symbols),
        }

    def run_once(self) -> dict[str, Any]:
        out1 = self.process_manual_actions_once()
        out2 = self.scan_reminders_once()
        return {'manual_actions': out1, 'reminders': out2}


if __name__ == '__main__':  # pragma: no cover
    import argparse

    ap = argparse.ArgumentParser(description='Binance dust cleanup admin notification/reminder worker')
    ap.add_argument('--once', action='store_true', help='Run exactly one cycle and print JSON result')
    ap.add_argument('--interval-sec', type=int, default=int(os.getenv('BINANCE_DUST_ADMIN_NOTIFY_INTERVAL_SEC', '30') or '30'))
    ns = ap.parse_args()
    svc = BinanceDustCleanupAdminNotifier()
    if ns.once:
        print(json.dumps(svc.run_once(), ensure_ascii=False, indent=2))
        raise SystemExit(0)
    while True:
        try:
            svc.run_once()
        except KeyboardInterrupt:
            raise
        except Exception:
            pass
        time.sleep(max(1, int(ns.interval_sec)))
