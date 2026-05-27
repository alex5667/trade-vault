from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""P7: Atomic/CAS wrapper for the active-symbol guard Redis key.

Key: orders:active_symbol_sid:{SYMBOL}

Design goals
============
- Protect against stale writers deleting or resurrecting a guard after a newer
  write.  Every successful write increments ``guard_version``; a write whose
  ``expected_version`` does not match the current version is silently rejected.
- Keep a short-lived *release tombstone* so a stale writer cannot re-create the
  key after a legitimate release.
- Expose low-level version/lease fields for auditability and metrics.

Runtime semantics
=================
- active guard   → ``guard_status == "active"``
- released guard → ``guard_status == "released"`` with a short TTL tombstone

Callers that want pure blocking semantics should use :meth:`load_active`, not
:meth:`load_raw`.
"""

import json
import uuid
from typing import Any
import contextlib

try:
    from services.active_symbol_guard_semantics import active_guard_doc, guard_view
except Exception:  # pragma: no cover
    from active_symbol_guard_semantics import active_guard_doc, guard_view  # type: ignore

try:
    from services.execution_metrics import (
        EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_WINDOW_HOT_SYMBOLS,
    )
except Exception:  # pragma: no cover
    from execution_metrics import (  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_WINDOW_HOT_SYMBOLS,
    )


def _ms_now() -> int:
    """Current UTC timestamp in milliseconds."""
    return get_ny_time_millis()


def _i(v: Any, default: int = 0) -> int:
    """Safe int cast with fallback."""
    try:
        return int(v)
    except Exception:
        return default


class ActiveSymbolGuardStore:
    """Atomic/CAS wrapper for orders:active_symbol_sid:{symbol}.

    Design goals:
    - protect against stale writers deleting or resurrecting a guard after a newer write
    - keep a short-lived release tombstone so a stale writer cannot recreate the key after release
    - expose low-level version/lease fields for auditability

    Runtime policy:
    - active guard => guard_status == "active"
    - released guard => guard_status == "released" with a short TTL tombstone
    - readers that want blocking semantics should use load_active(...), not load_raw(...)
    """

    # Lua CAS script executed atomically on real Redis.
    # Returns [1, encoded_doc] on success, [0, reason_string] on rejection.
    _CAS_SET_LUA = """\
local key = KEYS[1]
local expected_version = tonumber(ARGV[1])
local expected_sid = ARGV[2]
local expected_lease = ARGV[3]
local ttl_sec = tonumber(ARGV[4])
local payload = ARGV[5]
local cur_raw = redis.call('GET', key)
if not cur_raw then
  if expected_version ~= 0 then
    return {0, 'version_mismatch_absent'}
  end
  local doc = cjson.decode(payload)
  doc['guard_version'] = 1
  redis.call('SET', key, cjson.encode(doc), 'EX', ttl_sec)
  return {1, cjson.encode(doc)}
end
local cur = cjson.decode(cur_raw)
local cur_ver = tonumber(cur['guard_version'] or 0)
local cur_sid = tostring(cur['sid'] or '')
local cur_lease = tostring(cur['guard_lease_token'] or '')
if cur_ver ~= expected_version then
  return {0, 'version_mismatch'}
end
if expected_sid ~= '' and cur_sid ~= expected_sid then
  return {0, 'sid_mismatch'}
end
if expected_lease ~= '' and cur_lease ~= expected_lease then
  return {0, 'lease_mismatch'}
end
local doc = cjson.decode(payload)
doc['guard_version'] = cur_ver + 1
redis.call('SET', key, cjson.encode(doc), 'EX', ttl_sec)
return {1, cjson.encode(doc)}
"""

    def __init__(
        self,
        redis_client: Any,
        *,
        key_prefix: str = 'orders:active_symbol_sid:',
        active_ttl_sec: int = 86400,
        tombstone_ttl_sec: int = 120,
    ) -> None:
        self.r = redis_client
        self.key_prefix = (key_prefix or 'orders:active_symbol_sid:').rstrip(':') + ':'
        self.index_key = self.key_prefix.rstrip(':') + '_index'
        self.active_ttl_sec = max(int(active_ttl_sec or 0), 1)
        self.tombstone_ttl_sec = max(int(tombstone_ttl_sec or 0), 1)
        self.diag_prefix = 'orders:active_symbol_guard:diag:'
        self.timeline_limit = 512
        self.symbol_timeline_limit = 128

    def key(self, symbol: str) -> str:
        """Build the full Redis key for the given symbol."""
        return f"{self.key_prefix}{(symbol or '').strip().upper()}"

    def load_raw(self, symbol: str) -> dict[str, Any]:
        """Load the raw guard document (including tombstones) from Redis.

        Returns {} on missing key or JSON errors — never raises.
        """
        try:
            raw = self.r.get(self.key(symbol))
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def load_view(self, symbol: str) -> dict[str, Any]:
        return guard_view(self.load_raw(symbol))

    def load_active(self, symbol: str) -> dict[str, Any]:
        """Load the guard document only if it represents an *active* guard.

        A released tombstone (``guard_status == "released"``) returns ``{}``.
        Old-style keys without ``guard_status`` are treated as active for
        backward compatibility.
        """
        return active_guard_doc(self.load_raw(symbol))

    def _hash_getall(self, key: str) -> dict[str, Any]:
        try:
            if hasattr(self.r, 'hgetall'):
                raw = self.r.hgetall(key) or {}
                if isinstance(raw, dict):
                    out: dict[str, Any] = {}
                    for k, v in raw.items():
                        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
                        out[kk] = v.decode() if isinstance(v, (bytes, bytearray)) else v
                    return out
            raw_json = self.r.get(key)
            doc = json.loads(raw_json) if raw_json else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _hash_set(self, key: str, field: str, value: Any) -> None:
        try:
            if hasattr(self.r, 'hset'):
                self.r.hset(key, str(field), value)
                return
            doc = self._hash_getall(key)
            doc[str(field)] = value
            self.r.set(key, json.dumps(doc, ensure_ascii=False, default=str))
        except Exception:
            pass

    def _hash_incr(self, key: str, field: str, delta: int = 1) -> int:
        try:
            if hasattr(self.r, 'hincrby'):
                return int(self.r.hincrby(key, str(field), int(delta)))
            doc = self._hash_getall(key)
            cur = _i(doc.get(str(field)), 0) + int(delta)
            doc[str(field)] = cur
            self.r.set(key, json.dumps(doc, ensure_ascii=False, default=str))
            return cur
        except Exception:
            return 0

    def _timeline_key(self) -> str:
        return f'{self.diag_prefix}timeline:global'

    def _symbol_timeline_key(self, symbol: str) -> str:
        return f"{self.diag_prefix}timeline:symbol:{(symbol or '').strip().upper()}"

    def _list_get(self, key: str) -> list[dict[str, Any]]:
        try:
            raw = self.r.get(key)
            arr = json.loads(raw) if raw else []
            return arr if isinstance(arr, list) else []
        except Exception:
            return []

    def _list_set(self, key: str, items: list[dict[str, Any]]) -> None:
        with contextlib.suppress(Exception):
            self.r.set(key, json.dumps(items, ensure_ascii=False, default=str))

    def _append_event(self, *, symbol: str, sid: str = '', writer: str, operation: str, event_type: str, reason: str = '', doc: dict[str, Any] | None = None) -> dict[str, Any]:
        symbol = (symbol or '').strip().upper()
        now_ms = _ms_now()
        base: dict[str, Any] = {
            'ts_ms': now_ms,
            'symbol': symbol,
            'sid': (sid or ''),
            'writer': (writer or ''),
            'operation': (operation or ''),
            'event_type': (event_type or ''),
            'reason': (reason or ''),
        }
        if isinstance(doc, dict) and doc:
            base.update({
                'guard_version': _i(doc.get('guard_version'), 0),
                'guard_status': (doc.get('guard_status') or ''),
                'guard_release_pending': bool(doc.get('guard_release_pending')),
                'guard_writer': (doc.get('guard_writer') or ''),
            })
        gkey = self._timeline_key()
        skey = self._symbol_timeline_key(symbol)
        events = self._list_get(gkey)
        events.append(base)
        if len(events) > int(self.timeline_limit):
            events = events[-int(self.timeline_limit):]
        self._list_set(gkey, events)
        sevents = self._list_get(skey)
        sevents.append(base)
        if len(sevents) > int(self.symbol_timeline_limit):
            sevents = sevents[-int(self.symbol_timeline_limit):]
        self._list_set(skey, sevents)
        return base

    def get_symbol_timeline(self, symbol: str, *, limit: int = 50, since_ms: int | None = None) -> list[dict[str, Any]]:
        symbol = (symbol or '').strip().upper()
        items = self._list_get(self._symbol_timeline_key(symbol))
        if since_ms is not None:
            items = [ev for ev in items if _i(ev.get('ts_ms'), 0) >= int(since_ms)]
        return items[-max(int(limit or 50), 1):]

    def get_global_timeline(self, *, limit: int = 100, since_ms: int | None = None) -> list[dict[str, Any]]:
        items = self._list_get(self._timeline_key())
        if since_ms is not None:
            items = [ev for ev in items if _i(ev.get('ts_ms'), 0) >= int(since_ms)]
        return items[-max(int(limit or 100), 1):]

    def rolling_hot_symbols(self, *, window_ms: int, limit: int = 10) -> list[dict[str, Any]]:
        now_ms = _ms_now()
        since_ms = max(0, int(now_ms - int(window_ms or 0)))
        counts: dict[str, int] = {}
        reasons: dict[str, dict[str, Any]] = {}
        for ev in self.get_global_timeline(limit=int(self.timeline_limit), since_ms=since_ms):
            et = (ev.get('event_type') or '')
            if et not in {'cas_conflict', 'resurrection_attempt'}:
                continue
            symbol = (ev.get('symbol') or '').strip().upper()
            if not symbol:
                continue
            counts[symbol] = int(counts.get(symbol) or 0) + 1
            reasons[symbol] = ev
        out: list[dict[str, Any]] = []
        for symbol, count in sorted(counts.items(), key=lambda kv: (-int(kv[1]), kv[0]))[:max(int(limit or 10), 1)]:
            out.append({
                'symbol': symbol,
                'count': int(count),
                'window_ms': int(window_ms),
                'latest': reasons.get(symbol) or {},
            })
            try:
                if EXECUTION_ACTIVE_SYMBOL_GUARD_WINDOW_HOT_SYMBOLS is not None:
                    label = '5m' if int(window_ms) <= 300000 else '1h' if int(window_ms) <= 3600000 else f"{int(window_ms)}ms"
                    EXECUTION_ACTIVE_SYMBOL_GUARD_WINDOW_HOT_SYMBOLS.labels(window=label, symbol=symbol).set(int(count))
            except Exception:
                pass
        return out

    def reset_window_hot_metric(self, *, window_label: str, symbols: list[str] | None = None) -> None:
        try:
            metric = EXECUTION_ACTIVE_SYMBOL_GUARD_WINDOW_HOT_SYMBOLS
            if metric is None:
                return
            for symbol in list(symbols or self.list_symbols()):
                metric.labels(window=(window_label or ''), symbol=(symbol or '').strip().upper()).set(0)
        except Exception:
            pass

    def _conflict_count_key(self) -> str:
        return f'{self.diag_prefix}cas_conflict_count'

    def _conflict_meta_key(self) -> str:
        return f'{self.diag_prefix}cas_conflict_last'

    def _resurrection_count_key(self) -> str:
        return f'{self.diag_prefix}resurrection_count'

    def _resurrection_meta_key(self) -> str:
        return f'{self.diag_prefix}resurrection_last'

    def get_conflict_counts(self) -> dict[str, int]:
        return {str(k).strip().upper(): _i(v, 0) for k, v in self._hash_getall(self._conflict_count_key()).items()}

    def get_resurrection_counts(self) -> dict[str, int]:
        return {str(k).strip().upper(): _i(v, 0) for k, v in self._hash_getall(self._resurrection_count_key()).items()}

    def get_latest_conflict_meta(self, symbol: str) -> dict[str, Any]:
        raw = self._hash_getall(self._conflict_meta_key()).get((symbol or '').strip().upper())
        try:
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def get_latest_resurrection_meta(self, symbol: str) -> dict[str, Any]:
        raw = self._hash_getall(self._resurrection_meta_key()).get((symbol or '').strip().upper())
        try:
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def top_conflict_symbols(self, *, limit: int = 10) -> list[dict[str, Any]]:
        counts = self.get_conflict_counts()
        out: list[dict[str, Any]] = []
        for sym, count in sorted(counts.items(), key=lambda kv: (-int(kv[1]), kv[0]))[:max(int(limit or 10), 1)]:
            out.append({
                'symbol': sym,
                'count': int(count),
                'latest': self.get_latest_conflict_meta(sym),
            })
        return out

    def top_resurrection_symbols(self, *, limit: int = 10) -> list[dict[str, Any]]:
        counts = self.get_resurrection_counts()
        out: list[dict[str, Any]] = []
        for sym, count in sorted(counts.items(), key=lambda kv: (-int(kv[1]), kv[0]))[:max(int(limit or 10), 1)]:
            out.append({
                'symbol': sym,
                'count': int(count),
                'latest': self.get_latest_resurrection_meta(sym),
            })
        return out

    def list_symbols(self) -> list[str]:
        out = []
        try:
            members = self.r.smembers(self.index_key)
            if members:
                stale = []
                for k in members:
                    symbol = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
                    symbol = symbol.strip().upper()
                    if symbol:
                        if self.r.exists(self.key(symbol)):
                            out.append(symbol)
                        else:
                            stale.append(symbol)
                if stale:
                    try:
                        self.r.srem(self.index_key, *stale)
                    except Exception:
                        pass
                return sorted(set(out))

            # Skip expensive keyspace SCAN when no guards were found recently.
            # Write paths (acquire_or_refresh / mark_released) populate index_key
            # immediately, so missing this window is safe.
            _empty_sentinel = self.key_prefix.rstrip(':') + '_scan_empty'
            if self.r.exists(_empty_sentinel):
                return []

            prefix = f'{self.key_prefix}*'
            # count=500 keeps each SCAN call under slowlog threshold (<1 ms per hop).
            for key in self.r.scan_iter(match=prefix, count=500):
                k = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
                symbol = str(k).replace(self.key_prefix, '', 1).strip().upper()
                if symbol:
                    out.append(symbol)
                    self.r.sadd(self.index_key, symbol)

            if not out:
                # No active guards — cache absence for 90 s to avoid repeated full SCAN.
                try:
                    self.r.set(_empty_sentinel, '1', ex=90)
                except Exception:
                    pass
        except Exception:
            pass
        return sorted(set(out))

    def _record_conflict(self, *, symbol: str, writer: str, operation: str, reason: str) -> None:
        symbol = (symbol or '').strip().upper()
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL.labels(
                    writer=(writer or ''),
                    operation=(operation or ''),
                    reason=(reason or ''),
                ).inc()
        except Exception:
            pass
        self._hash_incr(self._conflict_count_key(), symbol, 1)
        meta = {
            'symbol': symbol,
            'writer': (writer or ''),
            'operation': (operation or ''),
            'reason': (reason or ''),
            'at_ms': _ms_now(),
        }
        self._hash_set(self._conflict_meta_key(), symbol, json.dumps(meta, ensure_ascii=False, default=str))
        self._append_event(symbol=symbol, writer=writer, operation=operation, event_type='cas_conflict', reason=reason, doc=meta)

    def _record_resurrection_attempt(self, *, symbol: str, writer: str, reason: str) -> None:
        symbol = (symbol or '').strip().upper()
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL.labels(
                    writer=(writer or ''),
                    reason=(reason or ''),
                ).inc()
        except Exception:
            pass
        self._hash_incr(self._resurrection_count_key(), symbol, 1)
        meta = {
            'symbol': symbol,
            'writer': (writer or ''),
            'reason': (reason or ''),
            'at_ms': _ms_now(),
        }
        self._hash_set(self._resurrection_meta_key(), symbol, json.dumps(meta, ensure_ascii=False, default=str))
        self._append_event(symbol=symbol, writer=writer, operation='acquire_or_refresh', event_type='resurrection_attempt', reason=reason, doc=meta)

    # ------------------------------------------------------------------
    # Internal CAS helpers
    # ------------------------------------------------------------------

    def _fallback_cas_set(
        self,
        *,
        symbol: str,
        payload_doc: dict[str, Any],
        expected_version: int,
        expected_sid: str,
        expected_lease_token: str,
        ttl_sec: int,
    ) -> dict[str, Any]:
        """Non-atomic Python fallback for FakeRedis / test environments.

        In production, :meth:`_cas_set` uses the Lua script for real atomicity.
        """
        key = self.key(symbol)
        cur = self.load_raw(symbol)
        if not cur:
            if int(expected_version) != 0:
                return {'applied': False, 'reason': 'version_mismatch_absent', 'doc': {}}
            final_doc = dict(payload_doc)
            final_doc['guard_version'] = 1
            self.r.set(key, json.dumps(final_doc, ensure_ascii=False, default=str), ex=int(ttl_sec))
            return {'applied': True, 'reason': 'created', 'doc': final_doc}
        cur_ver = _i(cur.get('guard_version'), 0)
        if cur_ver != int(expected_version):
            return {'applied': False, 'reason': 'version_mismatch', 'doc': cur}
        if expected_sid and (cur.get('sid') or '').strip() != (expected_sid or '').strip():
            return {'applied': False, 'reason': 'sid_mismatch', 'doc': cur}
        if expected_lease_token and (cur.get('guard_lease_token') or '').strip() != (expected_lease_token or '').strip():
            return {'applied': False, 'reason': 'lease_mismatch', 'doc': cur}
        final_doc = dict(payload_doc)
        final_doc['guard_version'] = cur_ver + 1
        self.r.set(key, json.dumps(final_doc, ensure_ascii=False, default=str), ex=int(ttl_sec))
        return {'applied': True, 'reason': 'updated', 'doc': final_doc}

    def _cas_set(
        self,
        *,
        symbol: str,
        payload_doc: dict[str, Any],
        expected_version: int,
        expected_sid: str,
        expected_lease_token: str,
        ttl_sec: int,
    ) -> dict[str, Any]:
        """CAS write: atomically update the guard key iff version/sid/lease match.

        Uses Lua eval on real Redis; falls back to Python for FakeRedis / tests.
        Returns ``{'applied': bool, 'reason': str, 'doc': dict}``.
        """
        key = self.key(symbol)
        if hasattr(self.r, 'eval'):
            try:
                result = self.r.eval(
                    self._CAS_SET_LUA,
                    1,
                    key,
                    int(expected_version),
                    (expected_sid or ''),
                    (expected_lease_token or ''),
                    int(ttl_sec),
                    json.dumps(payload_doc, ensure_ascii=False, default=str),
                )
                ok = bool(result and int(result[0]) == 1)
                if ok:
                    doc = json.loads(result[1]) if len(result) > 1 and result[1] else {}
                    return {'applied': True, 'reason': 'updated', 'doc': doc if isinstance(doc, dict) else {}}
                reason = str(result[1] if len(result) > 1 else 'cas_rejected')
                return {'applied': False, 'reason': reason, 'doc': self.load_raw(symbol)}
            except Exception:
                pass  # fall through to Python fallback
        return self._fallback_cas_set(
            symbol=symbol,
            payload_doc=payload_doc,
            expected_version=expected_version,
            expected_sid=expected_sid,
            expected_lease_token=expected_lease_token,
            ttl_sec=ttl_sec,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire_or_refresh(
        self,
        *,
        symbol: str,
        sid: str,
        payload_patch: dict[str, Any],
        writer: str,
        ttl_sec: int | None = None,
        retry_once: bool = True,
    ) -> dict[str, Any]:
        """Atomically set/update the active-symbol guard for *sid*.

        Rules
        -----
        - If the key is absent → create with version=1 (any writer can claim a
          fresh slot).
        - If the key contains ``guard_status="released"`` and the same sid →
          reject (stale writer cannot resurrect an already-released guard).
        - If the key contains ``guard_status="released"`` and a *different* sid
          → allow take-over (new trade can claim the slot after release).
        - If the key is held by a *different* (non-released) sid → reject with
          ``held_by_other_sid``.
        - On ``version_mismatch`` (concurrent write raced us) → retry once.

        Returns ``{'applied': bool, 'reason': str, 'doc': dict}``.
        """
        symbol = (symbol or '').strip().upper()
        sid = (sid or '').strip()
        now_ms = _ms_now()
        ttl = int(ttl_sec or self.active_ttl_sec)
        current = self.load_raw(symbol)
        current_status = (current.get('guard_status') or 'active').strip().lower()
        current_sid = (current.get('sid') or '').strip()
        expected_version = _i(current.get('guard_version'), 0) if current else 0
        expected_lease = (current.get('guard_lease_token') or '').strip() if current else ''
        expected_sid = ''
        if current:
            if current_status == 'released':
                if current_sid and current_sid == sid:
                    # Stale writer cannot resurrect guard for SAME sid after release tombstone
                    self._record_conflict(symbol=symbol, writer=writer, operation='acquire_or_refresh', reason='released_tombstone_same_sid')
                    self._record_resurrection_attempt(symbol=symbol, writer=writer, reason='released_tombstone_same_sid')
                    return {'applied': False, 'reason': 'released_tombstone_same_sid', 'doc': current}
                # Different sid can take over a released tombstone (new trade claiming the slot)
                expected_sid = ''
            elif current_sid == sid:
                # Same sid refreshing its own guard — CAS on version + sid
                expected_sid = sid
            else:
                # A different active sid already holds the slot
                self._record_conflict(symbol=symbol, writer=writer, operation='acquire_or_refresh', reason='held_by_other_sid')
                return {'applied': False, 'reason': 'held_by_other_sid', 'doc': current}
        # Build the new document: start from current (if same sid & active), then apply patch
        lease_token = f"{writer}:{now_ms}:{uuid.uuid4().hex[:8]}"
        new_doc = dict(current if current_sid == sid and current_status != 'released' else {})
        new_doc.update(payload_patch or {})
        new_doc.update({
            'symbol': symbol,
            'sid': sid,
            'guard_status': 'active',
            'guard_writer': (writer or ''),
            'guard_writer_ts_ms': now_ms,
            'guard_lease_owner': (writer or ''),
            'guard_lease_token': lease_token,
            'guard_lease_epoch_ms': now_ms,
            'updated_at_ms': int((payload_patch or {}).get('updated_at_ms') or now_ms),
        })
        result = self._cas_set(
            symbol=symbol,
            payload_doc=new_doc,
            expected_version=expected_version,
            expected_sid=expected_sid,
            expected_lease_token=expected_lease,
            ttl_sec=ttl,
        )
        if not result.get('applied'):
            self._record_conflict(symbol=symbol, writer=writer, operation='acquire_or_refresh', reason=(result.get('reason') or 'cas_rejected'))
        # On version_mismatch (concurrent writer raced us) retry once with fresh read
        if (not result.get('applied')) and retry_once and (result.get('reason') or '').startswith('version_mismatch'):
            return self.acquire_or_refresh(
                symbol=symbol, sid=sid, payload_patch=payload_patch,
                writer=writer, ttl_sec=ttl, retry_once=False,
            )
        if result.get('applied'):
            try:
                self.r.sadd(self.index_key, symbol)
            except Exception:
                pass
            self._append_event(symbol=symbol, sid=sid, writer=writer, operation='acquire_or_refresh', event_type='guard_refresh', reason=(result.get('reason') or 'updated'), doc=result.get('doc') if isinstance(result.get('doc'), dict) else None)
        return result

    def mark_released(
        self,
        *,
        symbol: str,
        expected_sid: str = '',
        release_reason: str = '',
        writer: str,
        tombstone_ttl_sec: int | None = None,
        extra_patch: dict[str, Any] | None = None,
        retry_once: bool = True,
    ) -> dict[str, Any]:
        """Atomically write a release tombstone for the active-symbol guard.

        The tombstone keeps the key alive for ``tombstone_ttl_sec`` so that
        in-flight stale writers can detect the release and not re-create the
        key for the same sid.

        Returns ``{'applied': bool, 'reason': str, 'doc': dict}``.
        """
        symbol = (symbol or '').strip().upper()
        current = self.load_raw(symbol)
        if not current:
            return {'applied': False, 'reason': 'absent', 'doc': {}}
        current_sid = (current.get('sid') or '').strip()
        if expected_sid and current_sid != (expected_sid or '').strip():
            self._record_conflict(symbol=symbol, writer=writer, operation='mark_released', reason='sid_mismatch')
            return {'applied': False, 'reason': 'sid_mismatch', 'doc': current}
        expected_version = _i(current.get('guard_version'), 0)
        expected_lease = (current.get('guard_lease_token') or '').strip()
        now_ms = _ms_now()
        ttl = int(tombstone_ttl_sec or self.tombstone_ttl_sec)
        new_doc = dict(current)
        new_doc.update(extra_patch or {})
        new_doc.update({
            'symbol': symbol,
            'sid': current_sid,
            'guard_status': 'released',
            'released_at_ms': now_ms,
            'release_reason': (release_reason or ''),
            'guard_writer': (writer or ''),
            'guard_writer_ts_ms': now_ms,
            'guard_lease_owner': (writer or ''),
            'guard_lease_token': f"{writer}:{now_ms}:{uuid.uuid4().hex[:8]}",
            'guard_lease_epoch_ms': now_ms,
            'guard_release_pending': False,
            'updated_at_ms': now_ms,
        })
        result = self._cas_set(
            symbol=symbol,
            payload_doc=new_doc,
            expected_version=expected_version,
            expected_sid=current_sid,
            expected_lease_token=expected_lease,
            ttl_sec=ttl,
        )
        if not result.get('applied'):
            self._record_conflict(symbol=symbol, writer=writer, operation='mark_released', reason=(result.get('reason') or 'cas_rejected'))
        # Retry once on concurrent version_mismatch
        if (not result.get('applied')) and retry_once and (result.get('reason') or '').startswith('version_mismatch'):
            return self.mark_released(
                symbol=symbol, expected_sid=expected_sid, release_reason=release_reason,
                writer=writer, tombstone_ttl_sec=ttl, extra_patch=extra_patch, retry_once=False,
            )
        if result.get('applied'):
            try:
                self.r.sadd(self.index_key, symbol)
            except Exception:
                pass
            self._append_event(symbol=symbol, sid=current_sid, writer=writer, operation='mark_released', event_type='guard_released', reason=str(release_reason or result.get('reason') or 'released'), doc=result.get('doc') if isinstance(result.get('doc'), dict) else None)
        return result
