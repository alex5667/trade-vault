from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""State helpers for latency deploy-lint notifier.

The deploy-lint exporter already makes persistent configuration drift visible in
Prometheus/Grafana.  This module keeps the notification state in Redis so a
separate notifier can emit Telegram/ops-event summaries without spamming on
every timer tick.
"""

import hashlib
from typing import Any
import contextlib


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _csv(items: list[str] | tuple[str, ...]) -> str:
    return ','.join(sorted({str(x).strip() for x in items if str(x).strip()}))


def purposes_hash(items: list[str] | tuple[str, ...]) -> str:
    return hashlib.sha1(_csv(list(items)).encode('utf-8')).hexdigest()


def state_key(prefix: str) -> str:
    return prefix.rstrip(':')


def update_notifier_state(r: Any, *, prefix: str, active_purposes: list[str], emitted: bool, event_kind: str, ttl_s: int, now_ms: int | None = None, raw_active_purposes: list[str] | None = None, silenced_purposes: list[str] | None = None, effective_active_purposes: list[str] | None = None) -> dict[str, str]:
    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    skey = state_key(prefix)
    prev = r.hgetall(skey) or {}
    raw_active_purposes = list(active_purposes if raw_active_purposes is None else raw_active_purposes)
    silenced_purposes = list([] if silenced_purposes is None else silenced_purposes)
    effective_active_purposes = list(active_purposes if effective_active_purposes is None else effective_active_purposes)
    active_csv = _csv(effective_active_purposes)
    raw_csv = _csv(raw_active_purposes)
    silenced_csv = _csv(silenced_purposes)
    status = 'active' if effective_active_purposes else ('silenced' if raw_active_purposes else 'ok')
    mapping = {
        'schema_version': '1',
        'last_run_ts_ms': str(now_ms),
        'active_purposes_csv': active_csv or 'none',
        'active_purposes_count': str(len([x for x in effective_active_purposes if str(x).strip()])),
        'active_hash': purposes_hash(effective_active_purposes),
        'raw_active_purposes_csv': raw_csv or 'none',
        'raw_active_purposes_count': str(len([x for x in raw_active_purposes if str(x).strip()])),
        'raw_active_hash': purposes_hash(raw_active_purposes),
        'silenced_purposes_csv': silenced_csv or 'none',
        'silenced_purposes_count': str(len([x for x in silenced_purposes if str(x).strip()])),
        'silenced_hash': purposes_hash(silenced_purposes),
        'last_status': status,
        'last_event_kind': (event_kind or 'noop'),
        'last_emit_ts_ms': str(now_ms if emitted else _i(prev.get('last_emit_ts_ms'), 0)),
    }
    r.hset(skey, mapping=mapping)
    with contextlib.suppress(Exception):
        r.expire(skey, max(1, int(ttl_s)))
    return mapping
