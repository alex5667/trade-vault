from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Shared research-guard blocker helpers for rollout-sensitive jobs (P5.2).

Reads compact Redis hashes produced by the strategy research guard job/exporter:
  - STRATEGY_RESEARCH_GUARD_BLOCKER_KEY (default: cfg:research_guard:blocker:v1)
  - STRATEGY_RESEARCH_GUARD_SUMMARY_KEY (default: metrics:strategy_research_guard:last)

Design goals:
  - deterministic fail-closed orchestration checks for promotion/apply paths
  - low coupling: callers can use a simple bool API or a richer evaluation dict
  - safe handling of missing/stale state without assuming exporter availability
"""

import os
from collections.abc import Mapping
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _read_hash(client: Any, key: str) -> dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _extract_updated_ts_ms(blocker: Mapping[str, str], summary: Mapping[str, str]) -> int:
    for src in (blocker, summary):
        for key in ('updated_ts_ms', 'ts_ms', 'last_updated_ts_ms'):
            raw = src.get(key)
            if raw not in (None, ''):
                v = _to_int(raw, 0)
                if v > 0:
                    return v
    return 0


def evaluate_research_guard_gate(
    redis_url: str,
    blocker_key: str,
    summary_key: str,
    *,
    max_age_sec: float = 0.0,
    fail_closed_missing: int = 1,
    client: Any | None = None,
) -> dict[str, Any]:
    """Evaluate research guard gate for rollout-sensitive jobs.

    Returns a compact dict:
      status: ok | block | invalid
      reason: machine-friendly reason code
      blocker: latest blocker hash (may be empty)
      summary: latest summary hash (may be empty)
    """
    if client is None:
        if redis is None:
            return {
                'status': 'invalid',
                'reason': 'redis_unavailable',
                'blocked': True,
                'blocker': {},
                'summary': {},
                'updated_ts_ms': 0,
                'age_sec': 0.0,
                'report_only': 0,
            }
        try:
            client = redis.Redis.from_url(redis_url, decode_responses=True)
        except Exception:
            return {
                'status': 'invalid',
                'reason': 'redis_connect_failed',
                'blocked': True,
                'blocker': {},
                'summary': {},
                'updated_ts_ms': 0,
                'age_sec': 0.0,
                'report_only': 0,
            }

    blocker = _read_hash(client, blocker_key)
    summary = _read_hash(client, summary_key)
    updated_ts_ms = _extract_updated_ts_ms(blocker, summary)
    now_ms = get_ny_time_millis()
    age_sec = max(0.0, (now_ms - updated_ts_ms) / 1000.0) if updated_ts_ms > 0 else 0.0

    report_only = 1 if _to_int(blocker.get('report_only', summary.get('report_only', '0')), 0) > 0 else 0
    blocked = 1 if _to_int(blocker.get('blocked', blocker.get('active', blocker.get('blocker_active', summary.get('blocker_active', '0')))), 0) > 0 else 0
    reason = str(blocker.get('reason') or blocker.get('reason_code') or summary.get('blocker_reason') or '').strip()

    if not blocker and not summary:
        status = 'invalid' if int(fail_closed_missing) == 1 else 'ok'
        return {
            'status': status,
            'reason': 'state_missing' if status == 'invalid' else 'ok',
            'blocked': status != 'ok',
            'blocker': blocker,
            'summary': summary,
            'updated_ts_ms': 0,
            'age_sec': 0.0,
            'report_only': report_only,
        }

    # Report-only mode is intentionally fail-open: operators can still collect reports
    # without hard-blocking rollout-sensitive jobs.
    if report_only == 1:
        return {
            'status': 'ok',
            'reason': 'report_only',
            'blocked': False,
            'blocker': blocker,
            'summary': summary,
            'updated_ts_ms': updated_ts_ms,
            'age_sec': age_sec,
            'report_only': report_only,
        }

    if max_age_sec > 0:
        if updated_ts_ms <= 0:
            return {
                'status': 'invalid',
                'reason': 'ts_missing',
                'blocked': True,
                'blocker': blocker,
                'summary': summary,
                'updated_ts_ms': updated_ts_ms,
                'age_sec': age_sec,
                'report_only': report_only,
            }
        if age_sec > float(max_age_sec):
            return {
                'status': 'block',
                'reason': 'report_stale',
                'blocked': True,
                'blocker': blocker,
                'summary': summary,
                'updated_ts_ms': updated_ts_ms,
                'age_sec': age_sec,
                'report_only': report_only,
            }

    if blocked == 1:
        return {
            'status': 'block',
            'reason': reason or 'research_guard_blocked',
            'blocked': True,
            'blocker': blocker,
            'summary': summary,
            'updated_ts_ms': updated_ts_ms,
            'age_sec': age_sec,
            'report_only': report_only,
        }

    return {
        'status': 'ok',
        'reason': 'ok',
        'blocked': False,
        'blocker': blocker,
        'summary': summary,
        'updated_ts_ms': updated_ts_ms,
        'age_sec': age_sec,
        'report_only': report_only,
    }


def check_research_guard_blocker(
    redis_url: str,
    blocker_key: str,
    summary_key: str | None = None,
    *,
    max_age_sec: float = 0.0,
    fail_closed_missing: int = 1,
    client: Any | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    """Compatibility bool API used by apply/promote jobs.

    Returns (blocked, reason, state).
    """
    state = evaluate_research_guard_gate(
        redis_url,
        blocker_key,
        summary_key or _env('STRATEGY_RESEARCH_GUARD_SUMMARY_KEY', 'metrics:strategy_research_guard:last'),
        max_age_sec=max_age_sec,
        fail_closed_missing=fail_closed_missing,
        client=client,
    )
    return bool(state.get('blocked')), (state.get('reason') or 'unknown'), state


def assert_research_guard_open(
    redis_url: str,
    *,
    purpose: str,
    stage_mode: bool = False,
    exit_code_blocked: int = 24,
    exit_code_invalid: int = 25,
) -> None:
    """Raise SystemExit when a rollout-sensitive job must not proceed."""
    if stage_mode and _env('STRATEGY_RESEARCH_GUARD_PREFLIGHT_ALLOW_STAGE', '1') == '1':
        return

    state = evaluate_research_guard_gate(
        redis_url,
        _env('STRATEGY_RESEARCH_GUARD_BLOCKER_KEY', 'cfg:research_guard:blocker:v1'),
        _env('STRATEGY_RESEARCH_GUARD_SUMMARY_KEY', 'metrics:strategy_research_guard:last'),
        max_age_sec=_to_float(_env('STRATEGY_RESEARCH_GUARD_MAX_AGE_SEC', '129600'), 129600.0),
        fail_closed_missing=_to_int(_env('STRATEGY_RESEARCH_GUARD_FAIL_CLOSED_MISSING', '1'), 1),
    )
    status = (state.get('status') or 'invalid')
    reason = (state.get('reason') or 'unknown')
    if status == 'ok':
        return
    if status == 'block':
        print(f'STRATEGY_RESEARCH_GUARD_BLOCK purpose={purpose} reason={reason}')
        raise SystemExit(int(exit_code_blocked))
    print(f'STRATEGY_RESEARCH_GUARD_INVALID purpose={purpose} reason={reason}')
    raise SystemExit(int(exit_code_invalid))
