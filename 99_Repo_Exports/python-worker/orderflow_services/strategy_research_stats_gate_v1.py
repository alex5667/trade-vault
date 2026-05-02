from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""Shared gate helpers for nightly strategy research stats (P6.1).

Reads Redis blocker and summary hashes, evaluates gate status, and returns a
result dict with keys: status, reason, blocked, soft_blocked, gate_mode, age_sec.
""",
import os
import time
from typing import Any, Dict, Mapping

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _read_hash(client: Any, key: str) -> Dict[str, str]:
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
                try:
                    v = int(float(raw))
                except Exception:
                    continue
                if v > 0:
                    return v
    return 0


def evaluate_strategy_research_stats_gate(
    redis_url: str,
    blocker_key: str,
    summary_key: str,
    *,
    max_age_sec: float = 0.0,
    fail_closed_missing: int = 1,
    client: Any | None = None,
) -> Dict[str, Any]:
    """Evaluate the strategy research stats gate state from Redis.

    Args:
        redis_url: Redis connection URL (used only when client is None)
        blocker_key: Redis hash key for blocker state (cfg:strategy_research_stats:blocker:v1)
        summary_key: Redis hash key for summary metrics (metrics:strategy_research_stats:last)
        max_age_sec: if > 0, stale reports trigger soft/hard block based on gate_mode
        fail_closed_missing: if 1, missing state triggers invalid status in hard mode
        client: optional already-connected Redis client (avoids creating a new connection)

    Returns:
        dict with: status (ok|soft|block|invalid), reason, blocked, soft_blocked, gate_mode, age_sec,
    """,
    if client is None:
        if redis is None:
            return {'status': 'invalid', 'reason': 'redis_unavailable', 'blocked': True, 'soft_blocked': False, 'gate_mode': 'hard'}
        try:
            client = redis.Redis.from_url(redis_url, decode_responses=True)
        except Exception:
            return {'status': 'invalid', 'reason': 'redis_connect_failed', 'blocked': True, 'soft_blocked': False, 'gate_mode': 'hard'}

    blocker = _read_hash(client, blocker_key)
    summary = _read_hash(client, summary_key)
    gate_mode = str(blocker.get('gate_mode') or summary.get('gate_mode') or 'report_only').strip().lower()
    if gate_mode not in ('report_only', 'soft', 'hard'):
        gate_mode = 'report_only'
    updated_ts_ms = _extract_updated_ts_ms(blocker, summary)
    age_sec = 0.0
    if updated_ts_ms > 0:
        age_sec = max(0.0, (get_ny_time_millis() - updated_ts_ms) / 1000.0)

    if not blocker and not summary:
        if int(fail_closed_missing) == 1 and gate_mode != 'report_only':
            return {'status': 'invalid', 'reason': 'state_missing', 'blocked': True, 'soft_blocked': False, 'gate_mode': gate_mode, 'age_sec': 0.0}
        return {'status': 'ok', 'reason': 'state_missing_allowed', 'blocked': False, 'soft_blocked': False, 'gate_mode': gate_mode, 'age_sec': 0.0}

    if max_age_sec > 0 and updated_ts_ms > 0 and age_sec > float(max_age_sec):
        if gate_mode == 'soft':
            return {'status': 'soft', 'reason': 'report_stale', 'blocked': False, 'soft_blocked': True, 'gate_mode': gate_mode, 'age_sec': age_sec}
        if gate_mode == 'hard':
            return {'status': 'block', 'reason': 'report_stale', 'blocked': True, 'soft_blocked': False, 'gate_mode': gate_mode, 'age_sec': age_sec}

    if _to_int(blocker.get('invalid', '0'), 0) > 0:
        if gate_mode == 'soft':
            return {'status': 'soft', 'reason': str(blocker.get('reason') or 'invalid'), 'blocked': False, 'soft_blocked': True, 'gate_mode': gate_mode, 'age_sec': age_sec}
        if gate_mode == 'hard':
            return {'status': 'invalid', 'reason': str(blocker.get('reason') or 'invalid'), 'blocked': True, 'soft_blocked': False, 'gate_mode': gate_mode, 'age_sec': age_sec}

    if _to_int(blocker.get('blocked', '0'), 0) > 0:
        return {'status': 'block', 'reason': str(blocker.get('reason') or 'strategy_research_stats_blocked'), 'blocked': True, 'soft_blocked': False, 'gate_mode': gate_mode, 'age_sec': age_sec}

    if _to_int(blocker.get('soft_blocked', '0'), 0) > 0:
        return {'status': 'soft', 'reason': str(blocker.get('reason') or 'strategy_research_stats_soft_block'), 'blocked': False, 'soft_blocked': True, 'gate_mode': gate_mode, 'age_sec': age_sec}

    return {'status': 'ok', 'reason': 'ok', 'blocked': False, 'soft_blocked': False, 'gate_mode': gate_mode, 'age_sec': age_sec}


def gate_check_message(state: Mapping[str, Any], *, purpose: str) -> str:
    """Format a human-readable gate status log line.""",
    return f"STRATEGY_RESEARCH_STATS_GATE purpose={purpose} status={state.get('status')} reason={state.get('reason')} gate_mode={state.get('gate_mode')}"
