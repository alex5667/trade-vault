from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Redis-backed state helpers for latency deploy-lint results.

The deploy-lint CLI already runs on every sensitive apply/rollout path. This
module turns those point-in-time results into a durable state contract that can
be exported to Prometheus/Grafana and used to detect *persistent* configuration
problems instead of only one-off failures.
"""

from collections.abc import Mapping
from typing import Any


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _clean_codes(values: list[str] | tuple[str, ...] | None) -> list[str]:
    out: list[str] = []
    for raw in values or []:
        s = (raw or '').strip()
        if not s:
            continue
        out.append(s)
    return out


def state_key(prefix: str, purpose: str) -> str:
    return f"{prefix}:{purpose}"


def gate_key(prefix: str, purpose: str) -> str:
    return f"{prefix}:{purpose}:v1"


def summary_key(prefix: str) -> str:
    return f"{prefix}:summary:last"


def update_deploy_lint_state(
    r: Any,
    *,
    purpose: str,
    report: Mapping[str, Any],
    state_prefix: str,
    gate_prefix: str,
    hold_s: int,
    ttl_s: int,
    now_ms: int | None = None,
) -> dict[str, str]:
    """Persist one deploy-lint result and maintain a persistent-drift gate.

    Semantics:
    - transient config drift => ok=0, gate_active=0, exit remains normal lint failure
    - persistent config drift => ok=0, gate_active=1 after hold_s seconds
    - success => clears fail_since/gate immediately
    """

    now_ms = get_ny_time_millis() if now_ms is None else int(now_ms)
    ok = 1 if bool(report.get('ok')) else 0
    errors = _clean_codes(list(report.get('errors') or []))
    warnings = _clean_codes(list(report.get('warnings') or []))
    checks = dict(report.get('checks') or {})

    skey = state_key(state_prefix, purpose)
    gkey = gate_key(gate_prefix, purpose)
    prev = r.hgetall(skey) or {}

    prev_fail_since = _i(prev.get('fail_since_ts_ms'), 0)
    prev_last_ok = _i(prev.get('last_ok_ts_ms'), 0)
    if ok:
        fail_since_ts_ms = 0
        fail_age_s = 0
        last_ok_ts_ms = now_ms
        gate_active = 0
    else:
        fail_since_ts_ms = prev_fail_since if prev_fail_since > 0 else now_ms
        fail_age_s = int(max(0, (now_ms - fail_since_ts_ms) / 1000.0))
        last_ok_ts_ms = prev_last_ok
        gate_active = 1 if fail_age_s >= max(1, int(hold_s)) else 0

    mapping = {
        'schema_version': '1',
        'purpose': purpose,
        'last_checked_ts_ms': str(now_ms),
        'last_ok_ts_ms': str(last_ok_ts_ms),
        'last_fail_ts_ms': str(0 if ok else now_ms),
        'fail_since_ts_ms': str(fail_since_ts_ms),
        'fail_age_s': str(fail_age_s),
        'ok': str(ok),
        'errors_count': str(len(errors)),
        'warnings_count': str(len(warnings)),
        'gate_active': str(gate_active),
        'gate_hold_s': str(int(hold_s)),
        'gate_reason_code': 'persistent_config_drift' if gate_active else ('ok' if ok else 'transient_config_drift'),
        'error_codes': ','.join(errors) if errors else 'ok',
        'warning_codes': ','.join(warnings) if warnings else 'none',
        'compose_file': (checks.get('compose_file', '')),
        'wrapper_file': (checks.get('wrapper_file', '')),
        'unit_file': (checks.get('unit_file', '')),
        'env_file': (checks.get('env_file', '')),
        'missing_runtime_env': ','.join(checks.get('missing_runtime_env') or []) or 'none',
        'missing_env_file_vars': ','.join(checks.get('missing_env_file_vars') or []) or 'none',
    }

    r.hset(skey, mapping=mapping)
    try:
        r.expire(skey, max(1, int(ttl_s)))
    except Exception:
        pass

    if gate_active:
        gate_mapping = {
            'schema_version': '1',
            'purpose': purpose,
            'gate_active': '1',
            'gate_reason_code': 'persistent_config_drift',
            'last_checked_ts_ms': str(now_ms),
            'fail_since_ts_ms': str(fail_since_ts_ms),
            'fail_age_s': str(fail_age_s),
            'errors_count': str(len(errors)),
            'error_codes': mapping['error_codes'],
        }
        r.hset(gkey, mapping=gate_mapping)
        try:
            r.expire(gkey, max(1, int(ttl_s)))
        except Exception:
            pass
    else:
        try:
            r.delete(gkey)
        except Exception:
            pass

    return mapping
