from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger(__name__)

FAMILIES = ('pbo_high', 'report_stale', 'psr_dsr_low')
LIFECYCLE_STATES = ('none', 'active', 'cleared', 'expired')
DEFAULT_PURPOSES = (
    'conf_score_guardrails_apply',
    'conf_score_guardrails_promote',
    'conf_score_guardrails_autopromo_controller',
    'meta_cov_rollout_controller',
)
DEFAULTS = {
    'pbo_high': {
        'enabled': 1.0,
        'suppress_active': 0.0,
        'min_events_24h': 3.0,
        'min_events_7d': 0.0,
        'share_threshold_24h': 0.0,
        'delta_vs_7d': 0.0,
    },
    'report_stale': {
        'enabled': 1.0,
        'suppress_active': 0.0,
        'min_events_24h': 0.0,
        'min_events_7d': 5.0,
        'share_threshold_24h': 0.0,
        'delta_vs_7d': 0.0,
    },
    'psr_dsr_low': {
        'enabled': 1.0,
        'suppress_active': 0.0,
        'min_events_24h': 4.0,
        'min_events_7d': 0.0,
        'share_threshold_24h': 0.60,
        'delta_vs_7d': 0.15,
    },
}


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _read_hash(client: Any, key: str) -> Dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _redis_client() -> Any | None:
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(_env('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)
    except Exception:
        return None


def _purposes() -> tuple[str, ...]:
    raw = _env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PURPOSES', ','.join(DEFAULT_PURPOSES))
    vals = tuple(dict.fromkeys([x.strip() for x in raw.split(',') if x.strip()]))
    return vals or DEFAULT_PURPOSES


def _defaults_key() -> str:
    return _env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_DEFAULTS_KEY', 'cfg:strategy_research_stats:alert_policy:v1:defaults')


def _purpose_key(purpose: str) -> str:
    prefix = _env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_PREFIX', 'cfg:strategy_research_stats:alert_policy:v1').rstrip(':')
    return f'{prefix}:{purpose}'


def _override_key(purpose: str, family: str) -> str:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX',
        'cfg:strategy_research_stats:alert_policy:suppress_override:v1',
    ).rstrip(':')
    return f'{prefix}:{purpose}:{family}'


def _now_ms() -> int:
    return int(time.time() * 1000)


def _resolve_override(client: Any, purpose: str, family: str, now_ms: int) -> Dict[str, Any]:
    raw = _read_hash(client, _override_key(purpose, family))
    if not raw:
        return {'present': 0.0, 'active': 0.0, 'created_ts_ms': 0, 'expire_ts_ms': 0, 'remaining_s': 0.0}
    created_ts_ms = int(_to_float(raw.get('created_ts_ms'), 0.0))
    expire_ts_ms = int(_to_float(raw.get('expire_ts_ms'), 0.0))
    has_ticket = 1.0 if str(raw.get('ticket') or '').strip() else 0.0
    has_operator = 1.0 if str(raw.get('operator') or '').strip() else 0.0
    has_reason = 1.0 if str(raw.get('reason') or '').strip() else 0.0
    if expire_ts_ms <= now_ms:
        try:
            client.delete(_override_key(purpose, family))
        except Exception:
            pass
        return {
            'present': 0.0,
            'active': 0.0,
            'created_ts_ms': created_ts_ms,
            'expire_ts_ms': expire_ts_ms,
            'remaining_s': 0.0,
            'has_ticket': has_ticket,
            'has_operator': has_operator,
            'has_reason': has_reason,
        }
    return {
        'present': 1.0,
        'active': 1.0,
        'created_ts_ms': created_ts_ms,
        'expire_ts_ms': expire_ts_ms,
        'remaining_s': max(0.0, (expire_ts_ms - now_ms) / 1000.0),
        'has_ticket': has_ticket,
        'has_operator': has_operator,
        'has_reason': has_reason,
    }


def _override_key(purpose: str, family: str) -> str:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_PREFIX',
        'cfg:strategy_research_stats:alert_policy:suppress_override:v1',
    ).rstrip(':')
    return f'{prefix}:{purpose}:{family}'


def _override_state_key(purpose: str, family: str) -> str:
    prefix = _env(
        'STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_STATE_PREFIX',
        'cfg:strategy_research_stats:alert_policy:suppress_state:v1',
    ).rstrip(':')
    return f'{prefix}:{purpose}:{family}'


def _ops_stream() -> str:
    return _env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OPS_STREAM', 'ops:strategy_research_stats:alert_policy:v1')


def _reminder_window_s() -> float:
    return max(60.0, _to_float(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_REMINDER_WINDOW_S', '3600'), 3600.0))


def _expired_recent_window_s() -> float:
    return max(60.0, _to_float(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_OVERRIDE_EXPIRED_RECENT_WINDOW_S', '21600'), 21600.0))


def _emit_event(client: Any, kind: str, payload: Dict[str, Any]) -> None:
    if client is None:
        return
    fields = {
        'ts_ms': str(_now_ms()),
        'kind': kind,
        'source': 'strategy_research_stats_alert_policy_exporter_v1',
    }
    for key, value in payload.items():
        if isinstance(value, (dict, list, tuple)):
            fields[key] = json.dumps(value, sort_keys=True)
        else:
            fields[key] = '' if value is None else str(value)
    try:
        client.xadd(_ops_stream(), fields, maxlen=200000, approximate=True)
    except Exception:
        pass


def _now_ms() -> int:
    return int(time.time() * 1000)


def _resolve_override(client: Any, purpose: str, family: str, now_ms: int) -> Dict[str, Any]:
    raw = _read_hash(client, _override_key(purpose, family))
    state_key = _override_state_key(purpose, family)
    state = _read_hash(client, state_key)
    if raw and not state:
        # Backfill lifecycle state for overrides created before P6.8.
        state = {
            'purpose': purpose,
            'family': family,
            'ticket': str(raw.get('ticket') or ''),
            'operator': str(raw.get('operator') or ''),
            'reason': str(raw.get('reason') or ''),
            'created_ts_ms': str(int(_to_float(raw.get('created_ts_ms'), 0.0))),
            'expire_ts_ms': str(int(_to_float(raw.get('expire_ts_ms'), 0.0))),
            'active': '1',
            'lifecycle_state': 'active',
            'cleared_ts_ms': '0',
            'expired_ts_ms': '0',
            'last_reminder_ts_ms': '0',
            'last_reminder_expire_ts_ms': '0',
            'last_reminder_kind': '',
        }
        try:
            client.hset(state_key, mapping=state)
        except Exception:
            pass
    created_ts_ms = int(_to_float((raw or state).get('created_ts_ms') if (raw or state) else 0, 0.0))
    expire_ts_ms = int(_to_float((raw or state).get('expire_ts_ms') if (raw or state) else 0, 0.0))
    reminder_ts_ms = int(_to_float(state.get('last_reminder_ts_ms'), 0.0)) if state else 0
    expired_ts_ms = int(_to_float(state.get('expired_ts_ms'), 0.0)) if state else 0
    lifecycle_state = str(state.get('lifecycle_state') or 'none') if state else 'none'
    if lifecycle_state == 'active' and expire_ts_ms and expire_ts_ms <= now_ms:
        lifecycle_state = 'expired'
        expired_ts_ms = now_ms
        try:
            client.delete(_override_key(purpose, family))
        except Exception:
            pass
        try:
            client.hset(
                state_key,
                mapping={
                    'purpose': purpose,
                    'family': family,
                    'ticket': str((raw or state).get('ticket') or ''),
                    'operator': str((raw or state).get('operator') or ''),
                    'reason': str((raw or state).get('reason') or ''),
                    'created_ts_ms': str(created_ts_ms),
                    'expire_ts_ms': str(expire_ts_ms),
                    'active': '0',
                    'lifecycle_state': 'expired',
                    'expired_ts_ms': str(expired_ts_ms),
                    # P6.9: flag that renew acknowledgement is required before next suppress
                    'renew_ack_required': '1',
                },
            )
        except Exception:
            pass
        _emit_event(
            client,
            'strategy_research_stats_alert_policy_suppress_override_expired',
            {
                'purpose': purpose,
                'family': family,
                'ticket': str((raw or state).get('ticket') or ''),
                'operator': str((raw or state).get('operator') or ''),
                'reason': str((raw or state).get('reason') or ''),
                'expire_ts_ms': expire_ts_ms,
                'expired_ts_ms': expired_ts_ms,
            },
        )
        raw = {}
        state = _read_hash(client, state_key)
    active = 1.0 if raw and expire_ts_ms > now_ms else 0.0
    if active and (expire_ts_ms - now_ms) / 1000.0 <= _reminder_window_s():
        last_reminder_expire_ts_ms = int(_to_float(state.get('last_reminder_expire_ts_ms'), 0.0)) if state else 0
        if last_reminder_expire_ts_ms != expire_ts_ms:
            reminder_ts_ms = now_ms
            try:
                client.hset(
                    state_key,
                    mapping={
                        'purpose': purpose,
                        'family': family,
                        'active': '1',
                        'lifecycle_state': 'active',
                        'last_reminder_ts_ms': str(reminder_ts_ms),
                        'last_reminder_expire_ts_ms': str(expire_ts_ms),
                        'last_reminder_kind': 'expiry_warning',
                        # P6.9: flag that renew acknowledgement is required before next suppress
                        'renew_ack_required': '1',
                    },
                )
            except Exception:
                pass
            _emit_event(
                client,
                'strategy_research_stats_alert_policy_suppress_override_expiry_warning',
                {
                    'purpose': purpose,
                    'family': family,
                    'ticket': str((raw or state).get('ticket') or ''),
                    'operator': str((raw or state).get('operator') or ''),
                    'reason': str((raw or state).get('reason') or ''),
                    'expire_ts_ms': expire_ts_ms,
                    'remaining_s': max(0.0, (expire_ts_ms - now_ms) / 1000.0),
                },
            )
    has_ticket = 1.0 if str((raw or state).get('ticket') or '').strip() else 0.0 if (raw or state) else 0.0
    has_operator = 1.0 if str((raw or state).get('operator') or '').strip() else 0.0 if (raw or state) else 0.0
    has_reason = 1.0 if str((raw or state).get('reason') or '').strip() else 0.0 if (raw or state) else 0.0
    expiring_soon = 1.0 if active and (expire_ts_ms - now_ms) / 1000.0 <= _reminder_window_s() else 0.0
    expired_recently = 1.0 if lifecycle_state == 'expired' and expired_ts_ms and (now_ms - expired_ts_ms) / 1000.0 <= _expired_recent_window_s() else 0.0
    # P6.9: resolve renewal acknowledgement state fields from lifecycle state hash
    renew_ack_required = 1.0 if _to_float((state or {}).get('renew_ack_required'), 0.0) > 0.0 else 0.0
    renew_ack_ts_ms = int(_to_float((state or {}).get('renew_ack_ts_ms'), 0.0)) if state else 0
    renew_ack_present = 1.0 if str((state or {}).get('renew_ack_ticket') or '').strip() else 0.0
    renew_count = _to_float((state or {}).get('renew_count'), 0.0) if state else 0.0
    return {
        'present': 1.0 if raw else 0.0,
        'active': active,
        'created_ts_ms': created_ts_ms,
        'expire_ts_ms': expire_ts_ms,
        'remaining_s': max(0.0, (expire_ts_ms - now_ms) / 1000.0) if active else 0.0,
        'has_ticket': has_ticket,
        'has_operator': has_operator,
        'has_reason': has_reason,
        'state_present': 1.0 if state else 0.0,
        'lifecycle_state': lifecycle_state,
        'last_reminder_ts_ms': reminder_ts_ms,
        'expired_ts_ms': expired_ts_ms,
        'expiring_soon': expiring_soon,
        'expired_recently': expired_recently,
        # P6.9 renewal tracking fields
        'renew_ack_required': renew_ack_required,
        'renew_ack_present': renew_ack_present,
        'renew_ack_ts_ms': renew_ack_ts_ms,
        'renew_count': renew_count,
    }

def _family_default(family: str, field: str) -> float:
    env_map = {
        ('pbo_high', 'min_events_24h'): 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_DEFAULT_PBO_HIGH_MIN_EVENTS_24H',
        ('report_stale', 'min_events_7d'): 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_DEFAULT_REPORT_STALE_MIN_EVENTS_7D',
        ('psr_dsr_low', 'min_events_24h'): 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_DEFAULT_PSR_DSR_LOW_MIN_EVENTS_24H',
        ('psr_dsr_low', 'share_threshold_24h'): 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_DEFAULT_PSR_DSR_LOW_SHARE_24H',
        ('psr_dsr_low', 'delta_vs_7d'): 'STRATEGY_RESEARCH_STATS_ALERT_POLICY_DEFAULT_PSR_DSR_LOW_DELTA_VS_7D',
    }
    env_name = env_map.get((family, field))
    base = DEFAULTS[family][field]
    if env_name:
        return _to_float(_env(env_name, str(base)), base)
    return base


def resolve_family_policy(family: str, defaults_hash: Dict[str, str], purpose_hash: Dict[str, str]) -> Dict[str, float]:
    base = {
        'enabled': _family_default(family, 'enabled'),
        'suppress_active': _family_default(family, 'suppress_active'),
        'min_events_24h': _family_default(family, 'min_events_24h'),
        'min_events_7d': _family_default(family, 'min_events_7d'),
        'share_threshold_24h': _family_default(family, 'share_threshold_24h'),
        'delta_vs_7d': _family_default(family, 'delta_vs_7d'),
    }
    for src in (defaults_hash, purpose_hash):
        if not src:
            continue
        for field in tuple(base.keys()):
            key = f'{field}_{family}'
            if key in src and src[key] not in ('', None):
                base[field] = _to_float(src[key], base[field])
    return base


UP = Gauge('strategy_research_stats_alert_policy_exporter_up', '1 if alert policy exporter loop is running')
REDIS_READ_OK = Gauge('strategy_research_stats_alert_policy_redis_read_ok', '1 if alert policy exporter can read Redis')
POLICY_ENABLED = Gauge('strategy_research_stats_alert_policy_enabled', '1 if family alerting is enabled for purpose', ['purpose', 'family'])
POLICY_SUPPRESS = Gauge('strategy_research_stats_alert_policy_suppress_active', '1 if family alerting is suppressed for purpose after TTL-aware overrides are applied', ['purpose', 'family'])
POLICY_STATIC_SUPPRESS = Gauge('strategy_research_stats_alert_policy_static_suppress_active', '1 if family alerting is statically suppressed by policy hash for purpose', ['purpose', 'family'])
POLICY_OVERRIDE_ACTIVE = Gauge('strategy_research_stats_alert_policy_override_active', '1 if a TTL-backed suppress override is active for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_PRESENT = Gauge('strategy_research_stats_alert_policy_override_present', '1 if an override hash is present and still active for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_CREATED_UNIX = Gauge('strategy_research_stats_alert_policy_override_created_unixtime', 'Creation time of the active suppress override for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_EXPIRE_UNIX = Gauge('strategy_research_stats_alert_policy_override_expire_unixtime', 'Expiry time of the active suppress override for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_REMAINING = Gauge('strategy_research_stats_alert_policy_override_remaining_seconds', 'Seconds until suppress override expiry for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_HAS_TICKET = Gauge('strategy_research_stats_alert_policy_override_ticket_present', '1 if active suppress override contains a ticket for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_HAS_OPERATOR = Gauge('strategy_research_stats_alert_policy_override_operator_present', '1 if active suppress override contains an operator for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_HAS_REASON = Gauge('strategy_research_stats_alert_policy_override_reason_present', '1 if active suppress override contains a reason for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_STATE_PRESENT = Gauge('strategy_research_stats_alert_policy_override_state_present', '1 if persistent lifecycle state exists for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_LIFECYCLE = Gauge('strategy_research_stats_alert_policy_override_lifecycle_state', 'Lifecycle state of suppress override for purpose/family', ['purpose', 'family', 'state'])
POLICY_OVERRIDE_EXPIRING_SOON = Gauge('strategy_research_stats_alert_policy_override_expiring_soon', '1 if active suppress override is within reminder window for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_EXPIRED_RECENTLY = Gauge('strategy_research_stats_alert_policy_override_expired_recently', '1 if suppress override expired recently for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_LAST_REMINDER_UNIX = Gauge('strategy_research_stats_alert_policy_override_last_reminder_unixtime', 'Unix time of the most recent expiry reminder for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_LAST_EXPIRED_UNIX = Gauge('strategy_research_stats_alert_policy_override_last_expired_unixtime', 'Unix time of the most recent observed override expiry for purpose/family', ['purpose', 'family'])
# P6.9: renewal workflow gauges — allow Grafana/alerts to surface pending renewal state
POLICY_OVERRIDE_RENEW_ACK_REQUIRED = Gauge('strategy_research_stats_alert_policy_override_renew_ack_required', '1 if reminder/expiry requires explicit acknowledgement before renew for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_RENEW_ACK_PRESENT = Gauge('strategy_research_stats_alert_policy_override_renew_ack_present', '1 if a renewal acknowledgement is currently stored for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_RENEW_ACK_AGE = Gauge('strategy_research_stats_alert_policy_override_renew_ack_age_seconds', 'Age of the current renewal acknowledgement for purpose/family', ['purpose', 'family'])
POLICY_OVERRIDE_RENEW_COUNT = Gauge('strategy_research_stats_alert_policy_override_renew_count', 'How many times a suppress override has been renewed for purpose/family', ['purpose', 'family'])
POLICY_ACTIVE_SUPPRESSIONS_TOTAL = Gauge('strategy_research_stats_alert_policy_active_suppressions_total', 'Number of active TTL-backed suppress overrides by family', ['family'])
POLICY_MIN_24H = Gauge('strategy_research_stats_alert_policy_min_events_24h', 'Minimum 24h events threshold for purpose/family alerts', ['purpose', 'family'])
POLICY_MIN_7D = Gauge('strategy_research_stats_alert_policy_min_events_7d', 'Minimum 7d events threshold for purpose/family alerts', ['purpose', 'family'])
POLICY_SHARE_24H = Gauge('strategy_research_stats_alert_policy_share_threshold_24h', '24h share threshold for purpose/family alerts', ['purpose', 'family'])
POLICY_DELTA_7D = Gauge('strategy_research_stats_alert_policy_delta_vs_7d', 'Required 24h-vs-7d delta for purpose/family alerts', ['purpose', 'family'])
POLICY_HASH_PRESENT = Gauge('strategy_research_stats_alert_policy_hash_present', '1 if explicit purpose policy hash exists', ['purpose'])
POLICY_DEFAULTS_PRESENT = Gauge('strategy_research_stats_alert_policy_defaults_present', '1 if defaults hash exists')


def publish(client: Any | None = None) -> None:
    if client is None:
        client = _redis_client()
    if client is None:
        REDIS_READ_OK.set(0.0)
        return
    REDIS_READ_OK.set(1.0)
    defaults_hash = _read_hash(client, _defaults_key())
    POLICY_DEFAULTS_PRESENT.set(1.0 if defaults_hash else 0.0)
    active_totals = {family: 0.0 for family in FAMILIES}
    now_ms = _now_ms()
    for purpose in _purposes():
        purpose_hash = _read_hash(client, _purpose_key(purpose))
        POLICY_HASH_PRESENT.labels(purpose=purpose).set(1.0 if purpose_hash else 0.0)
        for family in FAMILIES:
            policy = resolve_family_policy(family, defaults_hash, purpose_hash)
            override = _resolve_override(client, purpose, family, now_ms)
            static_suppress = 1.0 if policy['suppress_active'] else 0.0
            effective_suppress = 1.0 if static_suppress or override['active'] else 0.0
            POLICY_ENABLED.labels(purpose=purpose, family=family).set(policy['enabled'])
            POLICY_STATIC_SUPPRESS.labels(purpose=purpose, family=family).set(static_suppress)
            POLICY_SUPPRESS.labels(purpose=purpose, family=family).set(effective_suppress)
            POLICY_OVERRIDE_ACTIVE.labels(purpose=purpose, family=family).set(override['active'])
            POLICY_OVERRIDE_PRESENT.labels(purpose=purpose, family=family).set(override['present'])
            POLICY_OVERRIDE_CREATED_UNIX.labels(purpose=purpose, family=family).set(override['created_ts_ms'] / 1000.0 if override['created_ts_ms'] else 0.0)
            POLICY_OVERRIDE_EXPIRE_UNIX.labels(purpose=purpose, family=family).set(override['expire_ts_ms'] / 1000.0 if override['expire_ts_ms'] else 0.0)
            POLICY_OVERRIDE_REMAINING.labels(purpose=purpose, family=family).set(override['remaining_s'])
            POLICY_OVERRIDE_HAS_TICKET.labels(purpose=purpose, family=family).set(override.get('has_ticket', 0.0) if override['active'] else 0.0)
            POLICY_OVERRIDE_HAS_OPERATOR.labels(purpose=purpose, family=family).set(override.get('has_operator', 0.0) if override['active'] else 0.0)
            POLICY_OVERRIDE_HAS_REASON.labels(purpose=purpose, family=family).set(override.get('has_reason', 0.0) if override['active'] else 0.0)
            POLICY_OVERRIDE_STATE_PRESENT.labels(purpose=purpose, family=family).set(override.get('state_present', 0.0))
            for state in LIFECYCLE_STATES:
                POLICY_OVERRIDE_LIFECYCLE.labels(purpose=purpose, family=family, state=state).set(1.0 if override.get('lifecycle_state', 'none') == state else 0.0)
            POLICY_OVERRIDE_EXPIRING_SOON.labels(purpose=purpose, family=family).set(override.get('expiring_soon', 0.0))
            POLICY_OVERRIDE_EXPIRED_RECENTLY.labels(purpose=purpose, family=family).set(override.get('expired_recently', 0.0))
            POLICY_OVERRIDE_LAST_REMINDER_UNIX.labels(purpose=purpose, family=family).set((override.get('last_reminder_ts_ms', 0) or 0) / 1000.0)
            POLICY_OVERRIDE_LAST_EXPIRED_UNIX.labels(purpose=purpose, family=family).set((override.get('expired_ts_ms', 0) or 0) / 1000.0)
            # P6.9: renewal acknowledgement gauges
            POLICY_OVERRIDE_RENEW_ACK_REQUIRED.labels(purpose=purpose, family=family).set(override.get('renew_ack_required', 0.0))
            POLICY_OVERRIDE_RENEW_ACK_PRESENT.labels(purpose=purpose, family=family).set(override.get('renew_ack_present', 0.0))
            ack_ts_ms = override.get('renew_ack_ts_ms', 0) or 0
            POLICY_OVERRIDE_RENEW_ACK_AGE.labels(purpose=purpose, family=family).set(max(0.0, (now_ms - ack_ts_ms) / 1000.0) if ack_ts_ms else 0.0)
            POLICY_OVERRIDE_RENEW_COUNT.labels(purpose=purpose, family=family).set(override.get('renew_count', 0.0))
            if override['active']:
                active_totals[family] += 1.0
            POLICY_MIN_24H.labels(purpose=purpose, family=family).set(policy['min_events_24h'])
            POLICY_MIN_7D.labels(purpose=purpose, family=family).set(policy['min_events_7d'])
            POLICY_SHARE_24H.labels(purpose=purpose, family=family).set(policy['share_threshold_24h'])
            POLICY_DELTA_7D.labels(purpose=purpose, family=family).set(policy['delta_vs_7d'])
    for family, value in active_totals.items():
        POLICY_ACTIVE_SUPPRESSIONS_TOTAL.labels(family=family).set(value)


def main() -> None:
    port = int(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_EXPORTER_PORT', '9838') or 9838)
    interval_s = float(_env('STRATEGY_RESEARCH_STATS_ALERT_POLICY_EXPORTER_INTERVAL_S', '30') or 30)
    start_http_server(port)
    logger.info('strategy research stats alert policy exporter listening on %s', port)
    while True:
        UP.set(1.0)
        publish()
        time.sleep(interval_s)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
