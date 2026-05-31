#!/usr/bin/env python3
from __future__ import annotations

"""Prometheus exporter for latency deploy-lint state with P4.9 policy visibility."""

import os
import time
from dataclasses import dataclass
from typing import Any

from prometheus_client import Gauge, start_http_server

from services.observability.latency_deploy_contract import CONTRACTS
from services.observability.latency_deploy_lint_state import state_key
from services.observability.latency_deploy_lint_notify_state import state_key as notifier_state_key
from services.observability.latency_deploy_lint_silence_state import parse_silence_state, state_key as silence_state_key


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _f(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


G_UP = Gauge('latency_contract_deploy_lint_exporter_up', 'latency deploy lint exporter loop running')
G_READ_OK = Gauge('latency_contract_deploy_lint_exporter_read_ok', 'latency deploy lint exporter redis read ok')
G_STATE_PRESENT = Gauge('latency_contract_deploy_lint_state_present', 'deploy lint state present', ['purpose'])
G_OK = Gauge('latency_contract_deploy_lint_ok', 'latest deploy lint result ok', ['purpose'])
G_ERRORS = Gauge('latency_contract_deploy_lint_errors_total', 'latest deploy lint errors count', ['purpose'])
G_WARNINGS = Gauge('latency_contract_deploy_lint_warnings_total', 'latest deploy lint warnings count', ['purpose'])
G_LAST_CHECK_AGE = Gauge('latency_contract_deploy_lint_last_checked_age_seconds', 'age of last deploy lint check', ['purpose'])
G_FAIL_AGE = Gauge('latency_contract_deploy_lint_fail_age_seconds', 'age of current deploy lint failure streak', ['purpose'])
G_GATE_ACTIVE = Gauge('latency_contract_deploy_lint_gate_active', 'persistent deploy lint gate active', ['purpose'])
G_SILENCE_STATE_PRESENT = Gauge('latency_contract_deploy_lint_silence_state_present', 'deploy lint silence state present', ['purpose'])
G_SILENCE_ACTIVE = Gauge('latency_contract_deploy_lint_silence_active', 'deploy lint notifier silence active', ['purpose'])
G_SILENCE_REMAINING = Gauge('latency_contract_deploy_lint_silence_remaining_seconds', 'remaining notifier silence time', ['purpose'])
G_SILENCE_TTL_EXPIRED = Gauge('latency_contract_deploy_lint_silence_ttl_expired', 'last silence window for this purpose expired and escalation should remain active until fixed/re-acked', ['purpose'])
G_SILENCE_TTL_EXPIRED_AGE = Gauge('latency_contract_deploy_lint_silence_ttl_expired_age_seconds', 'age since notifier observed silence TTL expiry', ['purpose'])
# P4.9 policy metrics (per-purpose)
G_POLICY_WINDOW_ACK_COUNT = Gauge('latency_contract_deploy_lint_silence_policy_window_ack_count', 'ack count used in current silence policy window', ['purpose'])
G_POLICY_WINDOW_BUDGET_MINUTES = Gauge('latency_contract_deploy_lint_silence_policy_window_budget_minutes_used', 'budget minutes used in current silence policy window', ['purpose'])
G_POLICY_LIMIT_HIT_TOTAL = Gauge('latency_contract_deploy_lint_silence_policy_limit_hit_total', 'times ack policy limits were hit for this purpose', ['purpose'])
G_POLICY_DENIED_TOTAL = Gauge('latency_contract_deploy_lint_silence_policy_denied_total', 'times silence ack was denied by policy for this purpose', ['purpose'])
G_POLICY_OVERRIDE_ACTIVE = Gauge('latency_contract_deploy_lint_silence_policy_override_active', 'current notifier silence is using escalation-ticket override', ['purpose'])
G_SUMMARY_FAIL_TOTAL = Gauge('latency_contract_deploy_lint_summary_fail_total', 'number of purposes currently failing deploy lint')
G_SUMMARY_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_gate_active_total', 'number of purposes with persistent deploy lint gate active')
G_SUMMARY_SILENCED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_silenced_gate_active_total', 'number of purposes with persistent deploy lint gate active but silenced in notifier')
G_SUMMARY_UNSILENCED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_unsilenced_gate_active_total', 'number of purposes with persistent deploy lint gate active and not silenced in notifier')
G_SUMMARY_EXPIRED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_expired_gate_active_total', 'number of purposes with persistent deploy lint gate active after silence TTL expiry')
# P4.9 policy summary metrics (global)
G_SUMMARY_POLICY_BLOCKED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_policy_blocked_gate_active_total', 'number of active gate purposes where latest ack attempt was blocked by silence policy')
G_SUMMARY_POLICY_OVERRIDE_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_policy_override_gate_active_total', 'number of active gate purposes currently silenced via escalation-ticket override')
G_NOTIFIER_STATE_PRESENT = Gauge('latency_contract_deploy_lint_notifier_state_present', 'deploy lint notifier state present')
G_NOTIFIER_LAST_RUN_AGE = Gauge('latency_contract_deploy_lint_notifier_last_run_age_seconds', 'age of deploy lint notifier last run')
G_NOTIFIER_ACTIVE = Gauge('latency_contract_deploy_lint_notifier_active', 'deploy lint notifier sees active persistent drift')
G_NOTIFIER_SILENCED = Gauge('latency_contract_deploy_lint_notifier_silenced', 'deploy lint notifier currently suppressed by silence workflow')
G_NOTIFIER_SILENCED_PURPOSES_TOTAL = Gauge('latency_contract_deploy_lint_notifier_silenced_purposes_total', 'count of currently silenced purposes in notifier state')


@dataclass
class Cfg:
    redis_url: str
    port: int
    interval_s: float
    state_prefix: str
    silence_prefix: str
    summary_key: str


def load_cfg() -> Cfg:
    prefix = _env('LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX', 'metrics:latency_contract:deploy_lint:last')
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        port=_i(_env('LATENCY_CONTRACT_DEPLOY_LINT_EXPORTER_PORT', '9834'), 9834),
        interval_s=float(_env('LATENCY_CONTRACT_DEPLOY_LINT_EXPORTER_INTERVAL_S', '15') or 15),
        state_prefix=prefix,
        silence_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence'),
        summary_key=_env('LATENCY_CONTRACT_DEPLOY_LINT_SUMMARY_KEY', 'metrics:latency_contract:deploy_lint:summary:last'),
    )


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    start_http_server(cfg.port)
    purposes = tuple(sorted(CONTRACTS.keys()))
    while True:
        G_UP.set(1.0)
        now = time.time()
        now_ms = int(now * 1000)
        try:
            fail_total = gate_total = silenced_gate_total = unsilenced_gate_total = expired_gate_total = 0
            # P4.9: policy summary counters
            policy_blocked_gate_total = 0
            policy_override_gate_total = 0
            for purpose in purposes:
                raw = r.hgetall(state_key(cfg.state_prefix, purpose)) or {}
                present = 1.0 if raw else 0.0
                ok = _f(raw.get('ok'), 0.0)
                gate = _f(raw.get('gate_active'), 0.0)
                errors = _f(raw.get('errors_count'), 0.0)
                warnings = _f(raw.get('warnings_count'), 0.0)
                last_ts_ms = _i(raw.get('last_checked_ts_ms'), 0)
                fail_age_s = _f(raw.get('fail_age_s'), 0.0)
                age_s = max(0.0, now - (last_ts_ms / 1000.0)) if last_ts_ms > 0 else 0.0
                G_STATE_PRESENT.labels(purpose=purpose).set(present)
                G_OK.labels(purpose=purpose).set(ok)
                G_GATE_ACTIVE.labels(purpose=purpose).set(gate)
                G_ERRORS.labels(purpose=purpose).set(errors)
                G_WARNINGS.labels(purpose=purpose).set(warnings)
                G_LAST_CHECK_AGE.labels(purpose=purpose).set(age_s)
                G_FAIL_AGE.labels(purpose=purpose).set(fail_age_s)
                sraw = r.hgetall(silence_state_key(cfg.silence_prefix, purpose)) or {}
                sst = parse_silence_state(sraw, now_ms=now_ms)
                G_SILENCE_STATE_PRESENT.labels(purpose=purpose).set(1.0 if sraw else 0.0)
                G_SILENCE_ACTIVE.labels(purpose=purpose).set(1.0 if sst.silence_active else 0.0)
                G_SILENCE_REMAINING.labels(purpose=purpose).set(float(sst.remaining_s))
                G_SILENCE_TTL_EXPIRED.labels(purpose=purpose).set(1.0 if sst.ttl_expired else 0.0)
                expired_age_s = max(0.0, now - (sst.ttl_expiry_last_notify_ts_ms / 1000.0)) if sst.ttl_expiry_last_notify_ts_ms > 0 else 0.0
                G_SILENCE_TTL_EXPIRED_AGE.labels(purpose=purpose).set(expired_age_s)
                # P4.9 per-purpose policy metrics
                G_POLICY_WINDOW_ACK_COUNT.labels(purpose=purpose).set(float(sst.policy_window_ack_count))
                G_POLICY_WINDOW_BUDGET_MINUTES.labels(purpose=purpose).set(float(sst.policy_window_budget_minutes_used))
                G_POLICY_LIMIT_HIT_TOTAL.labels(purpose=purpose).set(float(sst.policy_limit_hit_total))
                G_POLICY_DENIED_TOTAL.labels(purpose=purpose).set(float(sst.policy_denied_total))
                G_POLICY_OVERRIDE_ACTIVE.labels(purpose=purpose).set(1.0 if sst.policy_current_override_active else 0.0)
                if present > 0 and ok <= 0:
                    fail_total += 1
                if gate > 0:
                    gate_total += 1
                    if sst.silence_active:
                        silenced_gate_total += 1
                    else:
                        unsilenced_gate_total += 1
                    if sst.ttl_expired:
                        expired_gate_total += 1
                    # P4.9: track policy-blocked and policy-override counts per active gate
                    if sst.last_action == 'ack_denied_policy':
                        policy_blocked_gate_total += 1
                    if sst.policy_current_override_active:
                        policy_override_gate_total += 1
            G_SUMMARY_FAIL_TOTAL.set(float(fail_total))
            G_SUMMARY_GATE_ACTIVE_TOTAL.set(float(gate_total))
            G_SUMMARY_SILENCED_GATE_ACTIVE_TOTAL.set(float(silenced_gate_total))
            G_SUMMARY_UNSILENCED_GATE_ACTIVE_TOTAL.set(float(unsilenced_gate_total))
            G_SUMMARY_EXPIRED_GATE_ACTIVE_TOTAL.set(float(expired_gate_total))
            # P4.9 summary policy metrics
            G_SUMMARY_POLICY_BLOCKED_GATE_ACTIVE_TOTAL.set(float(policy_blocked_gate_total))
            G_SUMMARY_POLICY_OVERRIDE_GATE_ACTIVE_TOTAL.set(float(policy_override_gate_total))
            r.hset(cfg.summary_key, mapping={'schema_version': '3', 'last_ts_ms': str(now_ms), 'fail_total': str(fail_total), 'gate_active_total': str(gate_total), 'silenced_gate_active_total': str(silenced_gate_total), 'unsilenced_gate_active_total': str(unsilenced_gate_total), 'expired_gate_active_total': str(expired_gate_total), 'policy_blocked_gate_active_total': str(policy_blocked_gate_total), 'policy_override_gate_active_total': str(policy_override_gate_total)})
            nraw = r.hgetall(notifier_state_key(_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFIER_STATE_KEY', 'metrics:latency_contract:deploy_lint:notifier:last'))) or {}
            nlast_ms = _i(nraw.get('last_run_ts_ms'), 0)
            nstatus = str(nraw.get('last_status', 'ok'))
            G_NOTIFIER_STATE_PRESENT.set(1.0 if nraw else 0.0)
            G_NOTIFIER_LAST_RUN_AGE.set(max(0.0, now - (nlast_ms / 1000.0)) if nlast_ms > 0 else 0.0)
            G_NOTIFIER_ACTIVE.set(1.0 if nstatus == 'active' else 0.0)
            G_NOTIFIER_SILENCED.set(1.0 if nstatus == 'silenced' else 0.0)
            G_NOTIFIER_SILENCED_PURPOSES_TOTAL.set(_f(nraw.get('silenced_purposes_count'), 0.0))
            G_READ_OK.set(1.0)
        except Exception:
            G_READ_OK.set(0.0)
        time.sleep(cfg.interval_s)


if __name__ == '__main__':
    raise SystemExit(main())
