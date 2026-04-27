#!/usr/bin/env python3
from __future__ import annotations

"""Prometheus exporter for latency deploy-lint state with P4.14 warning-policy / notifier-route approval binding visibility."""

import os
import time
from dataclasses import dataclass
from typing import Any

from prometheus_client import Gauge, start_http_server

from services.observability.latency_deploy_contract import CONTRACTS
from services.observability.latency_deploy_lint_state import state_key
from services.observability.latency_deploy_lint_notify_state import state_key as notifier_state_key
from services.observability.latency_deploy_lint_silence_approval_state import binding_mismatch_fields, build_drift_binding, parse_approval_state, read_latest_approval
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
# P4.10 dual-control per-purpose metrics
G_DUAL_CONTROL_REQUIRED = Gauge('latency_contract_deploy_lint_silence_dual_control_required', 'current silence requires dual-control exception approval metadata', ['purpose'])
G_DUAL_CONTROL_DENIED_TOTAL = Gauge('latency_contract_deploy_lint_silence_dual_control_denied_total', 'times silence ack was denied because dual-control approval was missing or invalid', ['purpose'])
G_DUAL_CONTROL_OVERRIDE_ACTIVE = Gauge('latency_contract_deploy_lint_silence_dual_control_override_active', 'current notifier silence is active under an approved dual-control exception', ['purpose'])
G_APPROVAL_PENDING = Gauge('latency_contract_deploy_lint_silence_approval_pending', 'latest override approval request is prepared and awaiting second approver', ['purpose'])
G_APPROVAL_READY = Gauge('latency_contract_deploy_lint_silence_approval_ready', 'latest override approval request is approved and ready for requester ack', ['purpose'])
G_APPROVAL_AGE = Gauge('latency_contract_deploy_lint_silence_approval_age_seconds', 'age of latest override approval request', ['purpose'])
# P4.11 freshness metrics
G_APPROVAL_EXPIRED = Gauge('latency_contract_deploy_lint_silence_approval_expired', 'latest override approval request auto-expired before approval', ['purpose'])
G_APPROVAL_CANCELLED = Gauge('latency_contract_deploy_lint_silence_approval_cancelled', 'latest override approval request auto-cancelled after approval freshness elapsed', ['purpose'])
G_APPROVAL_FRESHNESS_REMAINING = Gauge('latency_contract_deploy_lint_silence_approval_freshness_remaining_seconds', 'remaining freshness time for latest override approval request', ['purpose'])
# P4.12 binding metrics
G_APPROVAL_INVALIDATED = Gauge('latency_contract_deploy_lint_silence_approval_invalidated', 'latest override approval request was invalidated because deploy-lint drift changed before final ack', ['purpose'])
G_APPROVAL_BINDING_MATCH = Gauge('latency_contract_deploy_lint_silence_approval_binding_match', 'latest override approval still matches current deploy-lint drift binding', ['purpose'])
# P4.13 semantic binding metrics
G_APPROVAL_DETAILS_FINGERPRINT_MATCH = Gauge('latency_contract_deploy_lint_silence_approval_details_fingerprint_match', 'latest override approval still matches current deploy-lint semantic details_json fingerprint', ['purpose'])
G_APPROVAL_BINDING_SCHEMA_VERSION = Gauge('latency_contract_deploy_lint_silence_approval_binding_schema_version', 'binding schema version used by latest override approval request', ['purpose'])
# P4.14 warning-policy / route-class per-purpose metrics
G_APPROVAL_WARNING_POLICY_MATCH = Gauge('latency_contract_deploy_lint_silence_approval_warning_policy_match', 'latest override approval request warning severity policy still matches current state', ['purpose'])
G_APPROVAL_NOTIFIER_ROUTE_CLASS_MATCH = Gauge('latency_contract_deploy_lint_silence_approval_notifier_route_class_match', 'latest override approval request notifier route class still matches current state', ['purpose'])
G_SUMMARY_FAIL_TOTAL = Gauge('latency_contract_deploy_lint_summary_fail_total', 'number of purposes currently failing deploy lint')
G_SUMMARY_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_gate_active_total', 'number of purposes with persistent deploy lint gate active')
G_SUMMARY_SILENCED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_silenced_gate_active_total', 'number of purposes with persistent deploy lint gate active but silenced in notifier')
G_SUMMARY_UNSILENCED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_unsilenced_gate_active_total', 'number of purposes with persistent deploy lint gate active and not silenced in notifier')
G_SUMMARY_EXPIRED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_expired_gate_active_total', 'number of purposes with persistent deploy lint gate active after silence TTL expiry')
# P4.9 policy summary metrics (global)
G_SUMMARY_POLICY_BLOCKED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_policy_blocked_gate_active_total', 'number of active gate purposes where latest ack attempt was blocked by silence policy')
G_SUMMARY_POLICY_OVERRIDE_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_policy_override_gate_active_total', 'number of active gate purposes currently silenced via escalation-ticket override')
# P4.10 dual-control summary metrics (global)
G_SUMMARY_DUAL_CONTROL_PENDING_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_pending_total', 'number of purposes with pending dual-control override approval requests')
G_SUMMARY_DUAL_CONTROL_READY_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_ready_total', 'number of purposes with approved dual-control override requests waiting to be consumed')
# P4.11 freshness summary metrics
G_SUMMARY_DUAL_CONTROL_EXPIRED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_expired_gate_active_total', 'number of active gate purposes whose latest prepared override request auto-expired')
G_SUMMARY_DUAL_CONTROL_CANCELLED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_cancelled_gate_active_total', 'number of active gate purposes whose latest approved override request auto-cancelled before ack consumption')
# P4.12 binding summary metrics
G_SUMMARY_DUAL_CONTROL_INVALIDATED_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_invalidated_gate_active_total', 'number of active gate purposes whose latest override approval was invalidated because deploy-lint drift changed before final ack')
G_SUMMARY_DUAL_CONTROL_BINDING_MISMATCH_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_binding_mismatch_total', 'number of latest override approvals whose bound deploy-lint drift no longer matches the current drift snapshot')
# P4.13 semantic binding summary metric
G_SUMMARY_DUAL_CONTROL_SEMANTIC_BINDING_MISMATCH_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total', 'number of latest override approvals whose semantic drift binding no longer matches current gate_reason_code/errors_count/details fingerprint')
# P4.14 warning-policy / route-class summary metric
G_SUMMARY_DUAL_CONTROL_ROUTE_BINDING_MISMATCH_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_route_binding_mismatch_total', 'number of active gate purposes whose warning policy or notifier route class changed')
G_SUMMARY_DUAL_CONTROL_OVERRIDE_GATE_ACTIVE_TOTAL = Gauge('latency_contract_deploy_lint_summary_dual_control_override_gate_active_total', 'number of active gate purposes currently silenced under an approved dual-control exception')
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
    approval_prefix: str  # P4.10: separate prefix for dual-control approval request objects
    approval_ttl_s: int
    approval_prepared_freshness_s: int
    approval_approved_freshness_s: int
    summary_key: str
    # P4.14 warning-code CSV lists for route-aware binding
    notify_warn_codes_warn_csv: str = ''
    notify_warn_codes_crit_csv: str = ''
    notify_warn_codes_page_csv: str = ''


def load_cfg() -> Cfg:
    prefix = _env('LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX', 'metrics:latency_contract:deploy_lint:last')
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        port=_i(_env('LATENCY_CONTRACT_DEPLOY_LINT_EXPORTER_PORT', '9834'), 9834),
        interval_s=float(_env('LATENCY_CONTRACT_DEPLOY_LINT_EXPORTER_INTERVAL_S', '15') or 15),
        state_prefix=prefix,
        silence_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence'),
        # P4.10: approval prefix for dual-control request lookup
        approval_prefix=_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_PREFIX', 'cfg:orderflow:latency_contract:deploy_lint:silence_approval'),
        approval_ttl_s=max(3600, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_TTL_S', '604800'), 604800)),
        approval_prepared_freshness_s=max(60, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_PREPARED_FRESHNESS_S', '7200'), 7200)),
        approval_approved_freshness_s=max(60, _i(_env('LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_APPROVED_FRESHNESS_S', '1800'), 1800)),
        summary_key=_env('LATENCY_CONTRACT_DEPLOY_LINT_SUMMARY_KEY', 'metrics:latency_contract:deploy_lint:summary:last'),
        # P4.14: warning code policy CSVs for route-aware binding
        notify_warn_codes_warn_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_WARN_CSV', ''),
        notify_warn_codes_crit_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_CRIT_CSV', ''),
        notify_warn_codes_page_csv=_env('LATENCY_CONTRACT_DEPLOY_LINT_NOTIFY_WARN_CODES_PAGE_CSV', ''),
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
            # P4.9 policy summary counters
            policy_blocked_gate_total = 0
            policy_override_gate_total = 0
            # P4.10 dual-control summary counters
            dual_control_pending_total = 0
            dual_control_ready_total = 0
            dual_control_override_gate_total = 0
            # P4.11 freshness summary counters
            dual_control_expired_gate_total = 0
            dual_control_cancelled_gate_total = 0
            # P4.12 binding summary counters
            dual_control_invalidated_gate_total = 0
            dual_control_binding_mismatch_total = 0
            # P4.13 semantic binding summary counter
            dual_control_semantic_binding_mismatch_total = 0
            # P4.14 route-class binding summary counter
            dual_control_route_binding_mismatch_total = 0
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
                # P4.10: dual-control per-purpose metrics
                G_DUAL_CONTROL_REQUIRED.labels(purpose=purpose).set(1.0 if sst.dual_control_required else 0.0)
                G_DUAL_CONTROL_DENIED_TOTAL.labels(purpose=purpose).set(float(sst.dual_control_denied_total))
                dual_override_active = bool(sst.policy_current_override_active and sst.dual_control_required and sst.silence_active)
                G_DUAL_CONTROL_OVERRIDE_ACTIVE.labels(purpose=purpose).set(1.0 if dual_override_active else 0.0)
                # P4.11: read latest approval request with auto freshness transition
                araw = read_latest_approval(
                    r,
                    prefix=cfg.approval_prefix,
                    purpose=purpose,
                    prepared_freshness_s=cfg.approval_prepared_freshness_s,
                    approved_freshness_s=cfg.approval_approved_freshness_s,
                    ttl_s=cfg.approval_ttl_s,
                    ops_stream=_env('LATENCY_CONTRACT_DEPLOY_LINT_OPS_EVENT_STREAM', 'ops:latency_contract:events:v1'),
                    now_ms=now_ms,
                )
                ast = parse_approval_state(araw)
                # P4.12+P4.13+P4.14: compute semantic+route binding mismatch
                approval_ref_ts_ms = ast.approved_ts_ms or ast.prepared_ts_ms or ast.invalidated_ts_ms
                approval_age_s = max(0.0, now - (approval_ref_ts_ms / 1000.0)) if approval_ref_ts_ms > 0 else 0.0
                freshness_remaining_s = max(0.0, (ast.freshness_deadline_ts_ms - now_ms) / 1000.0) if ast.freshness_deadline_ts_ms > now_ms else 0.0
                current_binding = build_drift_binding(
                    r, state_prefix=cfg.state_prefix, purpose=purpose, now_ms=now_ms,
                    warn_codes_warn_csv=cfg.notify_warn_codes_warn_csv,
                    warn_codes_crit_csv=cfg.notify_warn_codes_crit_csv,
                    warn_codes_page_csv=cfg.notify_warn_codes_page_csv,
                )
                mismatch_fields = binding_mismatch_fields(ast, current_binding) if ast.present else []
                binding_match = bool(ast.present and not mismatch_fields)
                # P4.13: check if semantic details fingerprint still matches
                details_fingerprint_match = bool(
                    ast.present
                    and ast.binding_schema_version >= 2
                    and ast.bound_details_fingerprint
                    and ast.bound_details_fingerprint == current_binding.get('bound_details_fingerprint', '')
                )
                G_APPROVAL_PENDING.labels(purpose=purpose).set(1.0 if ast.status == 'prepared' else 0.0)
                G_APPROVAL_READY.labels(purpose=purpose).set(1.0 if ast.status == 'approved' else 0.0)
                G_APPROVAL_EXPIRED.labels(purpose=purpose).set(1.0 if ast.status == 'expired' else 0.0)
                G_APPROVAL_CANCELLED.labels(purpose=purpose).set(1.0 if ast.status == 'cancelled' else 0.0)
                G_APPROVAL_INVALIDATED.labels(purpose=purpose).set(1.0 if ast.status == 'invalidated' else 0.0)
                G_APPROVAL_BINDING_MATCH.labels(purpose=purpose).set(1.0 if binding_match else 0.0)
                G_APPROVAL_DETAILS_FINGERPRINT_MATCH.labels(purpose=purpose).set(1.0 if details_fingerprint_match else 0.0)
                G_APPROVAL_BINDING_SCHEMA_VERSION.labels(purpose=purpose).set(float(ast.binding_schema_version))
                # P4.14: warning-policy and notifier route class match metrics
                G_APPROVAL_WARNING_POLICY_MATCH.labels(purpose=purpose).set(
                    0.0 if 'warning_severity_policy' in mismatch_fields else (1.0 if ast.present else 0.0)
                )
                G_APPROVAL_NOTIFIER_ROUTE_CLASS_MATCH.labels(purpose=purpose).set(
                    0.0 if 'notifier_route_class' in mismatch_fields else (1.0 if ast.present else 0.0)
                )
                G_APPROVAL_AGE.labels(purpose=purpose).set(approval_age_s)
                G_APPROVAL_FRESHNESS_REMAINING.labels(purpose=purpose).set(freshness_remaining_s)
                if ast.status == 'prepared':
                    dual_control_pending_total += 1
                if ast.status == 'approved':
                    dual_control_ready_total += 1
                if ast.present and ast.status != 'consumed' and not binding_match:
                    dual_control_binding_mismatch_total += 1
                    # P4.13: count mismatches on semantic fields specifically
                    if ast.binding_schema_version >= 2 and any(x in {'gate_reason_code', 'errors_count', 'details_fingerprint'} for x in mismatch_fields):
                        dual_control_semantic_binding_mismatch_total += 1
                    # P4.14: count mismatches on warning-policy / route-class fields
                    if any(x in mismatch_fields for x in ('warning_codes', 'warning_severity_policy', 'notifier_route_class')):
                        dual_control_route_binding_mismatch_total += 1
                if gate > 0 and ast.status == 'expired':
                    dual_control_expired_gate_total += 1
                if gate > 0 and ast.status == 'cancelled':
                    dual_control_cancelled_gate_total += 1
                if gate > 0 and ast.status == 'invalidated':
                    dual_control_invalidated_gate_total += 1
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
                    # P4.10: track dual-control override active gate count
                    if dual_override_active:
                        dual_control_override_gate_total += 1
            G_SUMMARY_FAIL_TOTAL.set(float(fail_total))
            G_SUMMARY_GATE_ACTIVE_TOTAL.set(float(gate_total))
            G_SUMMARY_SILENCED_GATE_ACTIVE_TOTAL.set(float(silenced_gate_total))
            G_SUMMARY_UNSILENCED_GATE_ACTIVE_TOTAL.set(float(unsilenced_gate_total))
            G_SUMMARY_EXPIRED_GATE_ACTIVE_TOTAL.set(float(expired_gate_total))
            # P4.9 summary policy metrics
            G_SUMMARY_POLICY_BLOCKED_GATE_ACTIVE_TOTAL.set(float(policy_blocked_gate_total))
            G_SUMMARY_POLICY_OVERRIDE_GATE_ACTIVE_TOTAL.set(float(policy_override_gate_total))
            # P4.10 summary dual-control metrics
            G_SUMMARY_DUAL_CONTROL_PENDING_TOTAL.set(float(dual_control_pending_total))
            G_SUMMARY_DUAL_CONTROL_READY_TOTAL.set(float(dual_control_ready_total))
            # P4.11 freshness summary metrics
            G_SUMMARY_DUAL_CONTROL_EXPIRED_GATE_ACTIVE_TOTAL.set(float(dual_control_expired_gate_total))
            G_SUMMARY_DUAL_CONTROL_CANCELLED_GATE_ACTIVE_TOTAL.set(float(dual_control_cancelled_gate_total))
            # P4.12 binding summary metrics
            G_SUMMARY_DUAL_CONTROL_INVALIDATED_GATE_ACTIVE_TOTAL.set(float(dual_control_invalidated_gate_total))
            G_SUMMARY_DUAL_CONTROL_BINDING_MISMATCH_TOTAL.set(float(dual_control_binding_mismatch_total))
            # P4.13 semantic binding summary metric
            G_SUMMARY_DUAL_CONTROL_SEMANTIC_BINDING_MISMATCH_TOTAL.set(float(dual_control_semantic_binding_mismatch_total))
            # P4.14 route-class binding summary metric
            G_SUMMARY_DUAL_CONTROL_ROUTE_BINDING_MISMATCH_TOTAL.set(float(dual_control_route_binding_mismatch_total))
            G_SUMMARY_DUAL_CONTROL_OVERRIDE_GATE_ACTIVE_TOTAL.set(float(dual_control_override_gate_total))
            r.hset(cfg.summary_key, mapping={
                'schema_version': '6',
                'last_ts_ms': str(now_ms),
                'fail_total': str(fail_total),
                'gate_active_total': str(gate_total),
                'silenced_gate_active_total': str(silenced_gate_total),
                'unsilenced_gate_active_total': str(unsilenced_gate_total),
                'expired_gate_active_total': str(expired_gate_total),
                'policy_blocked_gate_active_total': str(policy_blocked_gate_total),
                'policy_override_gate_active_total': str(policy_override_gate_total),
                # P4.10 dual-control summary fields
                'dual_control_pending_total': str(dual_control_pending_total),
                'dual_control_ready_total': str(dual_control_ready_total),
                'dual_control_override_gate_active_total': str(dual_control_override_gate_total),
                # P4.11 freshness summary fields
                'dual_control_expired_gate_active_total': str(dual_control_expired_gate_total),
                'dual_control_cancelled_gate_active_total': str(dual_control_cancelled_gate_total),
                # P4.12 binding summary fields
                'dual_control_invalidated_gate_active_total': str(dual_control_invalidated_gate_total),
                'dual_control_binding_mismatch_total': str(dual_control_binding_mismatch_total),
                # P4.13 semantic binding summary field
                'dual_control_semantic_binding_mismatch_total': str(dual_control_semantic_binding_mismatch_total),
                # P4.14 route-class binding summary field
                'dual_control_route_binding_mismatch_total': str(dual_control_route_binding_mismatch_total),
            })
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
