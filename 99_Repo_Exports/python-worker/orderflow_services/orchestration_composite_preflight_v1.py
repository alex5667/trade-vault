from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""Composite orchestration preflight gate (P6.3).

Combines three independent safety sources behind one deterministic decision:
  - deploy-lint persistent drift gate
  - latency-contract rollout gate
  - strategy research stats gate

Design goals:
  - one preflight command for both host-wrapper and in-container paths
  - per-source audit state written to Redis hash + ops stream
  - soft status from strategy_research_stats does not block orchestration decision
  - legacy research_guard is preserved in history/exporter normalization

Exit codes:
  0: allow
  24: blocked (deploy-lint, latency-contract, or strategy_research_stats hard-block)
  25: invalid/misconfigured (fail-safer-than-open)
  64: bad arguments,
""",
import argparse
import json
import os
import time
from typing import Any, Dict

from services.observability.latency_deploy_lint_state import gate_key as deploy_lint_gate_key
from services.observability.latency_deploy_lint_state import state_key as deploy_lint_state_key
from orderflow_services.strategy_research_stats_gate_v1 import evaluate_strategy_research_stats_gate

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


ALLOWED_PURPOSES = {
    'latency_contract_sensitive_apply',
    'conf_score_guardrails_apply',
    'conf_score_guardrails_promote',
    'meta_cov_rollout_controller',
    'conf_score_guardrails_autopromo_controller',
    'ofc_contextual_rollout_controller',
}

# Lower priority_rank = higher importance (wins when multiple sources block).
SOURCE_PRIORITY = {
    'deploy_lint': 0,
    'latency_contract': 1,
    'strategy_research_stats': 2,
    'research_guard': 3,
}


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _read_hash(client: Any, key: str) -> Dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _state_prefix_key(prefix: str, purpose: str) -> str:
    return f'{prefix.rstrip(":")}:{purpose}'


def _stage_allowed_for_strategy_research_stats(purpose: str, stage_mode: bool) -> bool:
    """Return True when stage-mode bypass is configured for strategy_research_stats.""",
    if not stage_mode:
        return False
    if purpose != 'conf_score_guardrails_apply':
        return False
    return _env('STRATEGY_RESEARCH_STATS_PREFLIGHT_ALLOW_STAGE', '1') == '1'


def evaluate_latency_contract_gate(
    client: Any,
    *,
    purpose: str,
) -> Dict[str, Any]:
    """Read the latency-contract rollout gate state from Redis.""",
    state_key_val = _env(
        'LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY',
        'metrics:latency_contract:rollout_gate:last',
    )
    gate_key_val = _env(
        'LATENCY_CONTRACT_ROLLOUT_GATE_KEY',
        'cfg:orderflow:latency_contract:rollout_gate:v1',
    )
    gate = _read_hash(client, gate_key_val)
    if not gate:
        gate = _read_hash(client, state_key_val)
    if not gate:
        return {
            'source': 'latency_contract',
            'status': 'invalid',
            'reason': 'state_missing',
            'blocked': True,
            'raw': {},
        }
    active = _i(gate.get('gate_active'), 0)
    if active > 0:
        reason = gate.get('gate_reason_codes') or gate.get('gate_reason_code') or 'external_missing'
        return {
            'source': 'latency_contract',
            'status': 'block',
            'reason': str(reason),
            'blocked': True,
            'raw': gate,
        }
    return {
        'source': 'latency_contract',
        'status': 'ok',
        'reason': 'ok',
        'blocked': False,
        'raw': gate,
    }


def evaluate_deploy_lint_gate(
    client: Any,
    *,
    purpose: str,
) -> Dict[str, Any]:
    """Read the deploy-lint persistent gate state from Redis."""
    # deploy_lint_gate_key / deploy_lint_state_key take (prefix, purpose) positionally.
    gate_prefix = _env(
        'LATENCY_CONTRACT_DEPLOY_LINT_GATE_PREFIX',
        'cfg:orderflow:latency_contract:deploy_lint_gate',
    )
    state_prefix_dl = _env(
        'LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX',
        'metrics:latency_contract:deploy_lint:last',
    )
    try:
        gkey = deploy_lint_gate_key(gate_prefix, purpose)
    except Exception:
        gkey = ''
    try:
        skey = deploy_lint_state_key(state_prefix_dl, purpose)
    except Exception:
        skey = ''

    gate = _read_hash(client, gkey) if gkey else {}
    if not gate and skey:
        gate = _read_hash(client, skey)

    if not gate:
        # Allow missing deploy-lint state (it may not be configured for every purpose).
        return {
            'source': 'deploy_lint',
            'status': 'ok',
            'reason': 'state_missing',
            'blocked': False,
            'raw': {},
        }

    active = _i(gate.get('gate_active'), 0)
    ok_flag = _i(gate.get('ok'), -1)
    if active > 0:
        reason = gate.get('gate_reason_code') or gate.get('gate_reason_codes') or 'persistent_config_drift'
        return {
            'source': 'deploy_lint',
            'status': 'block',
            'reason': str(reason),
            'blocked': True,
            'raw': gate,
        }
    if ok_flag == 0:
        reason = gate.get('gate_reason_code') or gate.get('reason') or 'persistent_config_drift'
        return {
            'source': 'deploy_lint',
            'status': 'block',
            'reason': str(reason),
            'blocked': True,
            'raw': gate,
        }
    return {
        'source': 'deploy_lint',
        'status': 'ok',
        'reason': 'ok',
        'blocked': False,
        'raw': gate,
    }


def _priority_tuple(src: Dict[str, Any]) -> tuple:
    """Sort key: (bad_status_rank, source_priority). Bad = block > invalid > soft.""",
    status_rank = {'block': 0, 'invalid': 1, 'soft': 2, 'ok': 99}.get(
        str(src.get('status') or 'ok'), 99
    )
    return (status_rank, SOURCE_PRIORITY.get(str(src.get('source') or ''), 99))


def _select_reason(sources: list[Dict[str, Any]]) -> Dict[str, Any]:
    """Select the highest-priority non-ok source. Soft status is NOT blocking.""",
    bad = [src for src in sources if str(src.get('status')) in ('block', 'invalid')]
    if not bad:
        return {'source': 'none', 'status': 'ok', 'reason': 'ok', 'priority_rank': 999}
    chosen = sorted(bad, key=_priority_tuple)[0]
    rank = SOURCE_PRIORITY.get(str(chosen.get('source') or ''), 99)
    return {
        'source': chosen.get('source', 'none'),
        'status': chosen.get('status', 'invalid'),
        'reason': chosen.get('reason', 'unknown'),
        'priority_rank': rank,
    }


def _emit_audit_stream(
    client: Any,
    composite: Dict[str, Any],
    *,
    stream: str,
) -> None:
    """Append one compact event to the ops stream for history/rollup consumers.""",
    fields = {
        'ts_ms': str(get_ny_time_millis()),
        'purpose': str(composite.get('purpose') or ''),
        'status': str(composite.get('decision_status') or 'invalid'),
        'selected_source': str(composite.get('selected_source') or 'none'),
        'selected_reason_code': str(composite.get('selected_reason_code') or 'none:ok'),
        'selected_priority_rank': str(composite.get('selected_priority_rank') or 999),
        'deploy_lint_status': str(composite.get('deploy_lint_status') or 'unknown'),
        'deploy_lint_reason': str(composite.get('deploy_lint_reason') or 'unknown'),
        'latency_contract_status': str(composite.get('latency_contract_status') or 'unknown'),
        'latency_contract_reason': str(composite.get('latency_contract_reason') or 'unknown'),
        'strategy_research_stats_status': str(composite.get('strategy_research_stats_status') or 'unknown'),
        'strategy_research_stats_reason': str(composite.get('strategy_research_stats_reason') or 'unknown'),
        'sources_json': json.dumps(dict(composite.get('sources') or {}), sort_keys=True),
    }
    client.xadd(stream, fields, maxlen=200000, approximate=True)


def _emit_audit_state(
    client: Any,
    composite: Dict[str, Any],
    *,
    state_prefix: str,
    state_ttl_s: int,
) -> None:
    """Persist composite state to a Redis hash per-purpose for the exporter.""",
    purpose = str(composite.get('purpose') or '')
    skey = _state_prefix_key(state_prefix, purpose)
    mapping = {
        'updated_ts_ms': str(get_ny_time_millis()),
        'purpose': purpose,
        'status': str(composite.get('decision_status') or 'invalid'),
        'selected_source': str(composite.get('selected_source') or 'none'),
        'selected_reason_code': str(composite.get('selected_reason_code') or 'none:ok'),
        'selected_priority_rank': str(composite.get('selected_priority_rank') or 999),
        'deploy_lint_status': str(composite.get('deploy_lint_status') or 'unknown'),
        'deploy_lint_reason': str(composite.get('deploy_lint_reason') or 'unknown'),
        'latency_contract_status': str(composite.get('latency_contract_status') or 'unknown'),
        'latency_contract_reason': str(composite.get('latency_contract_reason') or 'unknown'),
        'strategy_research_stats_status': str(composite.get('strategy_research_stats_status') or 'unknown'),
        'strategy_research_stats_reason': str(composite.get('strategy_research_stats_reason') or 'unknown'),
        'sources_json': json.dumps(dict(composite.get('sources') or {}), sort_keys=True),
    }
    client.hset(skey, mapping=mapping)
    if state_ttl_s > 0:
        client.expire(skey, state_ttl_s)


def evaluate_composite_gate(
    redis_url: str,
    *,
    purpose: str,
    stage_mode: bool = False,
    emit_audit: bool = True,
    client: Any = None,
) -> Dict[str, Any]:
    """Evaluate all three preflight safety sources and return the composite decision.

    Returns a dict with:
      status: ok | block | invalid
      selected_source: the source that dominated the decision
      selected_reason_code: normalized reason code
      decision_status: same as status
      sources: per-source status/reason dict,
    """,
    state_prefix = _env('ORCHESTRATION_PREFLIGHT_STATE_PREFIX', 'metrics:orchestration:preflight:last')
    state_ttl_s = _i(_env('ORCHESTRATION_PREFLIGHT_STATE_TTL_S', '172800'), 172800)
    stream = _env('ORCHESTRATION_PREFLIGHT_OPS_EVENT_STREAM', 'ops:orchestration:preflight:v1')
    redis_url_resolved = redis_url or _env('REDIS_URL', 'redis://redis-worker-1:6379/0')

    if redis is None:
        # Redis library not available; fail-closed.
        sources = {
            'deploy_lint': {'status': 'invalid', 'reason': 'redis_unavailable'},
            'latency_contract': {'status': 'invalid', 'reason': 'redis_unavailable'},
            'strategy_research_stats': {'status': 'invalid', 'reason': 'redis_unavailable'},
        }
        selected = _select_reason([
            {'source': 'deploy_lint', **sources['deploy_lint']},
            {'source': 'latency_contract', **sources['latency_contract']},
            {'source': 'strategy_research_stats', **sources['strategy_research_stats']},
        ])
        return {
            'purpose': purpose,
            'status': 'invalid',
            'decision_status': 'invalid',
            'selected_source': 'deploy_lint',
            'selected_reason_code': 'deploy_lint:redis_unavailable',
            'selected_priority_rank': 0,
            'deploy_lint_status': 'invalid',
            'deploy_lint_reason': 'redis_unavailable',
            'latency_contract_status': 'invalid',
            'latency_contract_reason': 'redis_unavailable',
            'strategy_research_stats_status': 'invalid',
            'strategy_research_stats_reason': 'redis_unavailable',
        }

    if client is None:
        try:
            client = redis.Redis.from_url(redis_url_resolved, decode_responses=True)
        except Exception:
            return {
                'purpose': purpose,
                'status': 'invalid',
                'decision_status': 'invalid',
                'selected_source': 'deploy_lint',
                'selected_reason_code': 'deploy_lint:redis_connect_failed',
                'selected_priority_rank': 0,
                'deploy_lint_status': 'invalid',
                'deploy_lint_reason': 'redis_connect_failed',
                'latency_contract_status': 'invalid',
                'latency_contract_reason': 'redis_connect_failed',
                'strategy_research_stats_status': 'invalid',
                'strategy_research_stats_reason': 'redis_connect_failed',
            }

    # ── deploy-lint gate ─────────────────────────────────────────────────────
    if _env('ENABLE_LATENCY_CONTRACT_DEPLOY_LINT_PREFLIGHT', '1') != '1':
        deploy = {'source': 'deploy_lint', 'status': 'ok', 'reason': 'disabled', 'blocked': False, 'raw': {}}
    else:
        deploy = evaluate_deploy_lint_gate(client, purpose=purpose)

    # ── latency-contract rollout gate ─────────────────────────────────────────
    latency = evaluate_latency_contract_gate(
        client=client,
        purpose=purpose,
    )

    # ── strategy research stats gate ──────────────────────────────────────────
    if _env('ENABLE_STRATEGY_RESEARCH_STATS_COMPOSITE_PREFLIGHT', '1') != '1':
        # disabled — pass-through
        research_stats = {
            'source': 'strategy_research_stats',
            'status': 'ok',
            'reason': 'disabled',
            'blocked': False,
            'soft_blocked': False,
            'gate_mode': 'report_only',
            'raw': {},
        }
    elif _stage_allowed_for_strategy_research_stats(purpose, stage_mode):
        # Stage-mode bypass: conf_score_guardrails_apply in staging skips the stats gate.
        research_stats = {
            'source': 'strategy_research_stats',
            'status': 'ok',
            'reason': 'stage_allowed',
            'blocked': False,
            'soft_blocked': False,
            'gate_mode': 'soft',
            'raw': {},
        }
    else:
        research_stats = dict(
            source='strategy_research_stats',
            **evaluate_strategy_research_stats_gate(
                redis_url_resolved,
                _env('STRATEGY_RESEARCH_STATS_BLOCKER_KEY', 'cfg:strategy_research_stats:blocker:v1'),
                _env('STRATEGY_RESEARCH_STATS_SUMMARY_KEY', 'metrics:strategy_research_stats:last'),
                max_age_sec=float(_env('STRATEGY_RESEARCH_STATS_MAX_AGE_SEC', '129600') or 129600),
                fail_closed_missing=_i(_env('STRATEGY_RESEARCH_STATS_FAIL_CLOSED_MISSING', '0'), 0),
                client=client,
            ),
        )

    # ── composite decision ────────────────────────────────────────────────────
    # soft from research_stats does NOT block; only block/invalid do.
    selected = _select_reason([deploy, latency, research_stats])
    sources = {
        'deploy_lint': {'status': str(deploy.get('status') or 'invalid'), 'reason': str(deploy.get('reason') or 'unknown')},
        'latency_contract': {'status': str(latency.get('status') or 'invalid'), 'reason': str(latency.get('reason') or 'unknown')},
        'strategy_research_stats': {'status': str(research_stats.get('status') or 'invalid'), 'reason': str(research_stats.get('reason') or 'unknown')},
    }
    composite = {
        'purpose': purpose,
        'status': selected.get('status', 'invalid'),
        'decision_status': selected.get('status', 'invalid'),
        'selected_source': selected.get('source', 'none'),
        'selected_reason_code': f"{selected.get('source', 'none')}:{selected.get('reason', 'unknown')}" if selected.get('source') != 'none' else 'none:ok',
        'selected_priority_rank': selected.get('priority_rank', 999),
        'deploy_lint_status': sources['deploy_lint']['status'],
        'deploy_lint_reason': sources['deploy_lint']['reason'],
        'latency_contract_status': sources['latency_contract']['status'],
        'latency_contract_reason': sources['latency_contract']['reason'],
        'strategy_research_stats_status': sources['strategy_research_stats']['status'],
        'strategy_research_stats_reason': sources['strategy_research_stats']['reason'],
        'sources': sources,
    }

    if emit_audit:
        try:
            _emit_audit_state(client, composite, state_prefix=state_prefix, state_ttl_s=state_ttl_s)
        except Exception:
            pass
        try:
            _emit_audit_stream(client, composite, stream=stream)
        except Exception:
            pass

    return composite


def main() -> int:
    ap = argparse.ArgumentParser(description='Composite orchestration preflight gate check (P6.3)')
    ap.add_argument(
        '--purpose',
        default='latency_contract_sensitive_apply',
        help='Rollout-sensitive job purpose identifier',
    )
    ap.add_argument(
        '--stage-mode',
        type=int,
        default=0,
        help='Set to 1 to enable stage-mode bypass for certain gates',
    )
    ns = ap.parse_args()
    purpose = ns.purpose
    stage_mode = bool(ns.stage_mode)

    if purpose not in ALLOWED_PURPOSES:
        print(f'COMPOSITE_PREFLIGHT_INVALID purpose={purpose} reason=unknown_purpose')
        return 64

    redis_url = _env('REDIS_URL', 'redis://redis-worker-1:6379/0')
    result = evaluate_composite_gate(redis_url, purpose=purpose, stage_mode=stage_mode)

    status = str(result.get('decision_status') or result.get('status') or 'invalid')
    source = str(result.get('selected_source') or 'none')
    reason = str(result.get('strategy_research_stats_reason') if source == 'strategy_research_stats' else result.get(f'{source}_reason') or 'unknown')

    if status == 'ok':
        print(f'COMPOSITE_PREFLIGHT_OK purpose={purpose} source={source}')
        return 0

    if status == 'block':
        print(f'COMPOSITE_PREFLIGHT_BLOCK purpose={purpose} source={source} reason={reason}')
        return 24

    print(f'COMPOSITE_PREFLIGHT_INVALID purpose={purpose} source={source} reason={reason}')
    return 25


if __name__ == '__main__':
    raise SystemExit(main())
