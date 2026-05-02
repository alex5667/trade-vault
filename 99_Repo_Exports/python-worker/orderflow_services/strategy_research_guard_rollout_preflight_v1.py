#!/usr/bin/env python3
from __future__ import annotations
"""Preflight guard for strategy research blocker on rollout-sensitive jobs (P5.2).

Exit codes:
- 0: allow
- 24: blocked by active research guard blocker / stale report
- 25: invalid or missing state when fail-closed mode is enabled,
""",
import argparse
import os

from orderflow_services.research_guard_blocker_v1 import evaluate_research_guard_gate

ALLOWED_PURPOSES = {
    'latency_contract_sensitive_apply',
    'conf_score_guardrails_apply',
    'conf_score_guardrails_promote',
    'meta_cov_rollout_controller',
    'conf_score_guardrails_autopromo_controller',
}


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _stage_allowed(purpose: str) -> bool:
    return purpose == 'conf_score_guardrails_apply' and _env('CONF_SCORE_GUARD_STAGE', '0') == '1' and _env('STRATEGY_RESEARCH_GUARD_PREFLIGHT_ALLOW_STAGE', '1') == '1'


def main() -> int:
    ap = argparse.ArgumentParser(description='Strategy research guard rollout preflight gate check')
    ap.add_argument('--purpose', default='latency_contract_sensitive_apply')
    ns = ap.parse_args()

    if ns.purpose not in ALLOWED_PURPOSES:
        print(f'STRATEGY_RESEARCH_GUARD_PREFLIGHT_INVALID purpose={ns.purpose} reason=unknown_purpose')
        return 64

    if _env('ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE', '0') != '1':
        print(f'STRATEGY_RESEARCH_GUARD_PREFLIGHT_DISABLED purpose={ns.purpose}')
        return 0

    if _stage_allowed(ns.purpose):
        print(f'STRATEGY_RESEARCH_GUARD_PREFLIGHT_STAGE_ALLOW purpose={ns.purpose}')
        return 0

    state = evaluate_research_guard_gate(
        _env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        _env('STRATEGY_RESEARCH_GUARD_BLOCKER_KEY', 'cfg:research_guard:blocker:v1'),
        _env('STRATEGY_RESEARCH_GUARD_SUMMARY_KEY', 'metrics:strategy_research_guard:last'),
        max_age_sec=float(_env('STRATEGY_RESEARCH_GUARD_MAX_AGE_SEC', '129600') or 129600),
        fail_closed_missing=int(_env('STRATEGY_RESEARCH_GUARD_FAIL_CLOSED_MISSING', '1') or 1),
    )
    status = str(state.get('status') or 'invalid')
    reason = str(state.get('reason') or 'unknown')
    if status == 'ok':
        print(f'STRATEGY_RESEARCH_GUARD_PREFLIGHT_OK purpose={ns.purpose} reason={reason}')
        return 0
    if status == 'block':
        print(f'STRATEGY_RESEARCH_GUARD_PREFLIGHT_BLOCK purpose={ns.purpose} reason={reason}')
        return 24
    print(f'STRATEGY_RESEARCH_GUARD_PREFLIGHT_INVALID purpose={ns.purpose} reason={reason}')
    return 25


if __name__ == '__main__':
    raise SystemExit(main())
