#!/usr/bin/env python3
from __future__ import annotations

"""Pre-deploy linter for latency-contract sensitive rollout jobs.

Checks three layers from one source of truth:
1. file layout/binding: compose <-> wrapper <-> systemd unit
2. presence of EnvironmentFile in the systemd unit
3. required runtime env for the chosen sensitive job
"""

import argparse
from pathlib import Path
import os

from services.observability.latency_deploy_contract import lint_deploy_contract, render_json
from services.observability.latency_deploy_lint_state import update_deploy_lint_state


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--purpose', required=True)
    ap.add_argument('--repo-root', default=os.getenv('TRADE_REPO_ROOT') or os.getcwd())
    ap.add_argument('--compose-file', default='')
    ap.add_argument('--wrapper-file', default='')
    ap.add_argument('--unit-file', default='')
    ap.add_argument('--env-file', default=os.getenv('LATENCY_CONTRACT_ENV_FILE', ''))
    ap.add_argument('--json-out', default=os.getenv('LATENCY_CONTRACT_DEPLOY_LINT_REPORT_PATH', ''))
    ns = ap.parse_args()

    report = lint_deploy_contract(
        repo_root=ns.repo_root,
        purpose=ns.purpose,
        env=dict(os.environ),
        compose_file=ns.compose_file or None,
        wrapper_file=ns.wrapper_file or None,
        unit_file=ns.unit_file or None,
        env_file=ns.env_file or None,
    )
    if ns.json_out:
        report['report_path'] = str(ns.json_out)
    payload = render_json(report)
    if ns.json_out:
        p = Path(ns.json_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(payload, encoding='utf-8')

    gate_active = 0
    redis_url = (os.getenv('REDIS_URL') or '').strip()
    if redis_url:
        import redis  # type: ignore

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        state = update_deploy_lint_state(
            r,
            purpose=ns.purpose,
            report=report,
            state_prefix=(os.getenv('LATENCY_CONTRACT_DEPLOY_LINT_STATE_PREFIX') or 'metrics:latency_contract:deploy_lint:last').strip(),
            gate_prefix=(os.getenv('LATENCY_CONTRACT_DEPLOY_LINT_GATE_PREFIX') or 'cfg:orderflow:latency_contract:deploy_lint_gate').strip(),
            hold_s=int(float((os.getenv('LATENCY_CONTRACT_DEPLOY_LINT_PERSIST_HOLD_S') or '1800'))),
            ttl_s=int(float((os.getenv('LATENCY_CONTRACT_DEPLOY_LINT_STATE_TTL_S') or '172800'))),
        )
        gate_active = 1 if str(state.get('gate_active', '0')) == '1' else 0
        report['redis_state'] = state
        payload = render_json(report)
        if ns.json_out:
            p.write_text(payload, encoding='utf-8')

    print(payload)
    if report.get('ok'):
        return 0
    return 27 if gate_active else 26


if __name__ == '__main__':
    raise SystemExit(main())
