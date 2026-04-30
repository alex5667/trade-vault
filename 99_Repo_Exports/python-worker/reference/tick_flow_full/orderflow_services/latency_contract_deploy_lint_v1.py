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
        repo_root=ns.repo_root
        purpose=ns.purpose
        env=dict(os.environ)
        compose_file=ns.compose_file or None
        wrapper_file=ns.wrapper_file or None
        unit_file=ns.unit_file or None
        env_file=ns.env_file or None
    )
    payload = render_json(report)
    if ns.json_out:
        p = Path(ns.json_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(payload, encoding='utf-8')
    print(payload)
    return 0 if report.get('ok') else 26


if __name__ == '__main__':
    raise SystemExit(main())
