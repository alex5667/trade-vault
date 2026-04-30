#!/usr/bin/env python3
from __future__ import annotations

"""Preflight guard for sensitive rollout/apply paths (P4.2).

Checks the latency-contract rollout gate state before allowing a sensitive job
to start. This script is designed to be called by shell wrappers or systemd
units before any apply/rollout command.

Exit codes:
- 0: allow — gate is inactive, proceed
- 24: blocked by latency-contract rollout gate
- 25: state missing / invalid, treat as soft infrastructure failure (safer-than-open)

ENV vars:
  REDIS_URL                              (default: redis://redis-worker-1:6379/0)
  LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY (default: metrics:latency_contract:rollout_gate:last)
  LATENCY_CONTRACT_ROLLOUT_GATE_KEY       (default: cfg:orderflow:latency_contract:rollout_gate:v1)
"""

import argparse
import os
from typing import Any

ALLOWED_PURPOSES = {
    'latency_contract_sensitive_apply'
    'conf_score_guardrails_apply'
    'conf_score_guardrails_promote'
    'meta_cov_rollout_controller'
    'conf_score_guardrails_autopromo_controller'
}


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def main() -> int:
    ap = argparse.ArgumentParser(
        description='Latency contract rollout preflight gate check'
    )
    ap.add_argument(
        '--purpose'
        default='latency_contract_sensitive_apply'
        help='Identifier of the job being preflight-checked (logged only)'
    )
    ns = ap.parse_args()
    if ns.purpose not in ALLOWED_PURPOSES:
        print(f'LATENCY_CONTRACT_PREFLIGHT_INVALID purpose={ns.purpose} reason=unknown_purpose')
        return 64

    import redis  # type: ignore
    redis_url = _env('REDIS_URL', 'redis://redis-worker-1:6379/0')
    state_key = _env('LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY', 'metrics:latency_contract:rollout_gate:last')
    gate_key = _env('LATENCY_CONTRACT_ROLLOUT_GATE_KEY', 'cfg:orderflow:latency_contract:rollout_gate:v1')
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # Prefer the active gate key (written only when gate is active) for fast lookup.
    gate = r.hgetall(gate_key) or {}
    if not gate:
        # Fall back to state hash (daemon state, always written).
        gate = r.hgetall(state_key) or {}
    if not gate:
        # Neither key exists — rollout gate daemon may not be running.
        # Fail safer-than-open: return 25 (infrastructure warning, not hard block).
        print(f'LATENCY_CONTRACT_PREFLIGHT_INVALID purpose={ns.purpose} reason=state_missing')
        return 25
    active = _i(gate.get('gate_active'), 0)
    if active > 0:
        reason = gate.get('gate_reason_codes') or gate.get('gate_reason_code') or 'latency_contract_gate'
        print(f'LATENCY_CONTRACT_PREFLIGHT_BLOCK purpose={ns.purpose} reason={reason}')
        return 24
    print(f'LATENCY_CONTRACT_PREFLIGHT_OK purpose={ns.purpose}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
