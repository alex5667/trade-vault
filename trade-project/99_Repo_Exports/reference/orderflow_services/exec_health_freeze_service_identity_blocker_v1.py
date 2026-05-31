#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from typing import List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from services.orderflow.exec_health_freeze_service_identity import evaluate_client_list_against_contract, ensure_service_identity_sync


def _jprint(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='ExecHealth Redis service identity rollout blocker')
    ap.add_argument('--redis-url', default=os.getenv('EXEC_HEALTH_REDIS_AUDIT_URL') or os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0'))
    ap.add_argument('--services', nargs='*', default=[])
    ns = ap.parse_args(argv)
    if redis is None:
        raise RuntimeError('redis dependency missing')
    r = redis.Redis.from_url(ns.redis_url, decode_responses=True)
    ensure_service_identity_sync(r, 'exec_health_freeze_acl_drift_exporter_v1')
    raw = r.execute_command('CLIENT', 'LIST') or ''
    res = evaluate_client_list_against_contract(raw, required_services=list(ns.services or []))
    _jprint(res)
    return 0 if res.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
