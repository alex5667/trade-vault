#!/usr/bin/env python3
from __future__ import annotations

"""Rollout/apply blocker for latency-contract coverage (P4.2).

Blocks sensitive rollout/apply paths when:
- external required stages are missing (`go_ingest/ingest_to_redis`,
  `nest_gateway/emit_to_ws`, `nest_gateway/end_to_end_event`)
- or `budget_breach_total` stays above threshold for N minutes.

This service reads the P4.1 SLO summary, maintains a small state hash with
hold-duration bookkeeping, and writes an active gate key that preflight wrappers
can check before starting sensitive jobs.

State hash key:
  metrics:latency_contract:rollout_gate:last

Active gate key (written when gate is ON, deleted when gate is OFF):
  cfg:orderflow:latency_contract:rollout_gate:v1

ENV vars:
  LATENCY_CONTRACT_SLO_SUMMARY_KEY           (default: metrics:latency_contract:slo:last)
  LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY    (default: metrics:latency_contract:rollout_gate:last)
  LATENCY_CONTRACT_ROLLOUT_GATE_KEY          (default: cfg:orderflow:latency_contract:rollout_gate:v1)
  LATENCY_CONTRACT_ROLLOUT_GATE_INTERVAL_S   (default: 10)
  LATENCY_CONTRACT_ROLLOUT_GATE_BUDGET_HOLD_S (default: 300)
  LATENCY_CONTRACT_ROLLOUT_GATE_TTL_S        (default: 900)
"""

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple


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
        try:
            return float(str(v).strip())
        except Exception:
            return d


@dataclass
class Cfg:
    redis_url: str
    summary_key: str
    state_key: str
    gate_key: str
    interval_s: float
    budget_hold_s: int
    gate_ttl_s: int


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        summary_key=_env('LATENCY_CONTRACT_SLO_SUMMARY_KEY', 'metrics:latency_contract:slo:last'),
        state_key=_env('LATENCY_CONTRACT_ROLLOUT_GATE_STATE_KEY', 'metrics:latency_contract:rollout_gate:last'),
        gate_key=_env('LATENCY_CONTRACT_ROLLOUT_GATE_KEY', 'cfg:orderflow:latency_contract:rollout_gate:v1'),
        interval_s=float(_env('LATENCY_CONTRACT_ROLLOUT_GATE_INTERVAL_S', '10') or 10),
        budget_hold_s=_i(_env('LATENCY_CONTRACT_ROLLOUT_GATE_BUDGET_HOLD_S', '300'), 300),
        gate_ttl_s=_i(_env('LATENCY_CONTRACT_ROLLOUT_GATE_TTL_S', '900'), 900),
    )


def evaluate_once(r: Any, cfg: Cfg) -> Dict[str, str]:
    """Evaluate rollout gate state from the P4.1 SLO summary.

    Returns the complete state mapping to be written to Redis.
    'gate_active' = '1' means sensitive apply/rollout should be blocked.
    """
    now_ms = int(time.time() * 1000)
    raw = r.hgetall(cfg.summary_key) or {}
    prev = r.hgetall(cfg.state_key) or {}

    if not raw:
        # Missing SLO summary is treated as rollout-blocking because coverage is unknown.
        # Safer-than-open: safer to block than to allow apply without known SLO state.
        mapping = {
            'schema_version': '1',
            'last_ts_ms': str(now_ms),
            'summary_present': '0',
            'external_missing_total': '999999',
            'budget_breach_total': '0',
            'budget_breach_since_ts_ms': str(_i(prev.get('budget_breach_since_ts_ms'), 0)),
            'budget_breach_hold_s': '0',
            'budget_hold_reached': '0',
            'gate_active': '1',
            'gate_reason_code': 'summary_missing',
            'gate_reason_codes': 'summary_missing',
        }
        return mapping

    # P4.2 blocks on missing *external* stages only (go_ingest + nest_gateway),
    # not on Python-side coverage.  external_missing_total is written by the P4.1
    # SLO gate after it was extended in this patch.
    external_missing_total = _i(raw.get('external_missing_total'), 0)
    budget_breach_total = _i(raw.get('budget_breach_total'), 0)

    # Track hold-duration for sustained budget breach.
    prev_since = _i(prev.get('budget_breach_since_ts_ms'), 0)
    if budget_breach_total > 0:
        since_ms = prev_since if prev_since > 0 else now_ms
    else:
        since_ms = 0
    hold_s = int(max(0, (now_ms - since_ms) / 1000.0)) if since_ms > 0 else 0
    hold_reached = 1 if (budget_breach_total > 0 and hold_s >= cfg.budget_hold_s) else 0

    reasons = []
    if external_missing_total > 0:
        reasons.append('external_missing')
    if hold_reached:
        reasons.append('budget_breach_sustained')
    gate_active = 1 if reasons else 0
    reason_code = reasons[0] if reasons else 'ok'

    mapping = {
        'schema_version': '1',
        'last_ts_ms': str(now_ms),
        'summary_present': '1',
        'external_missing_total': str(external_missing_total),
        'budget_breach_total': str(budget_breach_total),
        'budget_breach_since_ts_ms': str(since_ms),
        'budget_breach_hold_s': str(hold_s),
        'budget_hold_reached': str(hold_reached),
        'gate_active': str(gate_active),
        'gate_reason_code': reason_code,
        'gate_reason_codes': ','.join(reasons) if reasons else 'ok',
    }
    return mapping


def reconcile_gate_key(r: Any, cfg: Cfg, mapping: Dict[str, str]) -> None:
    """Write or delete the active gate key based on gate state.

    When gate is active, write the gate key with TTL so that a crashed daemon
    does not leave a stale block indefinitely (TTL defaults to 900s).
    When gate is OK, delete the gate key so preflight sees no block.
    """
    active = _i(mapping.get('gate_active'), 0)
    if active:
        r.hset(cfg.gate_key, mapping=mapping)
        try:
            r.expire(cfg.gate_key, max(1, int(cfg.gate_ttl_s)))
        except Exception:
            pass
    else:
        try:
            r.delete(cfg.gate_key)
        except Exception:
            pass


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    while True:
        try:
            mapping = evaluate_once(r, cfg)
            r.hset(cfg.state_key, mapping=mapping)
            reconcile_gate_key(r, cfg, mapping)
        except Exception:
            pass
        time.sleep(cfg.interval_s)


if __name__ == '__main__':
    raise SystemExit(main())
