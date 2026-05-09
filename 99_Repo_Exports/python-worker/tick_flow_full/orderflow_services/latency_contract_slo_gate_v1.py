#!/usr/bin/env python3
from __future__ import annotations

"""Cross-service latency contract SLO gate (P4.1).

Reads latency contract Redis hashes from Python + external writers (Go/NestJS),
checks required stage ownership coverage, freshness and budget compliance, and
writes a compact summary hash consumed by the exporter / alert rules.

Required stage-owner matrix:
  go_ingest        / ingest_to_redis
  python_worker    / redis_to_feature
  python_worker    / feature_to_emit
  nest_gateway     / emit_to_ws
  nest_gateway     / end_to_end_event

Summary key (written every interval):
  metrics:latency_contract:slo:last
"""

import os
import time
from dataclasses import dataclass
from typing import Any

from services.observability.latency_semconv import (
    default_symbol_allowlist,
    external_required_stage_owners,
    required_stage_owners,
)


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
    key_prefix: str
    summary_key: str
    interval_s: float
    stale_s: int
    symbols: tuple[str, ...]


def load_cfg() -> Cfg:
    syms = tuple(sorted(default_symbol_allowlist()))
    return Cfg(
        redis_url=_env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
        key_prefix=_env('LATENCY_CONTRACT_KEY_PREFIX', 'metrics:latency_contract:last'),
        summary_key=_env('LATENCY_CONTRACT_SLO_SUMMARY_KEY', 'metrics:latency_contract:slo:last'),
        interval_s=float(_env('LATENCY_CONTRACT_SLO_GATE_INTERVAL_S', '10') or 10),
        stale_s=_i(_env('LATENCY_CONTRACT_SLO_STAGE_STALE_S', '180'), 180),
        symbols=syms,
    )


def _budget(stage: str) -> float:
    env_map = {
        'ingest_to_redis': 'LATENCY_BUDGET_INGEST_TO_REDIS_MS',
        'redis_to_feature': 'LATENCY_BUDGET_REDIS_TO_FEATURE_MS',
        'feature_to_emit': 'LATENCY_BUDGET_FEATURE_TO_EMIT_MS',
        'emit_to_ws': 'LATENCY_BUDGET_EMIT_TO_WS_MS',
        'end_to_end_event': 'LATENCY_BUDGET_END_TO_END_EVENT_MS',
    }
    return _f(os.getenv(env_map.get(stage, ''), '0') or '0', 0.0)


def _state_key(prefix: str, service: str, stage: str, symbol: str) -> str:
    return f"{prefix}:{service}:{stage}:{symbol}"


def evaluate_once(r: Any, cfg: Cfg) -> dict[str, str]:
    """Evaluate all required stage owners and return the summary mapping."""
    now = time.time()
    missing_total = 0
    stale_total = 0
    budget_breach_total = 0
    present_total = 0
    required_total = 0
    # P4.2: track missing/stale specifically for external (Go/NestJS) stages.
    external_missing_total = 0
    external_stale_total = 0
    external_required = set(external_required_stage_owners())
    per_stage: dict[str, str] = {}

    for service, stage in required_stage_owners():
        for symbol in cfg.symbols:
            required_total += 1
            key = _state_key(cfg.key_prefix, service, stage, symbol)
            raw = r.hgetall(key) or {}
            last_ts_ms = _i(raw.get('last_ts_ms'), 0)
            last_duration_ms = _f(raw.get('last_duration_ms'), 0.0)
            present = 1 if raw else 0
            stale = 0
            budget_breach = 0

            if not present:
                missing_total += 1
                if (service, stage) in external_required:
                    external_missing_total += 1
            else:
                present_total += 1
                age_s = max(0.0, now - (last_ts_ms / 1000.0)) if last_ts_ms > 0 else float(cfg.stale_s)
                if age_s > float(cfg.stale_s):
                    stale_total += 1
                    stale = 1
                    if (service, stage) in external_required:
                        external_stale_total += 1
                budget = _budget(stage)
                if budget > 0 and last_duration_ms > budget:
                    budget_breach_total += 1
                    budget_breach = 1

            per_stage[f"{service}|{stage}|{symbol}"] = (
                f"{present}:{stale}:{budget_breach}:{last_duration_ms:.3f}"
            )

    gate_ok = 1 if (missing_total == 0 and stale_total == 0) else 0
    mapping: dict[str, str] = {
        'schema_version': '1',
        'last_ts_ms': str(int(now * 1000)),
        'required_total': str(required_total),
        'present_total': str(present_total),
        'missing_total': str(missing_total),
        'stale_total': str(stale_total),
        # P4.2: rollout gate reads these to check external coverage specifically.
        'external_missing_total': str(external_missing_total),
        'external_stale_total': str(external_stale_total),
        'budget_breach_total': str(budget_breach_total),
        'gate_ok': str(gate_ok),
    }
    mapping.update(per_stage)
    return mapping


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore[import]
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    while True:
        try:
            mapping = evaluate_once(r, cfg)
            r.hset(cfg.summary_key, mapping=mapping)
        except Exception:
            pass
        time.sleep(cfg.interval_s)


if __name__ == '__main__':
    raise SystemExit(main())
