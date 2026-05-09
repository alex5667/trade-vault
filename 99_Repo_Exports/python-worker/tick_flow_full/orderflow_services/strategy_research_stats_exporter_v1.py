from __future__ import annotations

import logging
import os
import time
from typing import Any

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.strategy_research_stats_gate_v1 import evaluate_strategy_research_stats_gate

logger = logging.getLogger(__name__)


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _read_hash(client: Any, key: str) -> dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _redis_client():
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(_env('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)
    except Exception:
        return None


# --- Prometheus metrics ---
UP = Gauge('strategy_research_stats_exporter_up', '1 if exporter loop is running')
REDIS_READ_OK = Gauge('strategy_research_stats_exporter_redis_read_ok', '1 if exporter read redis successfully')
SUMMARY_PRESENT = Gauge('strategy_research_stats_summary_present', '1 if summary hash exists')
BLOCKER_PRESENT = Gauge('strategy_research_stats_blocker_present', '1 if blocker hash exists')
REPORT_AGE = Gauge('strategy_research_stats_report_age_seconds', 'Age of latest strategy research stats report')
PRIMARY = Gauge('strategy_research_stats_primary_metric_value', 'Primary strategy research metric')
NET_EXPECTANCY = Gauge('strategy_research_stats_net_expectancy', 'Net expectancy')
PRECISION_TOPX = Gauge('strategy_research_stats_precision_at_top_x', 'Precision@topX')
MEAN_R = Gauge('strategy_research_stats_mean_r', 'Mean R')
DOWNSIDE_RETURN = Gauge('strategy_research_stats_downside_adjusted_return', 'Downside adjusted return')
HIT_RATE_COST = Gauge('strategy_research_stats_hit_rate_conditioned_on_cost', 'Hit rate conditioned on cost')
PSR = Gauge('strategy_research_stats_psr', 'Probabilistic Sharpe ratio proxy')
DSR = Gauge('strategy_research_stats_dsr', 'Deflated Sharpe ratio proxy')
PBO = Gauge('strategy_research_stats_pbo', 'Probability of backtest overfitting')
ROWS = Gauge('strategy_research_stats_rows', 'Row count used in latest report')
PERIODS = Gauge('strategy_research_stats_period_count', 'Period count used in latest report')
VARIANTS = Gauge('strategy_research_stats_variant_count', 'Variant count used in latest report')
BLOCKED = Gauge('strategy_research_stats_blocker_active', '1 if hard blocker is active')
SOFT_BLOCKED = Gauge('strategy_research_stats_soft_block_active', '1 if soft blocker is active')
INVALID = Gauge('strategy_research_stats_invalid_state', '1 if gate state is invalid')
GATE_MODE = Gauge('strategy_research_stats_gate_mode', 'One-hot gate mode', ['mode'])
STATUS = Gauge('strategy_research_stats_gate_status', 'One-hot gate status', ['status'])
REASON = Gauge('strategy_research_stats_reason', 'One-hot blocker reason', ['kind'])

KNOWN_REASONS = ('ok', 'psr_low', 'dsr_low', 'pbo_high', 'metric_low', 'report_stale', 'state_missing', 'invalid', 'other')
KNOWN_STATUSES = ('ok', 'soft', 'block', 'invalid')
KNOWN_MODES = ('report_only', 'soft', 'hard')


def _reason_kind(reason: str) -> str:
    """Map a reason string to the nearest known reason label for Prometheus one-hot."""
    s = (reason or '').strip().lower()
    if not s:
        return 'ok'
    for k in ('psr_low', 'dsr_low', 'pbo_high', 'metric_low', 'report_stale', 'state_missing', 'invalid'):
        if k in s:
            return k
    if s == 'ok':
        return 'ok'
    return 'other'


def main() -> None:
    port = int(_env('STRATEGY_RESEARCH_STATS_EXPORTER_PORT', '9837') or 9837)
    interval_s = float(_env('STRATEGY_RESEARCH_STATS_EXPORTER_INTERVAL_S', '15') or 15)
    summary_key = _env('STRATEGY_RESEARCH_STATS_SUMMARY_KEY', 'metrics:strategy_research_stats:last')
    blocker_key = _env('STRATEGY_RESEARCH_STATS_BLOCKER_KEY', 'cfg:strategy_research_stats:blocker:v1')
    max_age_sec = float(_env('STRATEGY_RESEARCH_STATS_MAX_AGE_SEC', '129600') or 129600)
    fail_closed_missing = int(_env('STRATEGY_RESEARCH_STATS_FAIL_CLOSED_MISSING', '0') or 0)
    start_http_server(port)
    logger.info('strategy research stats exporter listening on %s', port)
    while True:
        UP.set(1.0)
        client = _redis_client()
        if client is None:
            REDIS_READ_OK.set(0.0)
            time.sleep(interval_s)
            continue
        summary = _read_hash(client, summary_key)
        blocker = _read_hash(client, blocker_key)
        state = evaluate_strategy_research_stats_gate(
            _env('REDIS_URL', 'redis://redis-worker-1:6379/0'),
            blocker_key,
            summary_key,
            max_age_sec=max_age_sec,
            fail_closed_missing=fail_closed_missing,
            client=client,
        )
        REDIS_READ_OK.set(1.0)
        SUMMARY_PRESENT.set(1.0 if summary else 0.0)
        BLOCKER_PRESENT.set(1.0 if blocker else 0.0)
        REPORT_AGE.set(float(state.get('age_sec') or 0.0))
        PRIMARY.set(_to_float(summary.get('primary_metric_value', 0.0), 0.0))
        NET_EXPECTANCY.set(_to_float(summary.get('net_expectancy', 0.0), 0.0))
        PRECISION_TOPX.set(_to_float(summary.get('precision_at_top_x', 0.0), 0.0))
        MEAN_R.set(_to_float(summary.get('mean_r', 0.0), 0.0))
        DOWNSIDE_RETURN.set(_to_float(summary.get('downside_adjusted_return', 0.0), 0.0))
        HIT_RATE_COST.set(_to_float(summary.get('hit_rate_conditioned_on_cost', 0.0), 0.0))
        PSR.set(_to_float(summary.get('psr', 0.0), 0.0))
        DSR.set(_to_float(summary.get('dsr', 0.0), 0.0))
        PBO.set(_to_float(summary.get('pbo', 0.0), 0.0))
        ROWS.set(_to_float(summary.get('rows', 0.0), 0.0))
        PERIODS.set(_to_float(summary.get('period_count', 0.0), 0.0))
        VARIANTS.set(_to_float(summary.get('variant_count', 0.0), 0.0))
        BLOCKED.set(1.0 if (state.get('status')) == 'block' else 0.0)
        SOFT_BLOCKED.set(1.0 if (state.get('status')) == 'soft' else 0.0)
        INVALID.set(1.0 if (state.get('status')) == 'invalid' else 0.0)
        for mode in KNOWN_MODES:
            GATE_MODE.labels(mode=mode).set(1.0 if mode == (state.get('gate_mode')) else 0.0)
        for status in KNOWN_STATUSES:
            STATUS.labels(status=status).set(1.0 if status == (state.get('status')) else 0.0)
        rk = _reason_kind((state.get('reason') or ''))
        for reason in KNOWN_REASONS:
            REASON.labels(kind=reason).set(1.0 if reason == rk else 0.0)
        time.sleep(interval_s)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
