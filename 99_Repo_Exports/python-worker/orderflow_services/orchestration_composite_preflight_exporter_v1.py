from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""Prometheus exporter for composite orchestration preflight state (P5.4 / P6.4).

The composite preflight persists one compact Redis hash per rollout-sensitive purpose.
This exporter turns that control-plane state into low-cardinality metrics so
Prometheus/Grafana can answer:
  - which purpose is blocked / invalid right now
  - which source currently dominates the orchestration decision
  - which normalized reason-code family is active
  - which *strategy_research_stats* criterion is currently contributing to the
    rollout decision path

The exporter intentionally keeps labels bounded. Raw reason strings from Redis are
normalized into a finite reason-code family set; unknown values collapse into
``<source>:other`` instead of exploding cardinality.
""",
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


logger = logging.getLogger(__name__)

ALLOWED_PURPOSES = (
    'latency_contract_sensitive_apply',
    'conf_score_guardrails_apply',
    'conf_score_guardrails_promote',
    'meta_cov_rollout_controller',
    'conf_score_guardrails_autopromo_controller',
)
KNOWN_STATUSES = ('ok', 'block', 'invalid', 'soft')
KNOWN_SOURCES = ('none', 'deploy_lint', 'latency_contract', 'strategy_research_stats', 'research_guard')
# Bounded family set for strategy_research_stats drilldown (P6.4).
# Unknown sub-reason values fall back to 'other' — no cardinality explosion.
STRATEGY_RESEARCH_STATS_REASON_FAMILIES = (
    'ok',
    'psr_low',
    'dsr_low',
    'pbo_high',
    'metric_low',
    'report_stale',
    'state_missing',
    'invalid',
    'other',
)
KNOWN_REASON_CODES = (
    'ok',
    'deploy_lint:persistent_config_drift',
    'deploy_lint:state_missing',
    'deploy_lint:redis_unavailable',
    'deploy_lint:redis_connect_failed',
    'deploy_lint:other',
    'latency_contract:external_missing',
    'latency_contract:state_missing',
    'latency_contract:redis_unavailable',
    'latency_contract:redis_connect_failed',
    'latency_contract:other',
    'strategy_research_stats:psr_low',
    'strategy_research_stats:dsr_low',
    'strategy_research_stats:pbo_high',
    'strategy_research_stats:metric_low',
    'strategy_research_stats:report_stale',
    'strategy_research_stats:state_missing',
    'strategy_research_stats:invalid',
    'strategy_research_stats:redis_unavailable',
    'strategy_research_stats:redis_connect_failed',
    'strategy_research_stats:stage_allowed',
    'strategy_research_stats:other',
    'research_guard:report_stale',
    'research_guard:state_missing',
    'research_guard:report_only',
    'research_guard:redis_unavailable',
    'research_guard:redis_connect_failed',
    'research_guard:other',
    'none:ok',
)


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_purposes(raw: str) -> list[str]:
    values = [v.strip() for v in str(raw or '').split(',') if v.strip()]
    if not values:
        return list(ALLOWED_PURPOSES)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        out.append(value)
        seen.add(value)
    return out


def _redis_client():
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(_env('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)
    except Exception:
        return None


def _read_hash(client: Any, key: str) -> Dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _state_key(prefix: str, purpose: str) -> str:
    return f'{prefix.rstrip(":")}:{purpose}'


def research_stats_reason_family(raw_reason: str) -> str:
    """Map a raw strategy_research_stats reason string to a bounded family name (P6.4).

    Accepts both 'strategy_research_stats:psr_low' and plain 'psr_low' formats.
    Unknown values collapse to 'other' to keep cardinality bounded.
    """,
    raw = (raw_reason or '').strip().lower()
    if ':' in raw:
        _, raw = raw.split(':', 1)
    raw = raw.strip()
    if not raw or raw == 'ok':
        return 'ok'
    for family in STRATEGY_RESEARCH_STATS_REASON_FAMILIES:
        if family in ('ok', 'other'):
            continue
        if family in raw:
            return family
    return 'other'


def normalize_reason_code(source: str, reason_code: str) -> str:
    src = (source or 'none').strip() or 'none'
    raw = (reason_code or '').strip()
    if not raw:
        return 'none:ok' if src == 'none' else f'{src}:other'

    # Composite state sometimes stores the full source-prefixed code already.
    if ':' in raw:
        prefix, suffix = raw.split(':', 1)
        if prefix:
            src = prefix
            raw = suffix

    suffix = raw.strip()
    known_for_source = {
        'deploy_lint': {'persistent_config_drift', 'state_missing', 'redis_unavailable', 'redis_connect_failed'},
        'latency_contract': {'external_missing', 'state_missing', 'redis_unavailable', 'redis_connect_failed'},
        'strategy_research_stats': {'psr_low', 'dsr_low', 'pbo_high', 'metric_low', 'report_stale', 'state_missing', 'invalid', 'redis_unavailable', 'redis_connect_failed', 'stage_allowed'},
        'research_guard': {'report_stale', 'state_missing', 'report_only', 'redis_unavailable', 'redis_connect_failed'},
        'none': {'ok'},
    }
    if suffix in known_for_source.get(src, set()):
        return f'{src}:{suffix}'
    if src == 'none':
        return 'none:ok'
    return f'{src}:other'


@dataclass(frozen=True)
class PurposeState:
    purpose: str
    present: float = 0.0
    age_seconds: float = 0.0
    selected_priority_rank: float = 999.0
    decision_status: str = 'invalid'
    selected_source: str = 'none'
    selected_reason_code: str = 'none:ok'
    deploy_lint_status: str = 'unknown'
    latency_contract_status: str = 'unknown'
    strategy_research_stats_status: str = 'unknown'
    # P6.4: bounded family for drilldown (psr_low / dsr_low / pbo_high / …)
    strategy_research_stats_reason_family: str = 'ok'
    research_guard_status: str = 'unknown'


def compute_purpose_state(purpose: str, raw: Mapping[str, str], now_ms: int | None = None) -> PurposeState:
    now_ms = int(now_ms if now_ms is not None else get_ny_time_millis())
    if not raw:
        # Use dataclass defaults — avoids duplicating field list here (P6.4 cleanup)
        return PurposeState(purpose=purpose)

    updated_ts_ms = _f(raw.get('updated_ts_ms') or 0.0)
    age_seconds = 0.0
    if updated_ts_ms > 0:
        age_seconds = max(0.0, (now_ms - updated_ts_ms) / 1000.0)

    status = str(raw.get('status') or 'invalid').strip() or 'invalid'
    if status not in KNOWN_STATUSES:
        status = 'invalid'
    source = str(raw.get('selected_source') or 'none').strip() or 'none'
    if source not in KNOWN_SOURCES:
        source = 'none'

    # P6.4: extract raw sub-reason for drilldown family mapping
    strategy_reason_raw = str(
        raw.get('strategy_research_stats_reason')
        or raw.get('research_stats_reason')
        or ''
    )

    return PurposeState(
        purpose=purpose,
        present=1.0,
        age_seconds=age_seconds,
        selected_priority_rank=_f(raw.get('selected_priority_rank') or 999.0),
        decision_status=status,
        selected_source=source,
        selected_reason_code=normalize_reason_code(source, str(raw.get('selected_reason_code') or raw.get('selected_reason') or '')),
        deploy_lint_status=str(raw.get('deploy_lint_status') or 'unknown'),
        latency_contract_status=str(raw.get('latency_contract_status') or 'unknown'),
        strategy_research_stats_status=str(raw.get('strategy_research_stats_status') or raw.get('research_stats_status') or 'unknown'),
        strategy_research_stats_reason_family=research_stats_reason_family(strategy_reason_raw),
        research_guard_status=str(raw.get('research_guard_status') or 'unknown'),
    )


@dataclass(frozen=True)
class SummaryState:
    purposes_total: float
    present_total: float
    block_total: float
    invalid_total: float
    ok_total: float
    soft_total: float  # P6.4: soft-block summary counter


def summarize(states: Iterable[PurposeState]) -> SummaryState:
    values = list(states)
    return SummaryState(
        purposes_total=float(len(values)),
        present_total=float(sum(1 for s in values if s.present > 0)),
        block_total=float(sum(1 for s in values if s.decision_status == 'block')),
        invalid_total=float(sum(1 for s in values if s.decision_status == 'invalid')),
        ok_total=float(sum(1 for s in values if s.decision_status == 'ok')),
        soft_total=float(sum(1 for s in values if s.decision_status == 'soft')),
    )


UP = Gauge('orchestration_composite_preflight_exporter_up', '1 if composite preflight exporter loop is alive')
REDIS_READ_OK = Gauge('orchestration_composite_preflight_exporter_redis_read_ok', '1 if exporter read Redis successfully')
PURPOSE_PRESENT = Gauge('orchestration_composite_preflight_state_present', '1 if persisted composite preflight state exists for the purpose', ['purpose'])
AGE_SECONDS = Gauge('orchestration_composite_preflight_state_age_seconds', 'Age of latest persisted composite preflight decision', ['purpose'])
PRIORITY_RANK = Gauge('orchestration_composite_preflight_selected_priority_rank', 'Priority rank of the selected composite reason', ['purpose'])
DECISION_STATUS = Gauge('orchestration_composite_preflight_decision_status', 'One-hot composite decision status', ['purpose', 'status'])
SELECTED_SOURCE = Gauge('orchestration_composite_preflight_selected_source', 'One-hot selected source dominating the composite decision', ['purpose', 'source'])
SELECTED_REASON_CODE = Gauge('orchestration_composite_preflight_selected_reason_code', 'One-hot normalized selected reason code', ['purpose', 'reason_code'])
SOURCE_STATUS = Gauge('orchestration_composite_preflight_source_status', 'Per-source orchestration preflight status', ['purpose', 'source', 'status'])
# P6.4: strategy_research_stats drilldown gauges — show which family blocks rollout
RESEARCH_STATS_REASON_FAMILY = Gauge(
    'orchestration_composite_preflight_strategy_research_stats_reason_family',
    'Current strategy_research_stats reason family per purpose (one-hot from per-source state)',
    ['purpose', 'family'],
)
RESEARCH_STATS_REASON_FAMILY_TOTAL = Gauge(
    'orchestration_composite_preflight_strategy_research_stats_reason_family_total',
    'Number of purposes currently showing each strategy_research_stats reason family',
    ['family'],
)
SUMMARY_PURPOSES = Gauge('orchestration_composite_preflight_purposes_total', 'Configured orchestration purposes covered by exporter')
SUMMARY_PRESENT = Gauge('orchestration_composite_preflight_present_total', 'Number of purposes with persisted state')
SUMMARY_BLOCK = Gauge('orchestration_composite_preflight_block_total', 'Number of purposes currently blocked')
SUMMARY_INVALID = Gauge('orchestration_composite_preflight_invalid_total', 'Number of purposes currently invalid')
SUMMARY_OK = Gauge('orchestration_composite_preflight_ok_total', 'Number of purposes currently OK')
SUMMARY_SOFT = Gauge('orchestration_composite_preflight_soft_total', 'Number of purposes currently soft-blocked')


def export_states(states: list[PurposeState]) -> None:
    summary = summarize(states)
    SUMMARY_PURPOSES.set(summary.purposes_total)
    SUMMARY_PRESENT.set(summary.present_total)
    SUMMARY_BLOCK.set(summary.block_total)
    SUMMARY_INVALID.set(summary.invalid_total)
    SUMMARY_OK.set(summary.ok_total)
    SUMMARY_SOFT.set(summary.soft_total)  # P6.4

    # P6.4: accumulate family totals across all purposes in a single pass
    family_totals = {family: 0.0 for family in STRATEGY_RESEARCH_STATS_REASON_FAMILIES}
    active_reason_codes = {state.purpose: state.selected_reason_code for state in states}
    for state in states:
        PURPOSE_PRESENT.labels(purpose=state.purpose).set(state.present)
        AGE_SECONDS.labels(purpose=state.purpose).set(state.age_seconds)
        PRIORITY_RANK.labels(purpose=state.purpose).set(state.selected_priority_rank)
        for status in KNOWN_STATUSES:
            DECISION_STATUS.labels(purpose=state.purpose, status=status).set(1.0 if status == state.decision_status else 0.0)
        for source in KNOWN_SOURCES:
            SELECTED_SOURCE.labels(purpose=state.purpose, source=source).set(1.0 if source == state.selected_source else 0.0)
        source_statuses = {
            'deploy_lint': state.deploy_lint_status,
            'latency_contract': state.latency_contract_status,
            'strategy_research_stats': state.strategy_research_stats_status,
            'research_guard': state.research_guard_status,
        }
        for source in KNOWN_SOURCES:
            if source == 'none':
                continue
            for status in KNOWN_STATUSES:
                SOURCE_STATUS.labels(purpose=state.purpose, source=source, status=status).set(1.0 if source_statuses.get(source) == status else 0.0)
        for reason_code in KNOWN_REASON_CODES:
            SELECTED_REASON_CODE.labels(purpose=state.purpose, reason_code=reason_code).set(1.0 if reason_code == active_reason_codes[state.purpose] else 0.0)
        # P6.4: family drilldown — only count when research_stats is an active blocker
        if state.strategy_research_stats_status in ('block', 'invalid', 'soft'):
            family_totals[state.strategy_research_stats_reason_family] += 1.0
        for family in STRATEGY_RESEARCH_STATS_REASON_FAMILIES:
            RESEARCH_STATS_REASON_FAMILY.labels(purpose=state.purpose, family=family).set(
                1.0 if family == state.strategy_research_stats_reason_family else 0.0
            )
    # emit cross-purpose family totals
    for family in STRATEGY_RESEARCH_STATS_REASON_FAMILIES:
        RESEARCH_STATS_REASON_FAMILY_TOTAL.labels(family=family).set(family_totals[family])


def main() -> None:
    port = int(_env('ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_PORT', '9837') or 9837)
    interval_s = float(_env('ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_INTERVAL_S', '15') or 15)
    state_prefix = _env('ORCHESTRATION_PREFLIGHT_STATE_PREFIX', 'metrics:orchestration:preflight:last')
    purposes = _parse_purposes(_env('ORCHESTRATION_COMPOSITE_PREFLIGHT_EXPORTER_PURPOSES', ','.join(ALLOWED_PURPOSES)))

    start_http_server(port)
    logger.info('orchestration composite preflight exporter listening on %s for purposes=%s', port, ','.join(purposes))

    while True:
        UP.set(1.0)
        client = _redis_client()
        if client is None:
            REDIS_READ_OK.set(0.0)
            time.sleep(interval_s)
            continue
        try:
            now_ms = get_ny_time_millis()
            states = [
                compute_purpose_state(purpose, _read_hash(client, _state_key(state_prefix, purpose)), now_ms=now_ms)
                for purpose in purposes
            ]
            export_states(states)
            REDIS_READ_OK.set(1.0)
        except Exception:
            logger.exception('orchestration composite preflight exporter iteration failed')
            REDIS_READ_OK.set(0.0)
        time.sleep(interval_s)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
