from __future__ import annotations

"""Unified latency contract recorder for Python services.

This module is intentionally lightweight:
- Prometheus histograms are the primary source for p95/p99 via histogram_quantile().
- Redis hashes are an auxiliary SoT for latest/freshness/cross-service contract coverage.
- Writers are fail-open and rate-limited to avoid hurting hot-path latency.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set
import asyncio
import os
import time

from prometheus_client import Counter, Gauge, Histogram, REGISTRY

from services.observability.latency_semconv import (
    STAGE_END_TO_END_EVENT,
    STAGE_FEATURE_TO_EMIT,
    STAGE_REDIS_TO_FEATURE,
    FIELD_TS_EMIT_MS,
    FIELD_TS_EVENT_MS,
    FIELD_TS_FEATURE_MS,
    FIELD_TS_REDIS_READ_MS,
    FIELD_TS_WS_EMIT_MS,
    FIELD_TS_INGEST_SOURCE_MS,
    FIELD_TS_REDIS_XADD_MS,
    SERVICE_PYTHON_WORKER,
    compute_contract_deltas,
    default_symbol_allowlist,
    ensure_epoch_ms_fields,
    label_symbol,
    now_wall_ms,
    build_external_state_mapping,
)


def _get_or_create_histogram(name: str, documentation: str, labelnames: list[str], buckets: list[float]) -> Histogram:
    try:
        return Histogram(name, documentation, labelnames, buckets=buckets)
    except ValueError:
        for collector, names in getattr(REGISTRY, '_collector_to_names', {}).items():
            if name in names:
                return collector  # type: ignore[return-value]
        raise


def _get_or_create_counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    try:
        return Counter(name, documentation, labelnames)
    except ValueError:
        for collector, names in getattr(REGISTRY, '_collector_to_names', {}).items():
            if name in names:
                return collector  # type: ignore[return-value]
        raise


def _get_or_create_gauge(name: str, documentation: str, labelnames: list[str]) -> Gauge:
    try:
        return Gauge(name, documentation, labelnames)
    except ValueError:
        for collector, names in getattr(REGISTRY, '_collector_to_names', {}).items():
            if name in names:
                return collector  # type: ignore[return-value]
        raise


LATENCY_BUCKETS = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0]

latency_contract_stage_ms = _get_or_create_histogram(
    'latency_contract_stage_ms',
    'Unified stage latency histogram (ms) for cross-service latency contract',
    ['service', 'stage', 'symbol'],
    LATENCY_BUCKETS,
)
latency_contract_obs_total = _get_or_create_counter(
    'latency_contract_obs_total',
    'Unified latency contract observations total',
    ['service', 'stage', 'symbol'],
)
latency_contract_invalid_total = _get_or_create_counter(
    'latency_contract_invalid_total',
    'Unified latency contract invalid/missing timestamp paths',
    ['service', 'stage', 'reason'],
)
latency_contract_latest_ms = _get_or_create_gauge(
    'latency_contract_latest_ms',
    'Latest observed stage latency in current process (ms)',
    ['service', 'stage', 'symbol'],
)
latency_contract_state_writes_total = _get_or_create_counter(
    'latency_contract_state_writes_total',
    'Redis state hash writes for unified latency contract',
    ['service', 'stage', 'result'],
)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        return int(float(s)) if s else int(default)
    except Exception:
        return int(default)


def build_state_mapping(
    *,
    service: str,
    stage: str,
    symbol: Any,
    duration_ms: int,
    payload: Dict[str, Any],
    now_ms: Optional[int] = None,
    instance_id: str = '',
    source: str = '',
) -> Dict[str, str]:
    """Convenience wrapper around build_external_state_mapping for Python writers.

    Normalises symbol and delegates to the canonical mapping builder so the
    Python writer produces the same hash format as Go/NestJS adapters.
    """
    sym = str(symbol or '').upper()
    return build_external_state_mapping(
        service=service,
        stage=stage,
        symbol=sym,
        duration_ms=int(max(0, duration_ms)),
        payload=payload,
        instance_id=instance_id,
        source=source,
        now_ms=now_ms,
    )


@dataclass
class LatencyStateWriter:
    service: str = SERVICE_PYTHON_WORKER
    key_prefix: str = field(default_factory=lambda: os.getenv('LATENCY_CONTRACT_KEY_PREFIX', 'metrics:latency_contract:last'))
    ttl_s: int = field(default_factory=lambda: _safe_int(os.getenv('LATENCY_CONTRACT_TTL_S', '172800'), 172800))
    min_update_ms: int = field(default_factory=lambda: _safe_int(os.getenv('LATENCY_CONTRACT_STATE_MIN_UPDATE_MS', '3000'), 3000))
    allowlist: Set[str] = field(default_factory=default_symbol_allowlist)
    symbol_mode: str = field(default_factory=lambda: os.getenv('LATENCY_CONTRACT_SYMBOL_LABEL_MODE', 'collapse'))
    _last_write_ms: Dict[str, int] = field(default_factory=dict)

    def _symbol_label(self, symbol: Any) -> Optional[str]:
        return label_symbol(symbol, allowlist=self.allowlist, mode=self.symbol_mode)

    def state_key(self, stage: str, symbol: Any) -> Optional[str]:
        sym = self._symbol_label(symbol)
        if sym is None:
            return None
        return f"{self.key_prefix}:{self.service}:{stage}:{sym}"

    async def write_async(self, redis_client: Any, *, stage: str, symbol: Any, duration_ms: int, payload: Dict[str, Any]) -> None:
        if redis_client is None:
            return
        key = self.state_key(stage, symbol)
        if not key:
            return
        now_ms = now_wall_ms()
        prev = int(self._last_write_ms.get(key, 0) or 0)
        if prev > 0 and (now_ms - prev) < int(self.min_update_ms):
            return
        self._last_write_ms[key] = int(now_ms)
        mapping = build_state_mapping(
            service=self.service,
            stage=stage,
            symbol=key.rsplit(':', 1)[-1],
            duration_ms=duration_ms,
            payload=payload,
            now_ms=now_ms,
            source='python_runtime',
        )
        try:
            await redis_client.hset(key, mapping=mapping)
            if int(self.ttl_s) > 0:
                await redis_client.expire(key, int(self.ttl_s))
            latency_contract_state_writes_total.labels(service=self.service, stage=stage, result='ok').inc()
        except Exception:
            latency_contract_state_writes_total.labels(service=self.service, stage=stage, result='error').inc()


_DEFAULT_WRITER = LatencyStateWriter()


def _observe(service: str, stage: str, symbol: Any, duration_ms: int) -> None:
    sym = _DEFAULT_WRITER._symbol_label(symbol)
    if sym is None:
        return
    latency_contract_stage_ms.labels(service=service, stage=stage, symbol=sym).observe(float(max(0, duration_ms)))
    latency_contract_obs_total.labels(service=service, stage=stage, symbol=sym).inc()
    latency_contract_latest_ms.labels(service=service, stage=stage, symbol=sym).set(float(max(0, duration_ms)))


def stamp_feature_ready(signal: Dict[str, Any], *, tick: Optional[Dict[str, Any]] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
    now_v = int(now_ms or now_wall_ms())
    if tick:
        if 'ts_event_ms' not in signal:
            signal['ts_event_ms'] = _safe_int(tick.get('event_ts_ms') or tick.get('ts_ms') or 0, 0)
        if 'ts_redis_read_ms' not in signal:
            signal['ts_redis_read_ms'] = _safe_int(tick.get('ts_redis_read_ms') or tick.get('ingest_ts_ms') or 0, 0)
    ensure_epoch_ms_fields(signal, default_feature_ms=now_v)
    signal[FIELD_TS_FEATURE_MS] = int(now_v)
    return signal


async def observe_feature_ready_async(signal: Dict[str, Any], *, redis_client: Any = None, service: str = SERVICE_PYTHON_WORKER, symbol: Any = None, writer: Optional[LatencyStateWriter] = None) -> Dict[str, Any]:
    payload = ensure_epoch_ms_fields(signal)
    d = compute_contract_deltas(payload)
    dur = int(d.get(STAGE_REDIS_TO_FEATURE, 0))
    if dur <= 0:
        latency_contract_invalid_total.labels(service=service, stage=STAGE_REDIS_TO_FEATURE, reason='missing_or_non_monotonic').inc()
    else:
        _observe(service, STAGE_REDIS_TO_FEATURE, symbol or payload.get('symbol'), dur)
        await (writer or _DEFAULT_WRITER).write_async(redis_client, stage=STAGE_REDIS_TO_FEATURE, symbol=symbol or payload.get('symbol'), duration_ms=dur, payload=payload)
    return payload


async def stamp_emit_and_observe_async(payload: Dict[str, Any], *, redis_client: Any = None, service: str = SERVICE_PYTHON_WORKER, symbol: Any = None, writer: Optional[LatencyStateWriter] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
    emit_ms = int(now_ms or now_wall_ms())
    ensure_epoch_ms_fields(payload, default_emit_ms=emit_ms)
    payload[FIELD_TS_EMIT_MS] = int(emit_ms)
    d = compute_contract_deltas(payload)
    for stage in (STAGE_FEATURE_TO_EMIT, STAGE_END_TO_END_EVENT):
        dur = int(d.get(stage, 0) or 0)
        if dur <= 0:
            latency_contract_invalid_total.labels(service=service, stage=stage, reason='missing_or_non_monotonic').inc()
            continue
        _observe(service, stage, symbol or payload.get('symbol'), dur)
        await (writer or _DEFAULT_WRITER).write_async(redis_client, stage=stage, symbol=symbol or payload.get('symbol'), duration_ms=dur, payload=payload)
    return payload
