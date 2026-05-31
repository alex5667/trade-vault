from __future__ import annotations

"""Low-cardinality latency semantic conventions for cross-service stage timing.

Goals
-----
- Single timestamp contract across services using epoch milliseconds only.
- Deterministic helpers that never throw on bad input.
- Explicit stage names so Prometheus/Grafana queries stay stable across runtimes.
- Optional symbol cardinality control via allowlist/collapse semantics.
"""

from typing import Any, Iterable, Optional, Set
import math
import os
import time

SCHEMA_VERSION = 1

FIELD_TS_EVENT_MS = "ts_event_ms"
FIELD_TS_REDIS_READ_MS = "ts_redis_read_ms"
FIELD_TS_FEATURE_MS = "ts_feature_ms"
FIELD_TS_EMIT_MS = "ts_emit_ms"
FIELD_TS_WS_EMIT_MS = "ts_ws_emit_ms"
FIELD_TS_INGEST_SOURCE_MS = "ts_ingest_source_ms"
FIELD_TS_REDIS_XADD_MS = "ts_redis_xadd_ms"

SERVICE_GO_INGEST = "go_ingest"
SERVICE_PYTHON_WORKER = "python_worker"
SERVICE_NEST_GATEWAY = "nest_gateway"
SERVICE_UI = "ui"

STAGE_INGEST_TO_REDIS = "ingest_to_redis"
STAGE_REDIS_TO_FEATURE = "redis_to_feature"
STAGE_FEATURE_TO_EMIT = "feature_to_emit"
STAGE_EMIT_TO_WS = "emit_to_ws"
STAGE_END_TO_END_EVENT = "end_to_end_event"

ALL_STAGES = (
    STAGE_INGEST_TO_REDIS,
    STAGE_REDIS_TO_FEATURE,
    STAGE_FEATURE_TO_EMIT,
    STAGE_EMIT_TO_WS,
    STAGE_END_TO_END_EVENT,
)


def now_wall_ms() -> int:
    return int(time.time() * 1000)


def as_int_ms(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        return int(float(s)) if s else int(default)
    except Exception:
        return int(default)


def non_negative_delta_ms(start_ms: Any, end_ms: Any) -> int:
    s = as_int_ms(start_ms, 0)
    e = as_int_ms(end_ms, 0)
    if s <= 0 or e <= 0:
        return 0
    if e < s:
        return 0
    return int(e - s)


def ensure_epoch_ms_fields(payload: dict[str, Any], *, default_event_ms: int = 0, default_feature_ms: int = 0, default_emit_ms: int = 0) -> dict[str, Any]:
    """Mutates payload in-place and returns it.

    Rules:
    - ts_event_ms is the canonical source event timestamp.
    - ts_feature_ms is the point where Python finished feature/gate computation.
    - ts_emit_ms is the point where a publishable payload is emitted to a sink/outbox.
    - All timestamps are epoch ms UTC, no timezone objects/strings.
    """
    event_ms = as_int_ms(payload.get(FIELD_TS_EVENT_MS) or payload.get('event_ts_ms') or payload.get('ts_ms') or default_event_ms, 0)
    if event_ms > 0:
        payload[FIELD_TS_EVENT_MS] = int(event_ms)
    redis_read_ms = as_int_ms(payload.get(FIELD_TS_REDIS_READ_MS) or payload.get('ingest_ts_ms') or 0, 0)
    if redis_read_ms > 0:
        payload[FIELD_TS_REDIS_READ_MS] = int(redis_read_ms)
    feature_ms = as_int_ms(payload.get(FIELD_TS_FEATURE_MS) or default_feature_ms or event_ms, 0)
    if feature_ms > 0:
        payload[FIELD_TS_FEATURE_MS] = int(feature_ms)
    emit_ms = as_int_ms(payload.get(FIELD_TS_EMIT_MS) or default_emit_ms or 0, 0)
    if emit_ms > 0:
        payload[FIELD_TS_EMIT_MS] = int(emit_ms)
    return payload


def compute_contract_deltas(payload: dict[str, Any]) -> dict[str, int]:
    payload = ensure_epoch_ms_fields(payload)
    out = {
        STAGE_REDIS_TO_FEATURE: non_negative_delta_ms(payload.get(FIELD_TS_REDIS_READ_MS), payload.get(FIELD_TS_FEATURE_MS)),
        STAGE_FEATURE_TO_EMIT: non_negative_delta_ms(payload.get(FIELD_TS_FEATURE_MS), payload.get(FIELD_TS_EMIT_MS)),
        STAGE_END_TO_END_EVENT: non_negative_delta_ms(payload.get(FIELD_TS_EVENT_MS), payload.get(FIELD_TS_EMIT_MS)),
    }
    # External stages may be stamped by Go / NestJS in the same contract later.
    out[STAGE_INGEST_TO_REDIS] = non_negative_delta_ms(payload.get(FIELD_TS_INGEST_SOURCE_MS), payload.get(FIELD_TS_REDIS_XADD_MS))
    out[STAGE_EMIT_TO_WS] = non_negative_delta_ms(payload.get(FIELD_TS_EMIT_MS), payload.get(FIELD_TS_WS_EMIT_MS))
    return out


def parse_allowlist(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {str(x).strip().upper() for x in str(raw).split(',') if str(x).strip()}


def label_symbol(symbol: Any, *, allowlist: Optional[Set[str]] = None, mode: str = "collapse") -> Optional[str]:
    s = str(symbol or "").strip().upper()
    if not s:
        return None
    allow = allowlist or set()
    if not allow:
        return s
    if s in allow:
        return s
    if str(mode or 'collapse').strip().lower() == 'drop':
        return None
    return '__other__'


def default_symbol_allowlist() -> Set[str]:
    return parse_allowlist(os.getenv('LATENCY_CONTRACT_SYMBOL_ALLOWLIST', 'BTCUSDT,ETHUSDT'))


# ---------------------------------------------------------------------------
# P4.1 — required owner-stage matrix
# ---------------------------------------------------------------------------

REQUIRED_STAGE_OWNERS: tuple[tuple[str, str], ...] = (
    (SERVICE_GO_INGEST, STAGE_INGEST_TO_REDIS),
    (SERVICE_PYTHON_WORKER, STAGE_REDIS_TO_FEATURE),
    (SERVICE_PYTHON_WORKER, STAGE_FEATURE_TO_EMIT),
    (SERVICE_NEST_GATEWAY, STAGE_EMIT_TO_WS),
    (SERVICE_NEST_GATEWAY, STAGE_END_TO_END_EVENT),
)


def required_stage_owners() -> tuple[tuple[str, str], ...]:
    """Canonical service->stage ownership for cross-service SLO coverage.

    Python may still emit a provisional end_to_end_event at publish time, but the
    required owner for user-visible end-to-end latency is the Nest gateway because
    only it sees ts_ws_emit_ms.
    """
    return REQUIRED_STAGE_OWNERS


# ---------------------------------------------------------------------------
# P4.2 — external required stage owners (rollout gate subset)
# ---------------------------------------------------------------------------

EXTERNAL_REQUIRED_STAGE_OWNERS: tuple[tuple[str, str], ...] = (
    (SERVICE_GO_INGEST, STAGE_INGEST_TO_REDIS),
    (SERVICE_NEST_GATEWAY, STAGE_EMIT_TO_WS),
    (SERVICE_NEST_GATEWAY, STAGE_END_TO_END_EVENT),
)


def external_required_stage_owners() -> tuple[tuple[str, str], ...]:
    """Subset of required stages that must be written by external runtimes.

    Used by rollout/apply blockers so that Python-side coverage alone cannot mask
    missing Go ingest or NestJS websocket stages.
    """
    return EXTERNAL_REQUIRED_STAGE_OWNERS


def build_external_state_mapping(
    *,
    service: str,
    stage: str,
    symbol: str,
    duration_ms: int,
    payload: dict[str, Any],
    instance_id: str = '',
    source: str = '',
    now_ms: Optional[int] = None,
) -> dict[str, str]:
    """Reference Redis-hash payload for non-Python writers (Go/NestJS).

    Produces the same hash format that LatencyStateWriter.write_async() writes,
    so all service handlers are interchangeable from the exporter's perspective.
    """
    payload = ensure_epoch_ms_fields(dict(payload or {}))
    ts_now = int(now_ms or now_wall_ms())
    mapping: dict[str, str] = {
        'schema_version': str(SCHEMA_VERSION),
        'service': str(service),
        'stage': str(stage),
        'symbol': str(symbol or '').upper(),
        'last_duration_ms': str(int(max(0, duration_ms))),
        'last_ts_ms': str(ts_now),
        FIELD_TS_EVENT_MS: str(as_int_ms(payload.get(FIELD_TS_EVENT_MS), 0)),
        FIELD_TS_REDIS_READ_MS: str(as_int_ms(payload.get(FIELD_TS_REDIS_READ_MS), 0)),
        FIELD_TS_FEATURE_MS: str(as_int_ms(payload.get(FIELD_TS_FEATURE_MS), 0)),
        FIELD_TS_EMIT_MS: str(as_int_ms(payload.get(FIELD_TS_EMIT_MS), 0)),
        FIELD_TS_WS_EMIT_MS: str(as_int_ms(payload.get(FIELD_TS_WS_EMIT_MS), 0)),
        FIELD_TS_INGEST_SOURCE_MS: str(as_int_ms(payload.get(FIELD_TS_INGEST_SOURCE_MS), 0)),
        FIELD_TS_REDIS_XADD_MS: str(as_int_ms(payload.get(FIELD_TS_REDIS_XADD_MS), 0)),
        'instance_id': str(instance_id or ''),
        'source': str(source or ''),
    }
    return mapping
