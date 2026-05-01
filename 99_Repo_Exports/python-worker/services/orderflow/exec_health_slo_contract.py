from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Low-overhead ExecHealth rollout contract state writer.

Purpose
-------
Keep a compact, process-local snapshot of ExecHealth behaviour and periodically
flush it to Redis. A separate SLO-checker / exporter can then validate:
  - rollout drift across edge / pipeline / entry_policy
  - apply / veto / pass share by scope
  - mode / threshold mismatches across instances and deployments

Design constraints
------------------
- No extra Redis write per event on the hot path.
- In-memory counters are updated for every SoT decision.
- Redis flush is rate-limited (default every 5s per scope/process).
- Fail-open: contract writing must never affect trading decisions.
"""

import math
import os
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, MutableMapping, Optional

from services.orderflow.exec_health_rollups import ExecHealthDecision, get_exec_health_policy_from_env

_SCOPE_SET = ("edge", "pipeline", "entry_policy")


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(d)
    if not math.isfinite(v):
        return float(d)
    return float(v)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _s(x: Any, d: str = "") -> str:
    try:
        if x is None:
            return str(d)
        return str(x)
    except Exception:
        return str(d)


@dataclass
class _LocalScopeState:
    scope: str
    instance_id: str
    deploy_id: str
    service_name: str
    process_pid: int
    started_ts_ms: int
    updated_ts_ms: int
    last_flush_ts_ms: int
    last_event_ts_ms: int
    last_profile: str = "default"
    last_mode: str = "monitor"
    last_symbol: str = "UNKNOWN"
    last_reason_code: str = ""
    last_flags_csv: str = ""
    threshold_is_p95_bps: float = 0.0
    threshold_perm_impact_p95_bps: float = 0.0
    threshold_realized_spread_p50_bps: float = -999.0
    total_n: int = 0
    apply_n: int = 0
    veto_n: int = 0
    pass_n: int = 0
    reader_error_n: int = 0


_LOCK = threading.RLock()
_STATE: Dict[str, _LocalScopeState] = {}


def _instance_id() -> str:
    return (
        os.getenv("EXEC_HEALTH_INSTANCE_ID")
        or os.getenv("HOSTNAME")
        or socket.gethostname()
        or "unknown-instance"
    ).strip()



def _deploy_id() -> str:
    return (
        os.getenv("EXEC_HEALTH_DEPLOY_ID")
        or os.getenv("DEPLOY_ID")
        or os.getenv("RELEASE_VERSION")
        or os.getenv("GIT_SHA")
        or "unknown-deploy"
    ).strip()



def _service_name(scope: str) -> str:
    return (
        os.getenv("EXEC_HEALTH_SERVICE_NAME")
        or os.getenv("SERVICE_NAME")
        or f"exec-health-{scope}"
    ).strip()



def _flush_interval_ms() -> int:
    return max(500, _i(os.getenv("EXEC_HEALTH_STATE_FLUSH_INTERVAL_MS", "5000"), 5000))



def _ttl_s() -> int:
    return max(30, _i(os.getenv("EXEC_HEALTH_STATE_TTL_S", "120"), 120))



def _state_prefix() -> str:
    return _s(os.getenv("EXEC_HEALTH_SCOPE_STATE_PREFIX", "metrics:exec_health:scope_state"), "metrics:exec_health:scope_state")



def _state_key(scope: str, instance_id: str) -> str:
    return f"{_state_prefix()}:{scope}:{instance_id}"



def _registry_key() -> str:
    return f"{_state_prefix()}:registry"



def _build_hash(state: _LocalScopeState) -> Dict[str, str]:
    return {
        "schema_name": "exec_health_scope_state",
        "schema_version": "1",
        "scope": str(state.scope),
        "instance_id": str(state.instance_id),
        "deploy_id": str(state.deploy_id),
        "service_name": str(state.service_name),
        "process_pid": str(int(state.process_pid)),
        "started_ts_ms": str(int(state.started_ts_ms)),
        "updated_ts_ms": str(int(state.updated_ts_ms)),
        "last_flush_ts_ms": str(int(state.last_flush_ts_ms)),
        "last_event_ts_ms": str(int(state.last_event_ts_ms)),
        "last_profile": str(state.last_profile),
        "last_mode": str(state.last_mode),
        "last_symbol": str(state.last_symbol),
        "last_reason_code": str(state.last_reason_code),
        "last_flags_csv": str(state.last_flags_csv),
        "threshold_is_p95_bps": f"{float(state.threshold_is_p95_bps):.9f}",
        "threshold_perm_impact_p95_bps": f"{float(state.threshold_perm_impact_p95_bps):.9f}",
        "threshold_realized_spread_p50_bps": f"{float(state.threshold_realized_spread_p50_bps):.9f}",
        "total_n": str(int(state.total_n)),
        "apply_n": str(int(state.apply_n)),
        "veto_n": str(int(state.veto_n)),
        "pass_n": str(int(state.pass_n)),
        "reader_error_n": str(int(state.reader_error_n)),
    }



def _get_state(scope: str) -> _LocalScopeState:
    sc = str(scope or "unknown")
    now = _now_ms()
    with _LOCK:
        st = _STATE.get(sc)
        if st is None:
            st = _LocalScopeState(
                scope=sc,
                instance_id=_instance_id(),
                deploy_id=_deploy_id(),
                service_name=_service_name(sc),
                process_pid=os.getpid(),
                started_ts_ms=now,
                updated_ts_ms=now,
                last_flush_ts_ms=0,
                last_event_ts_ms=0,
            )
            _STATE[sc] = st
        return st



def _should_flush(st: _LocalScopeState, now_ms: int) -> bool:
    return int(now_ms - int(st.last_flush_ts_ms)) >= _flush_interval_ms()



def record_exec_health_contract_state(
    *,
    scope: str,
    profile: str,
    symbol: str,
    decision: Optional[ExecHealthDecision],
    now_ms: Optional[int] = None,
) -> None:
    """Update in-memory counters/state from the already computed SoT decision."""
    ts = int(now_ms or _now_ms())
    st = _get_state(scope)
    pol = get_exec_health_policy_from_env(profile=profile, scope=scope)
    thr = pol.thresholds
    dec = decision
    with _LOCK:
        st.updated_ts_ms = ts
        st.last_event_ts_ms = ts
        st.last_profile = str(profile or pol.profile)
        st.last_mode = str((dec.mode if dec is not None else pol.mode) or pol.mode)
        st.last_symbol = str(symbol or st.last_symbol or "UNKNOWN").upper()
        st.threshold_is_p95_bps = _f(thr.max_is_p95_bps, 0.0)
        st.threshold_perm_impact_p95_bps = _f(thr.max_perm_impact_p95_bps, 0.0)
        st.threshold_realized_spread_p50_bps = _f(thr.min_realized_spread_p50_bps, -999.0)
        st.total_n += 1
        if dec is None:
            st.pass_n += 1
            st.last_reason_code = ""
            st.last_flags_csv = ""
            return
        st.last_reason_code = str(dec.reason_code or "")
        st.last_flags_csv = ",".join(list(dec.flags or []))
        if bool(dec.veto):
            st.veto_n += 1
        elif bool(dec.apply):
            st.apply_n += 1
        else:
            st.pass_n += 1



def record_exec_health_contract_reader_error(*, scope: str) -> None:
    st = _get_state(scope)
    with _LOCK:
        st.updated_ts_ms = _now_ms()
        st.reader_error_n += 1



def _sync_write_hash(redis_client: Any, *, key: str, payload: Mapping[str, str], ttl_s: int) -> None:
    if redis_client is None:
        return
    try:
        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(key, mapping=dict(payload))
        pipe.expire(key, int(ttl_s))
        pipe.sadd(_registry_key(), key)
        pipe.execute()
    except Exception as e:
        # Fallback for thin clients / mocks without pipeline
        try:
            redis_client.hset(key, mapping=dict(payload))
            redis_client.expire(key, int(ttl_s))
            redis_client.sadd(_registry_key(), key)
        except Exception as e2:
            # If both failed, propagate the last error to the caller (maintenance loop)
            raise e2 from e


async def _async_write_hash(redis_client: Any, *, key: str, payload: Mapping[str, str], ttl_s: int) -> None:
    if redis_client is None:
        return
    try:
        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(key, mapping=dict(payload))
        pipe.expire(key, int(ttl_s))
        pipe.sadd(_registry_key(), key)
        await pipe.execute()
    except Exception as e:
        try:
            await redis_client.hset(key, mapping=dict(payload))
            await redis_client.expire(key, int(ttl_s))
            await redis_client.sadd(_registry_key(), key)
        except Exception as e2:
            # Propagate error so the background loop can implement backoff (P4.1)
            raise e2 from e



def flush_exec_health_contract_state_sync(*, redis_client: Any, scope: str, force: bool = False) -> bool:
    st = _get_state(scope)
    now = _now_ms()
    with _LOCK:
        if (not force) and (not _should_flush(st, now)):
            return False
        st.updated_ts_ms = max(int(st.updated_ts_ms), now)
        st.last_flush_ts_ms = now
        payload = _build_hash(st)
        key = _state_key(st.scope, st.instance_id)
    _sync_write_hash(redis_client, key=key, payload=payload, ttl_s=_ttl_s())
    return True


async def flush_exec_health_contract_state_async(*, redis_client: Any, scope: str, force: bool = False) -> bool:
    st = _get_state(scope)
    now = _now_ms()
    with _LOCK:
        if (not force) and (not _should_flush(st, now)):
            return False
        st.updated_ts_ms = max(int(st.updated_ts_ms), now)
        st.last_flush_ts_ms = now
        payload = _build_hash(st)
        key = _state_key(st.scope, st.instance_id)
    await _async_write_hash(redis_client, key=key, payload=payload, ttl_s=_ttl_s())
    return True


__all__ = [
    "record_exec_health_contract_state",
    "record_exec_health_contract_reader_error",
    "flush_exec_health_contract_state_sync",
    "flush_exec_health_contract_state_async",
]
