from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Hard consumer hook for ExecHealth auto-freeze.

Purpose
-------
P5 autoguard can raise a global Redis key signaling sustained rollout drift or
cross-scope mode mismatch. P6 turns that key into an actual *runtime stop* for:
  - SignalPipeline publish path
  - EntryPolicyService entry emit path

Design constraints
------------------
- Single source of truth: read the exact autoguard key written by P5.
- Fail-open: missing Redis / malformed payload must not block trading by itself.
- Low latency: use short in-process TTL cache instead of GET per event.
- Deterministic reason codes across consumers.
"""

import json
import os
from dataclasses import dataclass
from typing import Any

from services.orderflow.exec_health_freeze_control import (
    parse_exec_health_freeze_control,
    verify_thaw_release_signature,
)
from services.orderflow.exec_health_freeze_sealed_state import verify_sealed_hash
from services.orderflow.metrics_exec_health_p6 import (
    exec_health_freeze_hook_active,
    exec_health_freeze_hook_block_total,
    exec_health_freeze_hook_freeze_until_ts_ms,
    exec_health_freeze_hook_reader_errors_total,
    exec_health_freeze_hook_state_age_seconds,
)


@dataclass(frozen=True)
class ExecHealthFreezeState:
    active: bool
    freeze_reason: str
    freeze_until_ts_ms: int
    source_ts_ms: int
    schema_version: int
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class ExecHealthFreezeDecision:
    block: bool
    gate: str
    reason_code: str
    notes: str
    state: ExecHealthFreezeState


# In-process TTL cache: maps (scope, freeze_key) -> (valid_until_ms, state)
_ASYNC_CACHE: dict[tuple[str, str], tuple[int, ExecHealthFreezeState]] = {}


def _state_from_control(scope: str, ctl: Any) -> ExecHealthFreezeState:
    """Convert a P7 ExecHealthFreezeControlState into the legacy ExecHealthFreezeState
    so existing callers (SignalPipeline, EntryPolicyService) need no changes.
    """
    _ = scope
    src_ts = int(
        getattr(ctl, "updated_ts_ms", 0)
        or getattr(ctl, "source_ts_ms", 0)
        or getattr(ctl, "manual_ack_ts_ms", 0)
        or 0
    )
    until_ts = int(getattr(ctl, "freeze_until_ts_ms", 0) or getattr(ctl, "manual_override_until_ts_ms", 0) or 0)
    payload = dict(getattr(ctl, "raw_payload", {}) or {})
    payload["control_source"] = str(getattr(ctl, "control_source", "") or payload.get("control_source") or "")
    return ExecHealthFreezeState(
        active=bool(getattr(ctl, "effective_freeze_active", False)),
        freeze_reason=str(getattr(ctl, "freeze_reason", "") or getattr(ctl, "control_source", "") or ""),
        freeze_until_ts_ms=until_ts,
        source_ts_ms=src_ts,
        schema_version=int(getattr(ctl, "schema_version", 0) or 0),
        raw_payload=payload,
    )


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _b(x: Any) -> bool:
    try:
        if isinstance(x, str):
            return x.strip().lower() in {"1", "true", "yes", "on"}
        return bool(int(x))
    except Exception:
        return False


def _safe_set(metric: Any, *, labels: dict, value: float) -> None:
    try:
        if metric is not None:
            metric.labels(**labels).set(float(value))
    except Exception:
        pass


def _safe_inc(metric: Any, *, labels: dict, value: float = 1.0) -> None:
    try:
        if metric is not None:
            metric.labels(**labels).inc(float(value))
    except Exception:
        pass


def parse_exec_health_auto_freeze(raw: Any, *, now_ms: int | None = None) -> ExecHealthFreezeState:
    """Parse the P5 autoguard freeze key payload into a typed state object.

    Returns an inactive ExecHealthFreezeState for any parse/missing error (fail-open).
    Active=True only when freeze_active flag is set AND freeze_until_ts_ms is in the future.
    """
    now = int(now_ms or _now_ms())
    if not raw:
        return ExecHealthFreezeState(False, "", 0, 0, 0, {})
    try:
        obj = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except Exception:
        return ExecHealthFreezeState(False, "", 0, 0, 0, {})

    until_ts_ms = _i(obj.get("freeze_until_ts_ms"), 0)
    source_ts_ms = _i(obj.get("ts_ms"), 0)
    explicit_active = _b(obj.get("freeze_active"))
    # Active only if explicitly flagged AND not expired
    active = bool(explicit_active and until_ts_ms > now)
    return ExecHealthFreezeState(
        active=active,
        freeze_reason=_s(obj.get("freeze_reason"), ""),
        freeze_until_ts_ms=until_ts_ms,
        source_ts_ms=source_ts_ms,
        schema_version=_i(obj.get("schema_version"), 0),
        raw_payload=obj,
    )


def record_exec_health_freeze_reader_error(*, scope: str, where: str) -> None:
    """Increment the reader error counter (fail-silently itself)."""
    _safe_inc(
        exec_health_freeze_hook_reader_errors_total,
        labels={"scope": (scope or "unknown"), "where": (where or "unknown")},
    )


def record_exec_health_freeze_state(*, scope: str, state: ExecHealthFreezeState, blocked: bool, now_ms: int | None = None) -> None:
    """Update Prometheus gauges and increment block counter if a block occurred."""
    sc = (scope or "unknown")
    now = int(now_ms or _now_ms())
    _safe_set(exec_health_freeze_hook_active, labels={"scope": sc}, value=1.0 if state.active else 0.0)
    _safe_set(exec_health_freeze_hook_freeze_until_ts_ms, labels={"scope": sc}, value=float(state.freeze_until_ts_ms or 0))
    age_s = 0.0
    if int(state.source_ts_ms or 0) > 0 and now >= int(state.source_ts_ms):
        age_s = float(now - int(state.source_ts_ms)) / 1000.0
    _safe_set(exec_health_freeze_hook_state_age_seconds, labels={"scope": sc}, value=age_s)
    if blocked:
        _safe_inc(
            exec_health_freeze_hook_block_total,
            labels={"scope": sc, "reason": str(state.freeze_reason or "exec_health_auto_freeze")},
        )


async def aread_exec_health_auto_freeze(
    *,
    redis: Any,
    scope: str,
    now_ms: int | None = None,
    force: bool = False,
    cache_ttl_ms: int | None = None,
    key: str | None = None,
) -> ExecHealthFreezeState:
    """Async read with in-process TTL cache to avoid one Redis GET per signal.

    Args:
        redis: aioredis client (may be None, fail-open in that case).
        scope: logging/metrics scope label (e.g. "pipeline", "entry_policy").
        now_ms: current time in ms (defaults to wall clock).
        force: bypass cache and always read from Redis.
        cache_ttl_ms: override cache TTL in ms (env: EXEC_HEALTH_AUTO_FREEZE_CACHE_TTL_MS).
        key: override Redis key (env: EXEC_HEALTH_AUTO_FREEZE_KEY).

    Returns:
        ExecHealthFreezeState (fail-open: active=False on Redis errors).
    """
    now = int(now_ms or _now_ms())
    cache_ms = int(cache_ttl_ms or _i(os.getenv("EXEC_HEALTH_AUTO_FREEZE_CACHE_TTL_MS", "1000"), 1000))
    freeze_key = str(key or os.getenv("EXEC_HEALTH_AUTO_FREEZE_KEY", "cfg:orderflow:exec_health:auto_freeze:v1"))
    # P7: latched control hash and autoguard state fallback
    control_key = os.getenv("EXEC_HEALTH_FREEZE_CONTROL_KEY", "cfg:orderflow:exec_health:freeze_control:v1")
    autoguard_state_key = os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY", "metrics:exec_health:slo:autoguard:state")
    cache_key = ((scope or "unknown"), freeze_key)

    # Check in-process cache (skip if force=True for final-safety checks like _emit_entry)
    if not force:
        cached = _ASYNC_CACHE.get(cache_key)
        if cached is not None:
            valid_until_ms, state = cached
            if now <= int(valid_until_ms):
                record_exec_health_freeze_state(scope=str(scope), state=state, blocked=bool(state.active), now_ms=now)
                return state

    # Fail-open if no Redis client
    state = ExecHealthFreezeState(False, "", 0, 0, 0, {})
    if redis is None:
        record_exec_health_freeze_state(scope=str(scope), state=state, blocked=False, now_ms=now)
        _ASYNC_CACHE[cache_key] = (now + max(100, cache_ms), state)
        return state

    # 1) P7 authoritative latched control hash.
    # Even if the raw TTL key has expired, the latch keeps the freeze active
    # until an operator writes a manual_ack via exec_health_freeze_override_v1.py.
    try:
        ctl_raw = await redis.hgetall(control_key) if hasattr(redis, "hgetall") else {}
        ctl = parse_exec_health_freeze_control(ctl_raw, now_ms=now)
        if getattr(ctl, "raw_payload", None):
            raw_payload = dict(getattr(ctl, "raw_payload", {}) or {})
            # P8: if control records a thaw, validate the HMAC signature before trusting it.
            # An invalid/missing signature means the thaw was not properly produced by the
            # signed operator workflow — fall through to the state/raw key path.
            # P11: также проверяем seal — невалидный seal = запись мимо whitelist FCALL entrypoints
            if not verify_sealed_hash(raw_payload):
                record_exec_health_freeze_reader_error(scope=str(scope), where="invalid_control_seal")
            elif getattr(ctl, "manual_override_action", "") == "thaw" and getattr(ctl, "manual_override_active", False):
                # P9: verify_thaw_release_signature enforces dual-control by default
                if verify_thaw_release_signature(raw_payload):
                    state = _state_from_control(str(scope), ctl)
                    _ASYNC_CACHE[cache_key] = (now + max(100, cache_ms), state)
                    record_exec_health_freeze_state(scope=str(scope), state=state, blocked=bool(state.active), now_ms=now)
                    return state
                record_exec_health_freeze_reader_error(scope=str(scope), where="invalid_control_ack_signature")
            else:
                state = _state_from_control(str(scope), ctl)
                _ASYNC_CACHE[cache_key] = (now + max(100, cache_ms), state)
                record_exec_health_freeze_state(scope=str(scope), state=state, blocked=bool(state.active), now_ms=now)
                return state
    except Exception:
        record_exec_health_freeze_reader_error(scope=str(scope), where="read_control")

    # 2) P5 autoguard state hash fallback.
    # Covers the window when autoguard has set state but not yet written the control hash,
    # and also ensures a simple raw key delete can't bypass the latch if manual_ack is pending.
    try:
        st_raw = await redis.hgetall(autoguard_state_key) if hasattr(redis, "hgetall") else {}
        ctl = parse_exec_health_freeze_control(st_raw, now_ms=now)
        if getattr(ctl, "raw_payload", None):
            raw_payload = dict(getattr(ctl, "raw_payload", {}) or {})
            # P9: same dual-control signature check on autoguard state fallback path
            # P11: проверяем seal state hash перед доверием
            if not verify_sealed_hash(raw_payload):
                record_exec_health_freeze_reader_error(scope=str(scope), where="invalid_state_seal")
            elif getattr(ctl, "manual_override_action", "") == "thaw" and getattr(ctl, "manual_override_active", False):
                if verify_thaw_release_signature(raw_payload):
                    state = _state_from_control(str(scope), ctl)
                    _ASYNC_CACHE[cache_key] = (now + max(100, cache_ms), state)
                    record_exec_health_freeze_state(scope=str(scope), state=state, blocked=bool(state.active), now_ms=now)
                    return state
                record_exec_health_freeze_reader_error(scope=str(scope), where="invalid_state_ack_signature")
            else:
                state = _state_from_control(str(scope), ctl)
                _ASYNC_CACHE[cache_key] = (now + max(100, cache_ms), state)
                record_exec_health_freeze_state(scope=str(scope), state=state, blocked=bool(state.active), now_ms=now)
                return state
    except Exception:
        record_exec_health_freeze_reader_error(scope=str(scope), where="read_autoguard_state")

    # 3) Legacy raw TTL key (P5/P6 backward-compat).
    try:
        raw = await redis.get(freeze_key)
        state = parse_exec_health_auto_freeze(raw, now_ms=now)
    except Exception:
        # Fail-open: Redis errors do NOT cause false-positive freezes
        record_exec_health_freeze_reader_error(scope=str(scope), where="read_key")
        state = ExecHealthFreezeState(False, "", 0, 0, 0, {})

    _ASYNC_CACHE[cache_key] = (now + max(100, cache_ms), state)
    record_exec_health_freeze_state(scope=str(scope), state=state, blocked=bool(state.active), now_ms=now)
    return state


def build_exec_health_auto_freeze_decision(
    *,
    scope: str,
    state: ExecHealthFreezeState,
    reason_code: str = "VETO_EXEC_HEALTH_AUTO_FREEZE",
    gate: str = "ExecHealthAutoFreezeGate",
) -> ExecHealthFreezeDecision:
    """Build a structured freeze decision for veto/deny path annotations.

    Default reason_code is VETO_EXEC_HEALTH_AUTO_FREEZE (publish path).
    Entry path callers should pass reason_code="DENY_EXEC_HEALTH_AUTO_FREEZE".
    """
    notes = (
        f"scope={(scope or 'unknown')} freeze_reason={state.freeze_reason or ''} "
        f"freeze_until_ts_ms={int(state.freeze_until_ts_ms or 0)} source_ts_ms={int(state.source_ts_ms or 0)}"
    )
    return ExecHealthFreezeDecision(
        block=bool(state.active),
        gate=str(gate),
        reason_code=str(reason_code),
        notes=notes,
        state=state,
    )
