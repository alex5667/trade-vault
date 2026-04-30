from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import logging
import os
import time

try:
    from .time_contract import utc_epoch_ms, monotonic_ms
except Exception:
    from time_contract import utc_epoch_ms, monotonic_ms
from typing import Any, Dict, List, Optional

from common.json_fast import dumps1
from common.json_safe import to_json_safe
from common.decision_trace import DecisionTrace, ensure_trace, set_summary_fields, trace_enabled, build_sidecar_meta
from common.outbox_contract import contract_check_best_effort, strip_forbidden_keys, FORBIDDEN_TARGET_KEYS
from common.payload_fingerprint import fingerprint_tradeable_payload
from core.redis_keys import RedisStreams as RS, RedisKeyPrefixes as RK

# Alias for external consumers
dumps_env = dumps1

# IMPORTANT CONTRACT:
#  - tradeable envelope MUST NOT contain full trace/events
#  - envelope contains only trace_id + trace_summary (short)
#  - full trace stored in sidecar OUTBOX_META_PREFIX+sid (via OutboxWriter)
OUTBOX_META_PREFIX = os.getenv("OUTBOX_META_PREFIX", RK.OUTBOX_META)

logger = logging.getLogger(__name__)


def _sanitize_meta(meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Trade-safe: drop diagnostics/heavy keys no matter what caller passes.
    This prevents accidental "trace/events/parts_full/payload_meta" leaks into tradeable envelope.
    """
    deny = {"trace", "events", "parts_full", "payload_meta", "raw_trace", "decision_trace"}
    out: Dict[str, Any] = {}
    if not isinstance(meta, dict):
        return out
    for k, v in meta.items():
        ks = str(k)
        if ks in deny:
            continue
        out[ks] = v
    return out


def build_outbox_envelope(
    *
    sid: str
    ctx: Any = None
    kind: str = ""
    symbol: str = ""
    notify_payload: Optional[Dict[str, Any]] = None
    signal_stream: Optional[str] = None
    signal_stream_payload: Optional[Dict[str, Any]] = None
    audit_stream: Optional[str] = None
    audit_payload: Optional[Dict[str, Any]] = None
    mt5_payload: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None
    trace: Optional[DecisionTrace] = None
) -> Dict[str, Any]:
    """
    Build outbox envelope v2.

    SECURITY/SAFETY:
      - tradeable envelope MUST NOT contain full trace/events
      - envelope contains only trace_id + trace_summary (short)
      - full trace stored in sidecar OUTBOX_META_PREFIX+sid (written by OutboxWriter)
    """
    sid_s = str(sid or "").strip()
    env: Dict[str, Any] = {
        "sid": sid_s
        "ts_ms": get_ny_time_millis()
        "targets": {}
        "meta": {}
    }
    if symbol:
        env["symbol"] = str(symbol)
    if kind:
        env["kind"] = str(kind)

    # meta (json-safe + trade-safe)
    try:
        env["meta"] = to_json_safe(_sanitize_meta(meta))
    except Exception:
        env["meta"] = {}

    targets: Dict[str, Any] = {}

    # notify
    if isinstance(notify_payload, dict):
        try:
            targets["notify"] = strip_forbidden_keys(to_json_safe(notify_payload), FORBIDDEN_TARGET_KEYS)
        except Exception:
            targets["notify"] = {}

    # signal stream
    if signal_stream and isinstance(signal_stream_payload, dict):
        env["meta"]["signal_stream"] = str(signal_stream)
        try:
            targets["signal_stream_payload"] = strip_forbidden_keys(to_json_safe(signal_stream_payload), FORBIDDEN_TARGET_KEYS)
        except Exception:
            targets["signal_stream_payload"] = {}

    # audit
    if audit_stream and isinstance(audit_payload, dict):
        env["meta"]["audit_stream"] = str(audit_stream)
        try:
            targets["audit_payload"] = strip_forbidden_keys(to_json_safe(audit_payload), FORBIDDEN_TARGET_KEYS)
        except Exception:
            targets["audit_payload"] = {}

    # mt5 plan
    if isinstance(mt5_payload, dict):
        try:
            targets["mt5_plan"] = strip_forbidden_keys(to_json_safe(mt5_payload), FORBIDDEN_TARGET_KEYS)
        except Exception:
            targets["mt5_plan"] = {}

    env["targets"] = targets

    # TRACE (summary only in env; full trace lives in sidecar)
    try:
        tr: Optional[DecisionTrace] = trace
        if tr is None and ctx is not None and trace_enabled():
            # create/restore trace on ctx (fail-open)
            tr = ensure_trace(ctx, sid=sid_s)
        if tr is not None:
            set_summary_fields(env, tr)
            env["meta"]["trace_meta_key"] = f"{OUTBOX_META_PREFIX}{sid_s}"
    except Exception:
        pass

    # Final contract enforcement (warn/raise/off inside outbox_contract).
    env_safe = to_json_safe(env)
    contract_check_best_effort(
        kind="envelope"
        obj=env_safe
        where="build_outbox_envelope"
        sid=sid_s
        logger=logger
    )

    # Mutation guard (dispatcher can detect accidental mutations/regressions).
    try:
        sha1, nbytes = fingerprint_tradeable_payload(env_safe)
        m = env_safe.get("meta")
        if isinstance(m, dict):
            m["payload_sha1"] = str(sha1)
            m["payload_bytes"] = int(nbytes)
            m["payload_schema"] = m.get("payload_schema") or "outbox_envelope:v2"
    except Exception:
        pass

    return env_safe


def build_trace_sidecar_meta(*, sid: str, trace: DecisionTrace) -> Dict[str, Any]:
    """
    Canonical sidecar payload stored by OutboxWriter into OUTBOX_META_PREFIX+sid.
    Keep this as a thin wrapper for call sites/tests.
    """
    from common.decision_trace import build_sidecar_meta

    return build_sidecar_meta(trace)


def outbox_stream_record(env: Dict[str, Any]) -> Dict[str, str]:
    """
    SignalDispatcher._parse_envelope() expects fields["data"] to be JSON string.
    Use centralized compact serializer for hot-path.
    """
    return {"data": dumps1(env)}


# New functions from fixes patch

def _now_ms() -> int:
    return utc_epoch_ms()


def _producer_instance_id() -> str:
    """Stable producer identity for replay correlation: hostname:pid or PRODUCER_INSTANCE_ID env."""
    host = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "unknown-host"
    return str(os.getenv("PRODUCER_INSTANCE_ID") or f"{host}:{os.getpid()}")


def _default_working_type_policy(payload: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """Merge working-type policy from payload/meta, falling back to ENV defaults (MARK_PRICE)."""
    src = payload.get("working_type_policy") or meta.get("working_type_policy") or {}
    if not isinstance(src, dict):
        src = {}
    out = dict(src)
    out.setdefault("sl", os.getenv("SL_WORKING_TYPE", "MARK_PRICE"))
    out.setdefault("tp_market", os.getenv("TP_MARKET_WORKING_TYPE", "MARK_PRICE"))
    out.setdefault("tp_limit_trigger", os.getenv("TP_LIMIT_TRIGGER_WORKING_TYPE", "MARK_PRICE"))
    out.setdefault("trail", os.getenv("TRAIL_WORKING_TYPE", "MARK_PRICE"))
    return out


def _default_exit_policy(payload: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """Merge exit policy from payload/meta, filling missing keys from ENV."""
    src = payload.get("exit_policy") or meta.get("exit_policy") or {}
    if not isinstance(src, dict):
        src = {}
    out = dict(src)
    out.setdefault("mode", os.getenv("EXIT_POLICY_MODE", "SAFETY_FIRST"))
    out.setdefault("maker_tp_ladder", str(out.get("mode", "")).upper() == "MAKER_FIRST")
    out.setdefault("watchdog_timeout_ms", int(os.getenv("TP_LIMIT_WATCHDOG_TIMEOUT_MS", "4000")))
    out.setdefault(
        "market_fallback"
        os.getenv("TP_LIMIT_WATCHDOG_MARKET_FALLBACK", "1").strip().lower() in {"1", "true", "yes", "on"}
    )
    return out


def _normalize_execution_policy(payload: Dict[str, Any], meta: Dict[str, Any]) -> str:
    """Resolve execution_policy to SAFETY_FIRST or MAKER_FIRST (unknown values default to SAFETY_FIRST)."""
    raw = str(
        payload.get("execution_policy")
        or meta.get("execution_policy")
        or os.getenv("EXECUTION_POLICY_DEFAULT", "SAFETY_FIRST")
    ).strip().upper()
    return raw if raw in {"SAFETY_FIRST", "MAKER_FIRST"} else "SAFETY_FIRST"


def _risk_snapshot(payload: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """Extract risk_snapshot dict from payload or meta (fail-open → {})."""
    src = payload.get("risk_snapshot") or meta.get("risk_snapshot") or {}
    return dict(src) if isinstance(src, dict) else {}


def _decision_id(sid: str, payload: Dict[str, Any], meta: Dict[str, Any]) -> str:
    """Stable decision identifier for correlation across replay/SoT streams."""
    raw = payload.get("decision_id") or meta.get("decision_id")
    return str(raw or sid)


def _schema_ver(payload: Dict[str, Any], meta: Dict[str, Any]) -> str:
    """Schema version tag for envelope compatibility checks."""
    raw = (
        payload.get("schema_ver")
        or meta.get("schema_ver")
        or os.getenv("OUTBOX_SCHEMA_VER", "execution_intent:v1")
    )
    return str(raw)


def _derive_req_targets(targets_obj: Dict[str, Any]) -> List[str]:
    """Derive dispatcher target names from targets payload dict (best-effort)."""
    t = targets_obj or {}
    out: List[str] = []
    if t.get("notify"):
        out.append("notify")
    if t.get("signal_stream_payload") is not None:
        out.append("signal_stream")
    if t.get("audit_payload") is not None:
        out.append("audit")
    if t.get("manual_payload") is not None:
        out.append("manual")
    if t.get("snapshot_payload") is not None or t.get("snapshot"):
        out.append("snapshot")
    if t.get("mt5_plan") is not None:
        out.append("mt5_plan")
    return out


def build_trace_sidecar_meta_from_ctx(*, ctx: Any, sid: str) -> Dict[str, Any]:
    """Build sidecar meta for DecisionTrace (stored outside tradeable payload)."""
    try:
        trace: DecisionTrace = ensure_trace(ctx, sid=sid)
    except Exception:
        trace = DecisionTrace(trace_id=str(sid), sid=str(sid))
    try:
        set_summary_fields(trace)
    except Exception:
        pass
    side = build_sidecar_meta(trace)
    # Backward-compat: some components expect key 'trace' instead of 'decision_trace'.
    try:
        if isinstance(side, dict) and isinstance(side.get("decision_trace"), dict) and "trace" not in side:
            side["trace"] = side.get("decision_trace")
    except Exception:
        pass
    return side if isinstance(side, dict) else {}


def build_envelope(
    *
    sid: str
    payload: Dict[str, Any]
    targets_obj: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None
    ctx: Any = None
) -> Dict[str, Any]:
    """Build outbox envelope (tradeable payload + routing meta/targets).

    Hard guarantees:
    - env contains 'targets' dict and 'meta' dict.
    - env does NOT include full trace object; trace goes to sidecar only.
    - meta always contains:
        * trace_meta_key
        * req_targets
        * payload_sha1 + payload_bytes (fingerprint of envelope excluding those keys)
    """
    env: Dict[str, Any] = {}
    # Tradeable payload fields at top-level (best-effort strip forbidden keys)
    if isinstance(payload, dict):
        try:
            from common.outbox_contract import strip_forbidden_keys, FORBIDDEN_TRADEABLE_KEYS
            clean_payload = strip_forbidden_keys(payload, FORBIDDEN_TRADEABLE_KEYS)
            env.update(clean_payload)
        except Exception:
            env.update(payload)
    env["sid"] = str(sid)
    env["ts_ms"] = _now_ms()
    env.setdefault("ts_event_ms", env["ts_ms"])
    env.setdefault("ts_publish_ms", env["ts_ms"])
    env.setdefault("mono_ms", monotonic_ms())

    t_obj: Dict[str, Any] = targets_obj if isinstance(targets_obj, dict) else {}
    m_obj: Dict[str, Any] = meta if isinstance(meta, dict) else {}
    p_obj: Dict[str, Any] = payload if isinstance(payload, dict) else {}

    # P3 contract: tradeable payload is a concrete execution intent rather than
    # a raw signal. Materialize stable policy/risk/timing fields at the envelope
    # level so downstream services can route and replay deterministically.
    try:
        event_ms = utc_epoch_ms(p_obj.get("ts_event_ms"))
        publish_ms = _now_ms()
        env["schema_ver"] = _schema_ver(p_obj, m_obj)
        env["decision_id"] = _decision_id(str(sid), p_obj, m_obj)
        env["contract_ver"] = str(
            p_obj.get("contract_ver") or m_obj.get("contract_ver")
            or os.getenv("OUTBOX_CONTRACT_VER", "execution_contract:v1")
        )
        env["execution_plan_id"] = str(
            p_obj.get("execution_plan_id") or m_obj.get("execution_plan_id") or sid
        )
        env["producer_instance_id"] = str(
            p_obj.get("producer_instance_id")
            or m_obj.get("producer_instance_id")
            or _producer_instance_id()
        )
        env["execution_policy"] = _normalize_execution_policy(p_obj, m_obj)
        env["working_type_policy"] = _default_working_type_policy(p_obj, m_obj)
        env["exit_policy"] = _default_exit_policy(p_obj, m_obj)
        env["risk_snapshot"] = _risk_snapshot(p_obj, m_obj)
        env["ts_event_ms"] = event_ms
        env["ts_publish_ms"] = publish_ms
        env["mono_ms"] = monotonic_ms()
    except Exception:
        pass

    # Ensure required targets set survives retries (dispatcher relies on meta.req_targets).
    try:
        m_obj.setdefault("req_targets", _derive_req_targets(t_obj))
    except Exception:
        pass

    # Always publish the expected trace sidecar key for dispatcher.
    try:
        m_obj.setdefault("trace_meta_key", OUTBOX_META_PREFIX + str(sid))
    except Exception:
        pass

    # P3.3: downstream recovery must treat orders:exec as the source of truth.
    # We annotate the authoritative event stream and keep orders:state:* as a
    # materialized view hint only.
    try:
        m_obj.setdefault("state_source_stream", os.getenv("OUTBOX_EVENT_STREAM_KEY", RS.ORDERS_EXEC))
        m_obj.setdefault("state_view_prefix", os.getenv("ORDERS_STATE_KEY_PREFIX", "orders:state:"))
    except Exception:
        pass

    env["targets"] = t_obj
    env["meta"] = m_obj

    # Fingerprint (after json-safe normalization; excludes meta fp keys).
    sha, nbytes = fingerprint_tradeable_payload(to_json_safe(env))
    try:
        m_obj["payload_sha1"] = sha
        m_obj["payload_bytes"] = int(nbytes)
        m_obj["payload_fp_v"] = 1
    except Exception:
        pass

    # Optional: best-effort attach trace_id for correlation (string only).
    try:
        if ctx is not None:
            tr = getattr(ctx, "trace", None)
            if isinstance(tr, DecisionTrace):
                env["trace_id"] = str(tr.trace_id or sid)
    except Exception:
        pass

    return env


# ---------------------------------------------------------------------------
# Diagnostics-only outbox (ENTRY_POLICY_DIAG_STREAM)
# ---------------------------------------------------------------------------


def build_entry_policy_diag_event(
    *
    sid: str
    trace_id: str
    kind: str
    symbol: str
    stage: str
    name: str
    reason_code: str
    metrics: Optional[Dict[str, Any]] = None
    extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a bounded diagnostics-only event.

    This is explicitly NOT a tradeable signal envelope.
    """

    return {
        "v": 1
        "type": "entry_policy_diag"
        "ts_ms": _now_ms()
        "sid": sid
        "trace_id": trace_id
        "kind": kind
        "symbol": symbol
        "stage": stage
        "name": name
        "reason_code": reason_code
        "metrics": metrics or {}
        "extra": extra or {}
    }


def emit_entry_policy_diag_best_effort(
    redis: Any
    event: Dict[str, Any]
    *
    stream: Optional[str] = None
    maxlen: int = 100_000
) -> bool:
    """Best-effort emit to diagnostics stream.

    Never raises. Returns True if XADD attempted.
    """

    try:
        stream_name = (stream or os.getenv("ENTRY_POLICY_DIAG_STREAM", "") or "").strip()
        if not stream_name:
            return False
        if redis is None:
            return False

        payload = _safe_json_dumps(event)

        # Use approximate trimming to keep XADD cheap.
        try:
            redis.xadd(stream_name, {"sid": str(event.get("sid", "")), "data": payload}, maxlen=maxlen, approximate=True)
        except TypeError:
            # Some redis clients use different argument names.
            redis.xadd(stream_name, {"sid": str(event.get("sid", "")), "data": payload}, maxlen=50000)
        return True
    except Exception:
        return False
