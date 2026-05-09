from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

# NOTE:
# This file defines the **wire contract** between producers (emit/pipeline) and
# the dispatcher. Keep it strict and boring.
#
# Tradeable vs diagnostics streams:
# - Tradeable outbox: only OK/SOFT_* signals (consumed by dispatcher).
# - ENTRY_POLICY_DIAG_STREAM: veto/diag events only (NOT consumed by dispatcher).

# Default prefix for trace sidecar keys.
OUTBOX_META_PREFIX = os.getenv("OUTBOX_META_PREFIX", "outbox:meta:")


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_json_dumps(obj: Any) -> str:
    # Deterministic enough for fingerprinting + safe for redis.
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _fingerprint_env_for_contract(env: dict[str, Any]) -> tuple[str, int]:
    """Compute a fingerprint over the envelope, excluding self-referential meta fields."""
    tmp = dict(env)
    meta = dict(tmp.get("meta") or {})
    meta.pop("payload_sha1", None)
    meta.pop("payload_bytes", None)
    tmp["meta"] = meta

    raw = _safe_json_dumps(tmp).encode("utf-8")
    sha1 = hashlib.sha1(raw).hexdigest()
    return sha1, len(raw)


def _derive_req_targets(targets_obj: dict[str, Any]) -> list[str]:
    # Keep stable ordering.
    keys = [str(k) for k in (targets_obj or {}).keys()]
    keys.sort()
    return keys


def build_trace_sidecar_meta(*, ctx: Any, sid: str) -> dict[str, Any]:
    """Minimal trace metadata embedded in the tradeable envelope.

    The full trace is stored in a separate sidecar (OUTBOX_META_PREFIX+sid).
    """
    try:
        from common.decision_trace import ensure_trace

        trace = ensure_trace(ctx)
        trace_id = str(getattr(trace, "trace_id", "") or "")
        span_id = str(getattr(trace, "span_id", "") or "")
        sample_rate = float(getattr(trace, "sample_rate", 0.0) or 0.0)
    except Exception:
        trace_id = ""
        span_id = ""
        sample_rate = 0.0

    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "trace_meta_key": f"{OUTBOX_META_PREFIX}{sid}",
        "sample_rate": sample_rate,
    }


def build_envelope(
    *,
    sid: str,
    payload: dict[str, Any],
    targets_obj: dict[str, Any],
    meta: dict[str, Any] | None = None,
    ctx: Any | None = None,
) -> dict[str, Any]:
    """Build a dispatcher-compatible outbox envelope.

    Contract highlights:
    - The envelope itself must stay bounded.
    - `meta.req_targets` must be present (list[str]) for downstream routing.
    - `meta.payload_sha1/payload_bytes` allow consumers to detect accidental
      mutations when rebuilding envelopes.
    """

    env: dict[str, Any] = {
        "v": 1,
        "sid": sid,
        "ts_ms": _now_ms(),
        "payload": payload if isinstance(payload, dict) else {},
        "targets": targets_obj if isinstance(targets_obj, dict) else {},
        "meta": meta if isinstance(meta, dict) else {},
    }

    # Ensure required metadata exists.
    m = env["meta"]

    # 1) Required routing list.
    if "req_targets" not in m:
        m["req_targets"] = _derive_req_targets(env["targets"])

    # 2) Trace sidecar key + minimal ids.
    if ctx is not None and "trace_meta_key" not in m:
        with contextlib.suppress(Exception):
            m.update(build_trace_sidecar_meta(ctx=ctx, sid=sid))

    # 3) Contract fingerprint.
    sha1, nbytes = _fingerprint_env_for_contract(env)
    m["payload_sha1"] = sha1
    m["payload_bytes"] = nbytes

    return env


# ---------------------------------------------------------------------------
# Diagnostics-only outbox (ENTRY_POLICY_DIAG_STREAM)
# ---------------------------------------------------------------------------


def build_entry_policy_diag_event(
    *,
    sid: str,
    trace_id: str,
    kind: str,
    symbol: str,
    stage: str,
    name: str,
    reason_code: str,
    metrics: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a bounded diagnostics-only event.

    This is explicitly NOT a tradeable signal envelope.
    """

    return {
        "v": 1,
        "type": "entry_policy_diag",
        "ts_ms": _now_ms(),
        "sid": sid,
        "trace_id": trace_id,
        "kind": kind,
        "symbol": symbol,
        "stage": stage,
        "name": name,
        "reason_code": reason_code,
        "metrics": metrics or {},
        "extra": extra or {},
    }


def emit_entry_policy_diag_best_effort(
    redis: Any,
    event: dict[str, Any],
    *,
    stream: str | None = None,
    maxlen: int = 100_000,
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
            redis.xadd(stream_name, {"sid": (event.get("sid", "")), "data": payload}, maxlen=maxlen, approximate=True)
        except TypeError:
            # Some redis clients use different argument names.
            redis.xadd(stream_name, {"sid": (event.get("sid", "")), "data": payload}, maxlen=50000)
        return True
    except Exception:
        return False
