from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Latched control state for ExecHealth auto-freeze / operator thaw.

P7/P8 introduced a latched control layer with signed operator ack. P9 upgrades
that thaw path to a strict two-phase / dual-control workflow:

1. prepare thaw request (request_id + nonce CAS)
2. second operator approves it
3. approved operator emits final signed commit event

The runtime hook only accepts thaw when the dual-control commit signature is
valid. Legacy single-operator thaw can be disabled globally and is disabled by
default in P9.

Redis keys (sources of truth, in priority order)
------------------------------------------------
1. cfg:orderflow:exec_health:freeze_control:v1   — latched P7/P8/P9 control hash
2. metrics:exec_health:slo:autoguard:state       — P5 autoguard state (fallback)
3. cfg:orderflow:exec_health:auto_freeze:v1      — legacy P5/P6 raw TTL key
"""

import hashlib
import hmac
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

ACK_SIGNING_SECRET_ENV = "EXEC_HEALTH_ACK_SIGNING_SECRET"


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


def _secret(explicit: str | None = None) -> str:
    sec = explicit if explicit is not None else os.getenv(ACK_SIGNING_SECRET_ENV, "")
    return (sec or "")


def _require_dual_control(explicit: bool | None = None) -> bool:
    if explicit is not None:
        return explicit
    raw = (os.getenv("EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


@dataclass(frozen=True)
class ExecHealthFreezeControlState:
    """Parsed latched state from cfg:orderflow:exec_health:freeze_control:v1.

    effective_freeze_active — final decision after applying override logic
    control_source          — who set the current state (autoguard / manual_override_thaw / …)
    manual_ack_required     — True => operator must acknowledge before thaw takes effect
    manual_ack_operator     — empty until an operator runs the thaw CLI
    manual_override_active  — True if operator explicitly set freeze or thaw
    manual_override_action  — 'thaw' | 'freeze' | ''
    expected_ack_nonce      — P8: nonce that operator thaw must match (CAS)
    last_trigger_nonce      — P8: nonce from last autoguard trigger
    manual_ack_nonce        — P8: nonce confirmed by operator thaw
    manual_ack_sig          — P8: HMAC-SHA256 signature over the ack fields
    manual_ack_event_id     — P8: Redis stream event ID of the signed ack event
    last_trigger_event_id   — P8: Redis stream event ID of the trigger latch event
    active_thaw_request_id  — P9: dual-control thaw request ID (prepared → approved → committed)
    thaw_request_status     — P9: 'prepared' | 'approved' | 'committed' | 'legacy_committed' | ''
    thaw_request_nonce      — P9: nonce tied to this dual-control request
    thaw_prepare_ts_ms      — P9: timestamp when prepare-thaw was called
    thaw_prepared_by        — P9: operator who called prepare-thaw
    thaw_request_reason     — P9: reason provided at prepare-thaw
    thaw_request_ticket     — P9: ticket provided at prepare-thaw
    thaw_approve_ts_ms      — P9: timestamp when approve-thaw was called
    thaw_approved_by        — P9: second operator who approved (must differ from prepared_by)
    manual_commit_request_id— P9: request_id echoed into the final commit hash
    manual_commit_by        — P9: operator who called commit-thaw
    manual_commit_ts_ms     — P9: timestamp of the commit
    manual_commit_sig       — P9: HMAC-SHA256 dual-control commit signature
    """
    effective_freeze_active: bool
    control_source: str
    freeze_reason: str
    freeze_until_ts_ms: int
    source_ts_ms: int
    updated_ts_ms: int
    schema_version: int
    manual_ack_required: bool
    manual_ack_ts_ms: int
    manual_ack_operator: str
    manual_ack_reason: str
    manual_ack_ticket: str
    manual_override_active: bool
    manual_override_action: str
    manual_override_until_ts_ms: int
    expected_ack_nonce: str
    last_trigger_nonce: str
    manual_ack_nonce: str
    manual_ack_sig: str
    manual_ack_event_id: str
    last_trigger_event_id: str
    # P9 dual-control fields
    active_thaw_request_id: str
    thaw_request_status: str
    thaw_request_nonce: str
    thaw_prepare_ts_ms: int
    thaw_prepared_by: str
    thaw_request_reason: str
    thaw_request_ticket: str
    thaw_approve_ts_ms: int
    thaw_approved_by: str
    manual_commit_request_id: str
    manual_commit_by: str
    manual_commit_ts_ms: int
    manual_commit_sig: str
    raw_payload: dict[str, Any]


def build_manual_ack_signing_message(
    *,
    action: str,
    operator: str,
    reason: str,
    ticket: str,
    ack_nonce: str,
    trigger_ts_ms: int,
    ack_ts_ms: int,
) -> str:
    """Build the canonical signed message string for HMAC-SHA256 signing.

    P8: all fields are pipe-separated; version prefix prevents cross-context reuse.
    """
    parts = [
        "exec_health_manual_ack_v1",
        action or "manual_ack_thaw",
        operator or "",
        reason or "",
        ticket or "",
        ack_nonce or "",
        str(trigger_ts_ms or 0),
        str(ack_ts_ms or 0),
    ]
    return "|".join(parts)


def sign_manual_ack(
    *,
    secret: str | None = None,
    action: str,
    operator: str,
    reason: str,
    ticket: str,
    ack_nonce: str,
    trigger_ts_ms: int,
    ack_ts_ms: int,
) -> str:
    """Sign a manual thaw ack event with HMAC-SHA256.

    Returns empty string if no signing secret is configured (unsigned mode).
    P8: signing secret is read from EXEC_HEALTH_ACK_SIGNING_SECRET env var by default.
    """
    sec = _secret(secret)
    if not sec:
        return ""
    msg = build_manual_ack_signing_message(
        action=action,
        operator=operator,
        reason=reason,
        ticket=ticket,
        ack_nonce=ack_nonce,
        trigger_ts_ms=trigger_ts_ms,
        ack_ts_ms=ack_ts_ms,
    ).encode("utf-8")
    return hmac.new(sec.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def verify_manual_ack_signature(raw: Mapping[str, Any] | None, *, secret: str | None = None) -> bool:
    """Verify the HMAC-SHA256 signature stored in a control/state hash or ack event payload.

    P8: used by exec_health_freeze_hook.py to reject unsigned or tampered thaw records.
    Returns False if no secret is configured, no sig present, or action != 'thaw'.
    """
    obj = dict(raw or {})
    sig = _s(obj.get("manual_ack_sig"), "")
    nonce = _s(obj.get("manual_ack_nonce"), "")
    action = _s(obj.get("manual_override_action"), "") or _s(obj.get("manual_ack_action"), "manual_ack_thaw")
    operator = _s(obj.get("manual_ack_operator"), "")
    reason = _s(obj.get("manual_ack_reason"), "")
    ticket = _s(obj.get("manual_ack_ticket"), "")
    trigger_ts_ms = _i(obj.get("last_trigger_ts_ms"), 0)
    ack_ts_ms = _i(obj.get("manual_ack_ts_ms"), 0)
    if action != "thaw" or not sig or not nonce or ack_ts_ms <= 0:
        return False
    exp = sign_manual_ack(
        secret=secret,
        action="thaw",
        operator=operator,
        reason=reason,
        ticket=ticket,
        ack_nonce=nonce,
        trigger_ts_ms=trigger_ts_ms,
        ack_ts_ms=ack_ts_ms,
    )
    if not exp:
        return False
    return hmac.compare_digest(sig, exp)


def build_dual_control_commit_signing_message(
    *,
    request_id: str,
    ack_nonce: str,
    prepared_by: str,
    approved_by: str,
    commit_by: str,
    reason: str,
    ticket: str,
    trigger_ts_ms: int,
    prepared_ts_ms: int,
    approved_ts_ms: int,
    commit_ts_ms: int,
) -> str:
    """Build canonical signing message for P9 dual-control commit.

    Version prefix 'exec_health_dual_control_commit_v1' ensures no cross-context reuse.
    All three timestamps are included to bind the signature to the exact dual-control chain.
    """
    parts = [
        "exec_health_dual_control_commit_v1",
        (request_id or ""),
        (ack_nonce or ""),
        (prepared_by or ""),
        (approved_by or ""),
        (commit_by or ""),
        (reason or ""),
        (ticket or ""),
        str(trigger_ts_ms or 0),
        str(prepared_ts_ms or 0),
        str(approved_ts_ms or 0),
        str(commit_ts_ms or 0),
    ]
    return "|".join(parts)


def sign_dual_control_commit(
    *,
    secret: str | None = None,
    request_id: str,
    ack_nonce: str,
    prepared_by: str,
    approved_by: str,
    commit_by: str,
    reason: str,
    ticket: str,
    trigger_ts_ms: int,
    prepared_ts_ms: int,
    approved_ts_ms: int,
    commit_ts_ms: int,
) -> str:
    """Sign a P9 dual-control commit event with HMAC-SHA256.

    Signature binds: request_id, nonce, all three operators, timestamps.
    Returns empty string if no secret configured (unsigned mode).
    """
    sec = _secret(secret)
    if not sec:
        return ""
    msg = build_dual_control_commit_signing_message(
        request_id=request_id,
        ack_nonce=ack_nonce,
        prepared_by=prepared_by,
        approved_by=approved_by,
        commit_by=commit_by,
        reason=reason,
        ticket=ticket,
        trigger_ts_ms=trigger_ts_ms,
        prepared_ts_ms=prepared_ts_ms,
        approved_ts_ms=approved_ts_ms,
        commit_ts_ms=commit_ts_ms,
    ).encode("utf-8")
    return hmac.new(sec.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def verify_dual_control_commit_signature(raw: Mapping[str, Any] | None, *, secret: str | None = None) -> bool:
    """Verify P9 dual-control commit HMAC-SHA256 signature.

    Enforces: distinct prepared_by != approved_by, all timestamps > 0, valid sig.
    Returns False on any mismatch or missing required field.
    """
    obj = dict(raw or {})
    request_id = _s(obj.get("manual_commit_request_id"), "") or _s(obj.get("active_thaw_request_id"), "")
    ack_nonce = _s(obj.get("manual_ack_nonce"), "") or _s(obj.get("thaw_request_nonce"), "") or _s(obj.get("expected_ack_nonce"), "")
    prepared_by = _s(obj.get("thaw_prepared_by"), "")
    approved_by = _s(obj.get("thaw_approved_by"), "")
    commit_by = _s(obj.get("manual_commit_by"), "") or _s(obj.get("manual_ack_operator"), "")
    reason = _s(obj.get("thaw_request_reason"), "") or _s(obj.get("manual_ack_reason"), "")
    ticket = _s(obj.get("thaw_request_ticket"), "") or _s(obj.get("manual_ack_ticket"), "")
    trigger_ts_ms = _i(obj.get("last_trigger_ts_ms"), 0)
    prepared_ts_ms = _i(obj.get("thaw_prepare_ts_ms"), 0)
    approved_ts_ms = _i(obj.get("thaw_approve_ts_ms"), 0)
    commit_ts_ms = _i(obj.get("manual_commit_ts_ms"), 0) or _i(obj.get("manual_ack_ts_ms"), 0)
    sig = _s(obj.get("manual_commit_sig"), "") or _s(obj.get("manual_ack_sig"), "")
    if not request_id or not ack_nonce or not prepared_by or not approved_by or not commit_by or not sig:
        return False
    if prepared_by == approved_by:
        return False
    if approved_ts_ms <= 0 or prepared_ts_ms <= 0 or commit_ts_ms <= 0:
        return False
    exp = sign_dual_control_commit(
        secret=secret,
        request_id=request_id,
        ack_nonce=ack_nonce,
        prepared_by=prepared_by,
        approved_by=approved_by,
        commit_by=commit_by,
        reason=reason,
        ticket=ticket,
        trigger_ts_ms=trigger_ts_ms,
        prepared_ts_ms=prepared_ts_ms,
        approved_ts_ms=approved_ts_ms,
        commit_ts_ms=commit_ts_ms,
    )
    if not exp:
        return False
    return hmac.compare_digest(sig, exp)


def verify_thaw_release_signature(
    raw: Mapping[str, Any] | None,
    *,
    secret: str | None = None,
    require_dual_control: bool | None = None,
) -> bool:
    """Unified thaw signature check for runtime hook (P9).

    If dual-control is required (default in P9) or a request_id is present,
    delegates to verify_dual_control_commit_signature. Otherwise falls back to
    legacy P8 verify_manual_ack_signature. This is the single entry point used
    by exec_health_freeze_hook.py and parse_exec_health_freeze_control.
    """
    obj = dict(raw or {})
    request_id = _s(obj.get("manual_commit_request_id"), "") or _s(obj.get("active_thaw_request_id"), "")
    dual_required = _require_dual_control(require_dual_control) or bool(request_id)
    if dual_required:
        return verify_dual_control_commit_signature(obj, secret=secret)
    return verify_manual_ack_signature(obj, secret=secret)


def parse_exec_health_freeze_control(raw: Any, *, now_ms: int | None = None) -> ExecHealthFreezeControlState:
    """Parse a Redis hash (dict) or JSON string into ExecHealthFreezeControlState.

    Logic (in priority order):
    1. manual_override_active + manual_override_action='freeze'  → effective = True
    2. manual_override_active + manual_override_action='thaw' + valid dual-control sig → effective = False
    3. manual_ack_required (latch from autoguard, raw TTL may have expired) → effective = True
    4. raw effective_freeze_active flag from the hash

    P9: thaw in step 2 requires verify_thaw_release_signature (dual-control by default).
    Fail-open: any parse error returns effective_freeze_active=False.
    """
    now = now_ms or _now_ms()
    obj: dict[str, Any] = {}
    if raw:
        try:
            if isinstance(raw, Mapping):
                obj = dict(raw)
            elif isinstance(raw, str):
                obj = json.loads(raw)
            else:
                obj = dict(raw)
        except Exception:
            obj = {}

    source = _s(obj.get("control_source"), "")
    freeze_reason = _s(obj.get("freeze_reason"), "")
    freeze_until_ts_ms = _i(obj.get("freeze_until_ts_ms"), 0)
    source_ts_ms = _i(obj.get("source_ts_ms"), 0)
    updated_ts_ms = _i(obj.get("updated_ts_ms"), 0)
    schema_version = _i(obj.get("schema_version"), 0)
    manual_ack_required = _b(obj.get("manual_ack_required"))
    manual_ack_ts_ms = _i(obj.get("manual_ack_ts_ms"), 0)
    manual_ack_operator = _s(obj.get("manual_ack_operator"), "")
    manual_ack_reason = _s(obj.get("manual_ack_reason"), "")
    manual_ack_ticket = _s(obj.get("manual_ack_ticket"), "")
    manual_override_active = _b(obj.get("manual_override_active"))
    manual_override_action = _s(obj.get("manual_override_action"), "")
    manual_override_until_ts_ms = _i(obj.get("manual_override_until_ts_ms"), 0)
    # P8: nonce / signed-ack fields
    expected_ack_nonce = _s(obj.get("expected_ack_nonce"), "")
    last_trigger_nonce = _s(obj.get("last_trigger_nonce"), expected_ack_nonce)
    manual_ack_nonce = _s(obj.get("manual_ack_nonce"), "")
    manual_ack_sig = _s(obj.get("manual_ack_sig"), "")
    manual_ack_event_id = _s(obj.get("manual_ack_event_id"), "")
    last_trigger_event_id = _s(obj.get("last_trigger_event_id"), "")
    # P9: dual-control thaw request fields
    active_thaw_request_id = _s(obj.get("active_thaw_request_id"), "")
    thaw_request_status = _s(obj.get("thaw_request_status"), "")
    thaw_request_nonce = _s(obj.get("thaw_request_nonce"), "") or expected_ack_nonce
    thaw_prepare_ts_ms = _i(obj.get("thaw_prepare_ts_ms"), 0)
    thaw_prepared_by = _s(obj.get("thaw_prepared_by"), "")
    thaw_request_reason = _s(obj.get("thaw_request_reason"), "")
    thaw_request_ticket = _s(obj.get("thaw_request_ticket"), "")
    thaw_approve_ts_ms = _i(obj.get("thaw_approve_ts_ms"), 0)
    thaw_approved_by = _s(obj.get("thaw_approved_by"), "")
    manual_commit_request_id = _s(obj.get("manual_commit_request_id"), "")
    manual_commit_by = _s(obj.get("manual_commit_by"), "")
    manual_commit_ts_ms = _i(obj.get("manual_commit_ts_ms"), 0)
    manual_commit_sig = _s(obj.get("manual_commit_sig"), "")

    effective = _b(obj.get("effective_freeze_active"))

    if manual_override_active and manual_override_action == "freeze":
        # Manual force-freeze: active until until_ts_ms or indefinitely if 0
        if manual_override_until_ts_ms <= 0 or manual_override_until_ts_ms > now:
            effective = True
            source = source or "manual_override_freeze"
    elif manual_override_active and manual_override_action == "thaw" and manual_ack_ts_ms > 0:
        # P9: operator wrote explicit thaw ack — must pass dual-control signature check
        if verify_thaw_release_signature(obj):
            if manual_override_until_ts_ms <= 0 or manual_override_until_ts_ms > now:
                effective = False
                source = source or "manual_override_thaw"
        elif _i(obj.get("last_trigger_ts_ms"), 0) > 0 or manual_ack_required or freeze_until_ts_ms > now:
            # Signature failed — keep frozen to honour fail-safe behaviour
            effective = True
            source = source or "autoguard"
    elif manual_ack_required and _i(obj.get("last_trigger_ts_ms"), 0) > 0:
        # Latched freeze survives expiry of the raw TTL key until an operator
        # explicitly records dual-control commit with a valid signed ack event.
        effective = True
        source = source or "autoguard"

    return ExecHealthFreezeControlState(
        effective_freeze_active=effective,
        control_source=source,
        freeze_reason=freeze_reason,
        freeze_until_ts_ms=freeze_until_ts_ms,
        source_ts_ms=source_ts_ms,
        updated_ts_ms=updated_ts_ms,
        schema_version=schema_version,
        manual_ack_required=manual_ack_required,
        manual_ack_ts_ms=manual_ack_ts_ms,
        manual_ack_operator=manual_ack_operator,
        manual_ack_reason=manual_ack_reason,
        manual_ack_ticket=manual_ack_ticket,
        manual_override_active=manual_override_active,
        manual_override_action=manual_override_action,
        manual_override_until_ts_ms=manual_override_until_ts_ms,
        expected_ack_nonce=expected_ack_nonce,
        last_trigger_nonce=last_trigger_nonce,
        manual_ack_nonce=manual_ack_nonce,
        manual_ack_sig=manual_ack_sig,
        manual_ack_event_id=manual_ack_event_id,
        last_trigger_event_id=last_trigger_event_id,
        active_thaw_request_id=active_thaw_request_id,
        thaw_request_status=thaw_request_status,
        thaw_request_nonce=thaw_request_nonce,
        thaw_prepare_ts_ms=thaw_prepare_ts_ms,
        thaw_prepared_by=thaw_prepared_by,
        thaw_request_reason=thaw_request_reason,
        thaw_request_ticket=thaw_request_ticket,
        thaw_approve_ts_ms=thaw_approve_ts_ms,
        thaw_approved_by=thaw_approved_by,
        manual_commit_request_id=manual_commit_request_id,
        manual_commit_by=manual_commit_by,
        manual_commit_ts_ms=manual_commit_ts_ms,
        manual_commit_sig=manual_commit_sig,
        raw_payload=obj,
    )


def build_autoguard_latch_update(
    *,
    prev: Mapping[str, Any] | None,
    now_ms: int,
    reasons: list[str],
    freeze_until_ts_ms: int,
    ack_nonce: str | None = None,
    trigger_event_id: str = "",
) -> dict[str, Any]:
    """Build the control hash payload written by autoguard on freeze trigger.

    Sets manual_ack_required=1 so that raw key deletion is not enough to thaw.
    Increments trigger_total counter for exporter tracking.
    P8: stores pending ack nonce for CAS check at thaw time.
    P9: clears all dual-control fields so a fresh prepare-thaw must be initiated.
    """
    p = dict(prev or {})
    nonce = ack_nonce or p.get("expected_ack_nonce") or f"ack-{now_ms}"
    return {
        "schema_name": "exec_health_freeze_control",
        "schema_version": 4,
        "updated_ts_ms": now_ms,
        "effective_freeze_active": 1,
        "control_source": "autoguard",
        "freeze_reason": ",".join(x for x in reasons if x.strip()),
        "freeze_until_ts_ms": freeze_until_ts_ms,
        "source_ts_ms": now_ms,
        # ACK fields — cleared until operator explicitly thaws with valid nonce+sig
        "manual_ack_required": 1,
        "manual_ack_ts_ms": 0,
        "manual_ack_operator": "",
        "manual_ack_reason": "",
        "manual_ack_ticket": "",
        "manual_ack_nonce": "",
        "manual_ack_sig": "",
        "manual_ack_event_id": "",
        # No manual override active — autoguard is the source
        "manual_override_active": 0,
        "manual_override_action": "",
        "manual_override_until_ts_ms": 0,
        # P8: pending nonce that operator thaw must match (CAS)
        "expected_ack_nonce": nonce,
        "last_trigger_nonce": nonce,
        # Audit fields
        "last_trigger_ts_ms": now_ms,
        "last_trigger_event_id": (trigger_event_id or ""),
        "last_trigger_ts_iso": "",
        "last_trigger_reasons_json": json.dumps(list(reasons), ensure_ascii=False),
        "last_operator_action": "autoguard_freeze",
        # Running counters (carry forward from previous state)
        "trigger_total": _i(p.get("trigger_total"), 0) + 1,
        "thaw_total": _i(p.get("thaw_total"), 0),
        "manual_freeze_total": _i(p.get("manual_freeze_total"), 0),
        # P9: dual-control fields cleared on new trigger
        "active_thaw_request_id": "",
        "thaw_request_status": "",
        "thaw_request_nonce": nonce,
        "thaw_prepare_ts_ms": 0,
        "thaw_prepared_by": "",
        "thaw_request_reason": "",
        "thaw_request_ticket": "",
        "thaw_approve_ts_ms": 0,
        "thaw_approved_by": "",
        "manual_commit_request_id": "",
        "manual_commit_by": "",
        "manual_commit_ts_ms": 0,
        "manual_commit_sig": "",
        # P10: request-log event IDs cleared on new trigger
        "thaw_prepare_request_event_id": "",
        "thaw_approve_request_event_id": "",
        "thaw_commit_request_event_id": "",
    }


def build_manual_ack_thaw_update(
    *,
    prev: Mapping[str, Any] | None,
    now_ms: int,
    operator: str,
    reason: str,
    ticket: str,
    provided_ack_nonce: str = "",
    manual_ack_sig: str = "",
    manual_ack_event_id: str = "",
) -> dict[str, Any]:
    """Build the control hash payload written by an operator thaw (legacy P8 single-operator path).

    P9: this path is still available but verify_thaw_release_signature will reject it
    when EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK=1 (the default). Populated with
    legacy_committed status markers for backward-compatibility with integrity exporter.
    """
    p = dict(prev or {})
    expected = _s(p.get("expected_ack_nonce"), "")
    nonce = provided_ack_nonce or expected or f"ack-{now_ms}"
    return {
        "schema_name": "exec_health_freeze_control",
        "schema_version": 4,
        "updated_ts_ms": now_ms,
        "effective_freeze_active": 0,
        "control_source": "manual_override_thaw",
        "freeze_reason": _s(p.get("freeze_reason"), reason),
        "freeze_until_ts_ms": 0,
        "source_ts_ms": now_ms,
        # ACK written — clears the latch
        "manual_ack_required": 0,
        "manual_ack_ts_ms": now_ms,
        "manual_ack_operator": operator,
        "manual_ack_reason": reason,
        "manual_ack_ticket": ticket,
        # P8: signed ack nonce + signature + event stream ID
        "manual_ack_nonce": nonce,
        "manual_ack_sig": manual_ack_sig,
        "manual_ack_event_id": (manual_ack_event_id or ""),
        # Override: thaw
        "manual_override_active": 1,
        "manual_override_action": "thaw",
        "manual_override_until_ts_ms": 0,
        # P8: carry forward nonce fields for integrity exporter correlation
        "expected_ack_nonce": expected,
        "last_trigger_nonce": _s(p.get("last_trigger_nonce"), expected),
        "last_trigger_ts_ms": _i(p.get("last_trigger_ts_ms"), 0),
        "last_trigger_event_id": _s(p.get("last_trigger_event_id"), ""),
        "last_operator_action": "manual_ack_thaw",
        # Counters
        "thaw_total": _i(p.get("thaw_total"), 0) + 1,
        "trigger_total": _i(p.get("trigger_total"), 0),
        "manual_freeze_total": _i(p.get("manual_freeze_total"), 0),
        # P9: legacy single-operator commit markers for integrity exporter
        "active_thaw_request_id": "",
        "thaw_request_status": "legacy_committed",
        "thaw_request_nonce": nonce,
        "thaw_prepare_ts_ms": now_ms,
        "thaw_prepared_by": operator,
        "thaw_request_reason": reason,
        "thaw_request_ticket": ticket,
        "thaw_approve_ts_ms": now_ms,
        "thaw_approved_by": operator,
        "manual_commit_request_id": "legacy-single-operator",
        "manual_commit_by": operator,
        "manual_commit_ts_ms": now_ms,
        "manual_commit_sig": manual_ack_sig,
        # P10: no request-log event for legacy single-operator path
        "thaw_prepare_request_event_id": "",
        "thaw_approve_request_event_id": "",
        "thaw_commit_request_event_id": (manual_ack_event_id or ""),
    }


def build_thaw_prepare_update(
    *,
    prev: Mapping[str, Any] | None,
    now_ms: int,
    request_id: str,
    operator: str,
    reason: str,
    ticket: str,
    provided_ack_nonce: str,
    request_event_id: str = "",
) -> dict[str, Any]:
    """Build the control hash payload for P9 prepare-thaw phase.

    Keeps effective_freeze_active=1 — only commit-thaw clears the freeze.
    Clears all previous ACK/override fields so a fresh dual-control chain starts.
    """
    p = dict(prev or {})
    expected = _s(p.get("expected_ack_nonce"), "")
    nonce = provided_ack_nonce or expected or f"ack-{now_ms}"
    return {
        **{k: v for k, v in p.items() if k not in {
            "updated_ts_ms", "control_source",
            "active_thaw_request_id", "thaw_request_status", "thaw_request_nonce",
            "thaw_prepare_ts_ms", "thaw_prepared_by", "thaw_request_reason", "thaw_request_ticket",
            "thaw_approve_ts_ms", "thaw_approved_by",
            "thaw_prepare_request_event_id", "thaw_approve_request_event_id", "thaw_commit_request_event_id",
            "manual_commit_request_id", "manual_commit_by", "manual_commit_ts_ms", "manual_commit_sig",
        }},
        "schema_name": "exec_health_freeze_control",
        "schema_version": 4,
        "updated_ts_ms": now_ms,
        "control_source": "manual_thaw_prepare",
        "effective_freeze_active": 1,
        "manual_ack_required": 1,
        "manual_override_active": 0,
        "manual_override_action": "",
        "manual_override_until_ts_ms": 0,
        "manual_ack_ts_ms": 0,
        "manual_ack_operator": "",
        "manual_ack_reason": "",
        "manual_ack_ticket": "",
        "manual_ack_nonce": "",
        "manual_ack_sig": "",
        "manual_ack_event_id": "",
        "expected_ack_nonce": expected or nonce,
        "thaw_request_nonce": nonce,
        "active_thaw_request_id": request_id,
        "thaw_request_status": "prepared",
        "thaw_prepare_ts_ms": now_ms,
        "thaw_prepared_by": operator,
        "thaw_request_reason": reason,
        "thaw_request_ticket": ticket,
        "thaw_approve_ts_ms": 0,
        "thaw_approved_by": "",
        "manual_commit_request_id": "",
        "manual_commit_by": "",
        "manual_commit_ts_ms": 0,
        "manual_commit_sig": "",
        # P10: link control projection to request-log event
        "thaw_prepare_request_event_id": (request_event_id or ""),
        "thaw_approve_request_event_id": "",
        "thaw_commit_request_event_id": "",
        "last_operator_action": "manual_ack_thaw_prepare",
    }


def build_thaw_approve_update(
    *,
    prev: Mapping[str, Any] | None,
    now_ms: int,
    request_id: str,
    approver: str,
    request_event_id: str = "",
) -> dict[str, Any]:
    """Build the control hash payload for P9 approve-thaw phase.

    Keeps effective_freeze_active=1 until commit-thaw completes.
    Only updates approval fields; all other fields are carried from prepare phase.
    """
    p = dict(prev or {})
    return {
        **p,
        "schema_name": "exec_health_freeze_control",
        "schema_version": 4,
        "updated_ts_ms": now_ms,
        "control_source": "manual_thaw_approve",
        "effective_freeze_active": 1,
        "manual_ack_required": 1,
        "active_thaw_request_id": request_id,
        "thaw_request_status": "approved",
        "thaw_approve_ts_ms": now_ms,
        "thaw_approved_by": approver,
        # P10: link approval step to request-log event
        "thaw_approve_request_event_id": (request_event_id or ""),
        "last_operator_action": "manual_ack_thaw_approve",
    }


def build_dual_control_commit_thaw_update(
    *,
    prev: Mapping[str, Any] | None,
    now_ms: int,
    request_id: str,
    commit_by: str,
    commit_sig: str,
    commit_event_id: str,
    request_event_id: str = "",
) -> dict[str, Any]:
    """Build the control hash payload for P9 commit-thaw (final phase).

    Clears effective_freeze_active and writes all commit fields so
    verify_thaw_release_signature will pass on the updated hash.
    """
    p = dict(prev or {})
    return {
        **p,
        "schema_name": "exec_health_freeze_control",
        "schema_version": 4,
        "updated_ts_ms": now_ms,
        "effective_freeze_active": 0,
        "control_source": "manual_override_thaw",
        "freeze_until_ts_ms": 0,
        "source_ts_ms": now_ms,
        "manual_ack_required": 0,
        "manual_ack_ts_ms": now_ms,
        "manual_ack_operator": commit_by,
        "manual_ack_reason": _s(p.get("thaw_request_reason"), ""),
        "manual_ack_ticket": _s(p.get("thaw_request_ticket"), ""),
        "manual_ack_nonce": _s(p.get("thaw_request_nonce"), _s(p.get("expected_ack_nonce"), "")),
        "manual_ack_sig": commit_sig,
        "manual_ack_event_id": (commit_event_id or ""),
        "manual_override_active": 1,
        "manual_override_action": "thaw",
        "manual_override_until_ts_ms": 0,
        "active_thaw_request_id": request_id,
        "thaw_request_status": "committed",
        # P10: link commit to both event stream ID and request-log event ID
        "thaw_commit_request_event_id": (request_event_id or commit_event_id or ""),
        "manual_commit_request_id": request_id,
        "manual_commit_by": commit_by,
        "manual_commit_ts_ms": now_ms,
        "manual_commit_sig": commit_sig,
        "last_operator_action": "manual_ack_thaw_commit",
        "thaw_total": _i(p.get("thaw_total"), 0) + 1,
    }


def build_manual_freeze_update(
    *,
    prev: Mapping[str, Any] | None,
    now_ms: int,
    operator: str,
    reason: str,
    ticket: str,
    until_ts_ms: int,
) -> dict[str, Any]:
    """Build the control hash payload for an operator-initiated force-freeze.

    Does NOT set manual_ack_required because the operator is already on record.
    P8: carries forward existing nonce fields to preserve correlation context.
    P9: clears dual-control fields since a new thaw will need a new prepare-thaw.
    """
    p = dict(prev or {})
    return {
        "schema_name": "exec_health_freeze_control",
        "schema_version": 4,
        "updated_ts_ms": now_ms,
        "effective_freeze_active": 1,
        "control_source": "manual_override_freeze",
        "freeze_reason": reason,
        "freeze_until_ts_ms": until_ts_ms,
        "source_ts_ms": now_ms,
        # manual_ack_required=0: operator is already the source, no ack needed
        "manual_ack_required": 0,
        "manual_ack_ts_ms": _i(p.get("manual_ack_ts_ms"), 0),
        "manual_ack_operator": _s(p.get("manual_ack_operator"), operator),
        "manual_ack_reason": _s(p.get("manual_ack_reason"), reason),
        "manual_ack_ticket": _s(p.get("manual_ack_ticket"), ticket),
        # P8: carry forward signed ack fields
        "manual_ack_nonce": _s(p.get("manual_ack_nonce"), ""),
        "manual_ack_sig": _s(p.get("manual_ack_sig"), ""),
        "manual_ack_event_id": _s(p.get("manual_ack_event_id"), ""),
        # Override: freeze
        "manual_override_active": 1,
        "manual_override_action": "freeze",
        "manual_override_until_ts_ms": until_ts_ms,
        # P8: carry forward trigger nonce fields
        "expected_ack_nonce": _s(p.get("expected_ack_nonce"), ""),
        "last_trigger_nonce": _s(p.get("last_trigger_nonce"), _s(p.get("expected_ack_nonce"), "")),
        "last_trigger_ts_ms": _i(p.get("last_trigger_ts_ms"), 0),
        "last_trigger_event_id": _s(p.get("last_trigger_event_id"), ""),
        "last_operator_action": "manual_force_freeze",
        "last_manual_freeze_operator": operator,
        "last_manual_freeze_reason": reason,
        "last_manual_freeze_ticket": ticket,
        # Counters
        "manual_freeze_total": _i(p.get("manual_freeze_total"), 0) + 1,
        "trigger_total": _i(p.get("trigger_total"), 0),
        "thaw_total": _i(p.get("thaw_total"), 0),
        # P9: dual-control fields — cleared on manual freeze
        "active_thaw_request_id": "",
        "thaw_request_status": "",
        "thaw_request_nonce": _s(p.get("thaw_request_nonce"), ""),
        "thaw_prepare_ts_ms": 0,
        "thaw_prepared_by": "",
        "thaw_request_reason": "",
        "thaw_request_ticket": "",
        "thaw_approve_ts_ms": 0,
        "thaw_approved_by": "",
        "manual_commit_request_id": "",
        "manual_commit_by": "",
        "manual_commit_ts_ms": 0,
        "manual_commit_sig": "",
    }


def stringify_mapping(d: Mapping[str, Any]) -> dict[str, str]:
    """Convert all values to str for Redis hset (requires string values)."""
    return {k: str(v) for k, v in d.items()}
