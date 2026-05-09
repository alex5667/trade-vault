#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Operator CLI for ExecHealth dual-control override / thaw workflow (P9).

Usage
-----
# Show current freeze state (includes pending_ack_nonce and active thaw request)
python orderflow_services/exec_health_freeze_override_v1.py status

# P9: three-phase thaw (dual-control)
python orderflow_services/exec_health_freeze_override_v1.py prepare-thaw \\
  --operator alice \\
  --reason "validated rollback, mismatch resolved" \\
  --ticket INC-42 \\
  --nonce <pending_ack_nonce>

python orderflow_services/exec_health_freeze_override_v1.py approve-thaw \\
  --operator bob \\
  --request-id <request_id>

python orderflow_services/exec_health_freeze_override_v1.py commit-thaw \\
  --operator bob \\
  --request-id <request_id>

# Operator force-freeze for a maintenance window
python orderflow_services/exec_health_freeze_override_v1.py freeze \\
  --operator alice \\
  --reason "maintenance window" \\
  --ticket CHG-17 \\
  --minutes 30

P9 Contract
-----------
Thaw requires a two-phase, dual-operator workflow:
1. prepare-thaw: operator A creates a thaw request bound to the current nonce
2. approve-thaw: operator B (B != A) approves the request
3. commit-thaw:  operator B emits a final signed dual-control commit event

The downstream hook (exec_health_freeze_hook.py) validates the HMAC dual-control
commit signature before accepting the thaw. An unsigned or same-operator commit is ignored.
"""

import argparse
import json
import os
import secrets
import sys
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from services.orderflow.exec_health_freeze_control import (
    ACK_SIGNING_SECRET_ENV,
    build_dual_control_commit_thaw_update,
    build_manual_freeze_update,
    build_thaw_approve_update,
    build_thaw_prepare_update,
    parse_exec_health_freeze_control,
    sign_dual_control_commit,
    stringify_mapping,
)
from services.orderflow.exec_health_freeze_reconnect_healing import heal_service_identity_sync
from services.orderflow.exec_health_freeze_service_identity import ensure_service_identity_sync


def _now_ms() -> int:
    return get_ny_time_millis()


def _jprint(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


class OverrideController:
    """Synchronous Redis controller for operator freeze/thaw actions (P9 dual-control).

    Every write is recorded in:
    - cfg:orderflow:exec_health:freeze_control:v1  (latched control hash)
    - metrics:exec_health:slo:autoguard:state       (state hash fallback)
    - ops:exec_health:freeze_events:v1              (event stream)
    """

    def __init__(self, redis_url: str | None = None):
        if redis is None:
            raise RuntimeError("redis dependency missing")
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.control_key = os.getenv("EXEC_HEALTH_FREEZE_CONTROL_KEY", "cfg:orderflow:exec_health:freeze_control:v1")
        self.state_key = os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY", "metrics:exec_health:slo:autoguard:state")
        self.freeze_key = os.getenv("EXEC_HEALTH_AUTO_FREEZE_KEY", "cfg:orderflow:exec_health:auto_freeze:v1")
        self.event_stream = os.getenv("EXEC_HEALTH_FREEZE_EVENT_STREAM", "ops:exec_health:freeze_events:v1")
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        ensure_service_identity_sync(self.r, "exec_health_freeze_override_v1")
        heal_service_identity_sync(self.r, "exec_health_freeze_override_v1", force=True)

    def _read_hash(self, key: str) -> dict[str, Any]:
        try:
            return self.r.hgetall(key) or {}
        except Exception:
            return {}

    def _write_hash(self, key: str, payload: dict[str, Any]) -> None:
        """Write hash and set 30-day TTL (operator actions are long-lived audit records)."""
        self.r.hset(key, mapping=stringify_mapping(payload))
        self.r.expire(key, 86400 * 30)

    def _emit_event(self, payload: dict[str, Any]) -> str:
        """Best-effort event emit to the freeze event stream. Returns event ID."""
        try:
            return str(self.r.xadd(self.event_stream, stringify_mapping(payload), maxlen=5000) or "")
        except Exception:
            return ""

    def status(self) -> dict[str, Any]:
        """Return current state from all three sources for operator inspection."""
        heal_service_identity_sync(self.r, "exec_health_freeze_override_v1")
        ctl = parse_exec_health_freeze_control(self._read_hash(self.control_key))
        st = parse_exec_health_freeze_control(self._read_hash(self.state_key))
        try:
            raw = self.r.get(self.freeze_key)
        except Exception:
            raw = None
        return {
            "control_key": self.control_key,
            "state_key": self.state_key,
            "freeze_key": self.freeze_key,
            # P8: expose pending nonce so operator knows what to pass to --nonce
            "pending_ack_nonce": ctl.expected_ack_nonce or st.expected_ack_nonce,
            # P9: expose active thaw request info
            "active_thaw_request_id": ctl.active_thaw_request_id or st.active_thaw_request_id,
            "thaw_request_status": ctl.thaw_request_status or st.thaw_request_status,
            "last_trigger_ts_ms": int(ctl.raw_payload.get("last_trigger_ts_ms") or st.raw_payload.get("last_trigger_ts_ms") or 0),
            "control": dict(getattr(ctl, "raw_payload", {}) or {}),
            "state_fallback": dict(getattr(st, "raw_payload", {}) or {}),
            "raw_freeze": json.loads(raw) if raw else {},
        }

    def _load_pair(self):
        """Load control and state hashes plus parsed states."""
        prev_ctl = self._read_hash(self.control_key)
        prev_state = self._read_hash(self.state_key)
        ctl = parse_exec_health_freeze_control(prev_ctl)
        st = parse_exec_health_freeze_control(prev_state)
        return prev_ctl, prev_state, ctl, st

    def prepare_thaw(self, *, operator: str, reason: str, ticket: str, nonce: str) -> dict[str, Any]:
        """P9 Phase 1: Prepare a thaw request (operator A).

        Creates a new request_id, validates the nonce CAS, stores the pending
        prepare state, and emits a manual_ack_thaw_prepare event.
        """
        heal_service_identity_sync(self.r, "exec_health_freeze_override_v1")
        now = _now_ms()
        if not operator.strip() or not reason.strip() or not nonce.strip():
            raise ValueError("operator, reason and nonce are required")
        prev_ctl, prev_state, ctl, st = self._load_pair()
        expected_nonce = ctl.expected_ack_nonce or st.expected_ack_nonce
        trigger_ts_ms = int(prev_ctl.get("last_trigger_ts_ms") or prev_state.get("last_trigger_ts_ms") or 0)
        if not expected_nonce:
            raise ValueError("no pending ack nonce found in control/state")
        if str(nonce) != str(expected_nonce):
            raise ValueError("ack nonce mismatch (CAS failed)")
        if ctl.active_thaw_request_id and ctl.thaw_request_status in {"prepared", "approved"}:
            raise ValueError(f"active thaw request already exists: {ctl.active_thaw_request_id}")
        request_id = f"thr-{now}-{secrets.token_hex(4)}"
        event_payload = {
            "ts_ms": now,
            "kind": "manual_ack_thaw_prepare",
            "request_id": request_id,
            "operator": operator,
            "reason": reason,
            "ticket": ticket,
            "ack_nonce": str(nonce),
            "trigger_ts_ms": trigger_ts_ms,
            "source": "exec_health_freeze_override_v1",
            "control_key": self.control_key,
        }
        event_id = self._emit_event(event_payload)
        upd = build_thaw_prepare_update(
            prev=prev_ctl,
            now_ms=now,
            request_id=request_id,
            operator=operator,
            reason=reason,
            ticket=ticket,
            provided_ack_nonce=str(nonce),
        )
        upd["thaw_prepare_event_id"] = (event_id or "")
        self._write_hash(self.control_key, upd)
        state_upd = dict(prev_state)
        state_upd.update(upd)
        state_upd["state_thaw_prepare_written_ts_ms"] = int(now)
        self._write_hash(self.state_key, state_upd)
        return {"ok": True, "action": "prepare-thaw", **event_payload, "event_id": event_id, "effective_freeze_active": 1}

    def approve_thaw(self, *, operator: str, request_id: str) -> dict[str, Any]:
        """P9 Phase 2: Approve a thaw request (operator B, must differ from operator A).

        Validates that the request is in 'prepared' state and that the approving
        operator is different from the preparer.
        """
        heal_service_identity_sync(self.r, "exec_health_freeze_override_v1")
        now = _now_ms()
        if not operator.strip() or not request_id.strip():
            raise ValueError("operator and request_id are required")
        prev_ctl, prev_state, ctl, st = self._load_pair()
        rid = ctl.active_thaw_request_id or st.active_thaw_request_id
        if str(request_id) != str(rid):
            raise ValueError("request_id mismatch")
        if ctl.thaw_request_status != 'prepared':
            raise ValueError(f"request is not in prepared state: {ctl.thaw_request_status}")
        if operator == ctl.thaw_prepared_by:
            raise ValueError("second approver must be different from preparer")
        event_payload = {
            "ts_ms": now,
            "kind": "manual_ack_thaw_approve",
            "request_id": str(request_id),
            "operator": operator,
            "prepared_by": ctl.thaw_prepared_by,
            "ack_nonce": ctl.thaw_request_nonce or ctl.expected_ack_nonce,
            "trigger_ts_ms": int(prev_ctl.get("last_trigger_ts_ms") or prev_state.get("last_trigger_ts_ms") or 0),
            "source": "exec_health_freeze_override_v1",
            "control_key": self.control_key,
        }
        event_id = self._emit_event(event_payload)
        upd = build_thaw_approve_update(prev=prev_ctl, now_ms=now, request_id=str(request_id), approver=operator)
        upd["thaw_approve_event_id"] = (event_id or "")
        self._write_hash(self.control_key, upd)
        state_upd = dict(prev_state)
        state_upd.update(upd)
        state_upd["state_thaw_approve_written_ts_ms"] = int(now)
        self._write_hash(self.state_key, state_upd)
        return {"ok": True, "action": "approve-thaw", **event_payload, "event_id": event_id, "effective_freeze_active": 1}

    def commit_thaw(self, *, operator: str, request_id: str) -> dict[str, Any]:
        """P9 Phase 3: Commit (execute) the approved thaw (operator B).

        Validates request is 'approved', approver != preparer, then signs and writes
        the dual-control commit event which clears the freeze.
        """
        heal_service_identity_sync(self.r, "exec_health_freeze_override_v1")
        now = _now_ms()
        if not operator.strip() or not request_id.strip():
            raise ValueError("operator and request_id are required")
        secret = os.getenv(ACK_SIGNING_SECRET_ENV, "")
        if not secret:
            raise ValueError(f"{ACK_SIGNING_SECRET_ENV} is required for signed commit thaw")
        prev_ctl, prev_state, ctl, st = self._load_pair()
        rid = ctl.active_thaw_request_id or st.active_thaw_request_id
        if str(request_id) != str(rid):
            raise ValueError("request_id mismatch")
        if ctl.thaw_request_status != 'approved':
            raise ValueError(f"request is not approved: {ctl.thaw_request_status}")
        if not ctl.thaw_approved_by or ctl.thaw_prepared_by == ctl.thaw_approved_by:
            raise ValueError("dual-control approval is invalid or missing")
        if operator != ctl.thaw_approved_by:
            raise ValueError("commit must be executed by the approved second operator")
        trigger_ts_ms = int(prev_ctl.get("last_trigger_ts_ms") or prev_state.get("last_trigger_ts_ms") or 0)
        sig = sign_dual_control_commit(
            secret=secret,
            request_id=str(request_id),
            ack_nonce=ctl.thaw_request_nonce or ctl.expected_ack_nonce,
            prepared_by=ctl.thaw_prepared_by,
            approved_by=ctl.thaw_approved_by,
            commit_by=operator,
            reason=ctl.thaw_request_reason,
            ticket=ctl.thaw_request_ticket,
            trigger_ts_ms=trigger_ts_ms,
            prepared_ts_ms=ctl.thaw_prepare_ts_ms,
            approved_ts_ms=ctl.thaw_approve_ts_ms,
            commit_ts_ms=now,
        )
        event_payload = {
            "ts_ms": now,
            "kind": "manual_ack_thaw_commit",
            "request_id": str(request_id),
            "operator": operator,
            "prepared_by": ctl.thaw_prepared_by,
            "approved_by": ctl.thaw_approved_by,
            "reason": ctl.thaw_request_reason,
            "ticket": ctl.thaw_request_ticket,
            "ack_nonce": ctl.thaw_request_nonce or ctl.expected_ack_nonce,
            "trigger_ts_ms": int(trigger_ts_ms),
            "prepared_ts_ms": int(ctl.thaw_prepare_ts_ms),
            "approved_ts_ms": int(ctl.thaw_approve_ts_ms),
            "commit_sig": sig,
            "source": "exec_health_freeze_override_v1",
            "control_key": self.control_key,
        }
        event_id = self._emit_event(event_payload)
        upd = build_dual_control_commit_thaw_update(
            prev=prev_ctl,
            now_ms=now,
            request_id=str(request_id),
            commit_by=operator,
            commit_sig=sig,
            commit_event_id=event_id,
        )
        self._write_hash(self.control_key, upd)
        state_upd = dict(prev_state)
        state_upd.update(upd)
        state_upd["state_thaw_commit_written_ts_ms"] = int(now)
        self._write_hash(self.state_key, state_upd)
        return {"ok": True, "action": "commit-thaw", **event_payload, "event_id": event_id, "effective_freeze_active": 0}

    def freeze(self, *, operator: str, reason: str, ticket: str, minutes: int) -> dict[str, Any]:
        """Operator force-freeze for maintenance windows or manual intervention.

        Writes to control hash, state hash, and raw freeze key (legacy compat).
        """
        now = _now_ms()
        if not operator.strip() or not reason.strip():
            raise ValueError("operator and reason are required")
        mins = max(1, int(minutes))
        until = now + mins * 60 * 1000

        prev_ctl = self._read_hash(self.control_key)
        upd = build_manual_freeze_update(
            prev=prev_ctl, now_ms=now, operator=operator, reason=reason, ticket=ticket, until_ts_ms=until
        )
        self._write_hash(self.control_key, upd)

        # Mirror into state hash so fallback path also sees manual freeze
        prev_state = self._read_hash(self.state_key)
        state_upd = dict(prev_state)
        state_upd.update(upd)
        state_upd["state_manual_freeze_written_ts_ms"] = int(now)
        self._write_hash(self.state_key, state_upd)

        # Write legacy raw TTL key for P5/P6 backward-compat
        raw = {
            "schema_name": "exec_health_auto_freeze",
            "schema_version": 1,
            "ts_ms": now,
            "freeze_active": 1,
            "freeze_reason": f"manual_override:{reason}",
            "freeze_until_ts_ms": until,
        }
        self.r.set(self.freeze_key, json.dumps(raw, separators=(",", ":")))
        self.r.pexpire(self.freeze_key, mins * 60 * 1000)

        ev = {
            "ts_ms": now,
            "kind": "manual_override_freeze",
            "operator": operator,
            "reason": reason,
            "ticket": ticket,
            "minutes": mins,
            "freeze_until_ts_ms": until,
            "control_key": self.control_key,
        }
        self._emit_event(ev)
        return {"ok": True, "action": "freeze", **ev, "effective_freeze_active": 1}


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="ExecHealth dual-control operator override / thaw workflow")
    ap.add_argument("command", choices=["status", "prepare-thaw", "approve-thaw", "commit-thaw", "freeze"])
    ap.add_argument("--operator", default=os.getenv("OPERATOR", ""))
    ap.add_argument("--reason", default=os.getenv("REASON", ""))
    ap.add_argument("--ticket", default=os.getenv("TICKET", ""))
    ap.add_argument("--nonce", default=os.getenv("ACK_NONCE", ""), help="P8/P9: pending ack nonce (from status command)")
    ap.add_argument("--request-id", default=os.getenv("REQUEST_ID", ""), help="P9: thaw request_id (from prepare-thaw output)")
    ap.add_argument(
        "--minutes",
        type=int,
        default=int(os.getenv("EXEC_HEALTH_MANUAL_FREEZE_MINUTES", "30") or 30),
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    ctl = OverrideController()
    if args.command == "status":
        _jprint(ctl.status())
        return 0
    if args.command == "prepare-thaw":
        _jprint(ctl.prepare_thaw(operator=args.operator, reason=args.reason, ticket=args.ticket, nonce=args.nonce))
        return 0
    if args.command == "approve-thaw":
        _jprint(ctl.approve_thaw(operator=args.operator, request_id=args.request_id))
        return 0
    if args.command == "commit-thaw":
        _jprint(ctl.commit_thaw(operator=args.operator, request_id=args.request_id))
        return 0
    if args.command == "freeze":
        _jprint(ctl.freeze(operator=args.operator, reason=args.reason, ticket=args.ticket, minutes=args.minutes))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
