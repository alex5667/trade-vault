from __future__ import annotations

"""Append-only request log + Lua CAS for ExecHealth freeze thaw workflow.

P10 moves prepare/approve/commit away from the mutable control hash into a
separate append-only Redis Stream. The control/state hashes become a
materialized projection, while the request log is the audit source of truth.

This module provides:
- request-log sequence evaluation
- small Lua CAS helpers for prepare/approve/commit projection writes
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from services.orderflow.exec_health_freeze_control import parse_exec_health_freeze_control

REQUEST_STREAM_ENV = "EXEC_HEALTH_FREEZE_REQUEST_STREAM"
DEFAULT_REQUEST_STREAM = "ops:exec_health:freeze_requests:v1"


REQUEST_LIBRARY_NAME = "exec_health_freeze_requestlog_v1"
FN_PREPARE_CAS = "exec_health_freeze_prepare_cas"
FN_APPROVE_CAS = "exec_health_freeze_approve_cas"
FN_COMMIT_CAS = "exec_health_freeze_commit_cas"

# P11: Redis Functions — FCALL-entrypoints для CAS операций.
# Это позволяет ACL заблокировать прямой EVAL/EVALSHA, но разрешить FCALL через whitelist.
REQUEST_FUNCTION_LIBRARY = r'''#!lua name=exec_health_freeze_requestlog_v1
redis.register_function('exec_health_freeze_prepare_cas', function(keys, args)
  local ctl=keys[1]
  local st=keys[2]
  local expected_nonce=args[1] or ''
  local request_id=args[2] or ''
  local n=tonumber(args[3] or '0') or 0
  local cur_expected=redis.call('HGET', ctl, 'expected_ack_nonce') or ''
  if cur_expected == '' then cur_expected = redis.call('HGET', st, 'expected_ack_nonce') or '' end
  if cur_expected ~= '' and expected_nonce ~= cur_expected then return 0 end
  local active_id=redis.call('HGET', ctl, 'active_thaw_request_id') or ''
  local active_status=redis.call('HGET', ctl, 'thaw_request_status') or ''
  if active_id ~= '' and active_id ~= request_id and (active_status == 'prepared' or active_status == 'approved') then return -2 end
  local idx = 4
  for i=1,n do
    local field=args[idx]
    local value=args[idx+1]
    idx = idx + 2
    redis.call('HSET', ctl, field, value)
    redis.call('HSET', st, field, value)
  end
  return 1
end)
redis.register_function('exec_health_freeze_approve_cas', function(keys, args)
  local ctl=keys[1]
  local st=keys[2]
  local request_id=args[1] or ''
  local approver=args[2] or ''
  local n=tonumber(args[3] or '0') or 0
  local active_id=redis.call('HGET', ctl, 'active_thaw_request_id') or ''
  local active_status=redis.call('HGET', ctl, 'thaw_request_status') or ''
  local prepared_by=redis.call('HGET', ctl, 'thaw_prepared_by') or ''
  if active_id ~= request_id then return 0 end
  if active_status ~= 'prepared' then return -2 end
  if prepared_by ~= '' and prepared_by == approver then return -3 end
  local idx = 4
  for i=1,n do
    local field=args[idx]
    local value=args[idx+1]
    idx = idx + 2
    redis.call('HSET', ctl, field, value)
    redis.call('HSET', st, field, value)
  end
  return 1
end)
redis.register_function('exec_health_freeze_commit_cas', function(keys, args)
  local ctl=keys[1]
  local st=keys[2]
  local request_id=args[1] or ''
  local operator=args[2] or ''
  local n=tonumber(args[3] or '0') or 0
  local active_id=redis.call('HGET', ctl, 'active_thaw_request_id') or ''
  local active_status=redis.call('HGET', ctl, 'thaw_request_status') or ''
  local approved_by=redis.call('HGET', ctl, 'thaw_approved_by') or ''
  if active_id ~= request_id then return 0 end
  if active_status ~= 'approved' then return -2 end
  if approved_by == '' or approved_by ~= operator then return -3 end
  local idx = 4
  for i=1,n do
    local field=args[idx]
    local value=args[idx+1]
    idx = idx + 2
    redis.call('HSET', ctl, field, value)
    redis.call('HSET', st, field, value)
  end
  return 1
end)
'''


def ensure_request_log_functions_loaded(redis_client: Any) -> bool:
    """Загружает Redis Function Library для CAS операций. Idempotent."""
    try:
        redis_client.execute_command('FUNCTION', 'LOAD', REQUEST_FUNCTION_LIBRARY)
        return True
    except Exception as exc:
        msg = str(exc).lower()
        return 'already exists' in msg or 'library name is already taken' in msg or 'busy' in msg


def _call_request_function(redis_client: Any, fn: str, control_key: str, state_key: str, argv: Sequence[Any]) -> int:
    """Вызывает CAS Function через FCALL (P11 путь). Returns -999 если FCALL недоступен."""
    if hasattr(redis_client, 'execute_command'):
        try:
            return int(redis_client.execute_command('FCALL', fn, 2, control_key, state_key, *list(argv)))
        except Exception as exc:
            msg = str(exc).lower()
            if ('unknown function' in msg or 'function not found' in msg) and ensure_request_log_functions_loaded(redis_client):
                return int(redis_client.execute_command('FCALL', fn, 2, control_key, state_key, *list(argv)))
    return -999

REQUEST_LOG_VIOLATION_KINDS = [
    "none",
    "request_log_prepare_missing",
    "request_log_approve_missing",
    "request_log_commit_missing",
    "request_log_same_operator_violation",
    "request_log_out_of_order",
    "request_log_nonce_mismatch",
    "control_request_mismatch",
]


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _event_payload(rec: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(rec, (list, tuple)) and len(rec) >= 2:
        return str(rec[0]), dict(rec[1] or {})
    return "", {}


def _xid_key(xid: str) -> tuple[int, int]:
    try:
        a, b = str(xid).split("-", 1)
        return int(a), int(b)
    except Exception:
        return 0, 0


def sort_events_asc(events: Sequence[Any]) -> list[tuple[str, dict[str, Any]]]:
    rows = [_event_payload(e) for e in list(events or [])]
    rows = [(eid, payload) for eid, payload in rows if eid]
    rows.sort(key=lambda x: _xid_key(x[0]))
    return rows


@dataclass(frozen=True)
class FreezeRequestLogResult:
    request_id: str
    request_nonce: str
    prepare_present: bool
    approve_present: bool
    commit_present: bool
    prepare_event_id: str
    approve_event_id: str
    commit_event_id: str
    prepared_by: str
    approved_by: str
    commit_by: str
    valid_sequence: bool
    same_operator_violation: bool
    control_request_mismatch: bool
    violation_kinds: list[str]


def find_request_events_for_request(events: Sequence[Any], request_id: str) -> list[tuple[str, dict[str, Any]]]:
    rid = (request_id or "")
    out: list[tuple[str, dict[str, Any]]] = []
    for eid, payload in sort_events_asc(events):
        if _s(payload.get("request_id")) == rid:
            out.append((eid, payload))
    return out


def evaluate_request_log_sequence(
    *,
    control_raw: Mapping[str, Any] | None,
    request_events: Sequence[Any] | None,
    request_id: str | None = None,
) -> FreezeRequestLogResult:
    """Evaluate the append-only request log sequence for a dual-control thaw request.

    P10: this is the source of truth for whether a thaw was properly authorised.
    A valid sequence requires: prepare → approve → commit, with distinct operators,
    monotonically increasing event IDs, and matching nonce.
    """
    control = parse_exec_health_freeze_control(control_raw or {})
    rid = str(request_id or control.active_thaw_request_id or control.manual_commit_request_id or "")
    expected_nonce = str(control.thaw_request_nonce or control.expected_ack_nonce or "")
    rows = find_request_events_for_request(request_events or [], rid) if rid else []

    prepare_eid = approve_eid = commit_eid = ""
    prepare_ev: dict[str, Any] = {}
    approve_ev: dict[str, Any] = {}
    commit_ev: dict[str, Any] = {}
    for eid, payload in rows:
        kind = _s(payload.get("kind"))
        if kind == "manual_ack_thaw_prepare":
            prepare_eid, prepare_ev = eid, payload
        elif kind == "manual_ack_thaw_approve":
            approve_eid, approve_ev = eid, payload
        elif kind == "manual_ack_thaw_commit":
            commit_eid, commit_ev = eid, payload

    prepare_present = bool(prepare_eid)
    approve_present = bool(approve_eid)
    commit_present = bool(commit_eid)
    prepared_by = _s(prepare_ev.get("operator")) or _s(control.raw_payload.get("thaw_prepared_by"))
    approved_by = _s(approve_ev.get("operator")) or _s(control.raw_payload.get("thaw_approved_by"))
    commit_by = _s(commit_ev.get("operator")) or _s(control.raw_payload.get("manual_commit_by"))
    request_nonce = _s(commit_ev.get("ack_nonce")) or _s(approve_ev.get("ack_nonce")) or _s(prepare_ev.get("ack_nonce")) or expected_nonce

    same_operator = bool(prepared_by and approved_by and prepared_by == approved_by)
    out_of_order = False
    if prepare_present and approve_present and _xid_key(approve_eid) <= _xid_key(prepare_eid):
        out_of_order = True
    if approve_present and commit_present and _xid_key(commit_eid) <= _xid_key(approve_eid):
        out_of_order = True

    control_request_mismatch = False
    if rid:
        if control.active_thaw_request_id and control.active_thaw_request_id != rid:
            control_request_mismatch = True
        if control.thaw_prepared_by and prepared_by and control.thaw_prepared_by != prepared_by:
            control_request_mismatch = True
        if control.thaw_approved_by and approved_by and control.thaw_approved_by != approved_by:
            control_request_mismatch = True
        if control.manual_commit_by and commit_by and control.manual_commit_by != commit_by:
            control_request_mismatch = True

    nonce_mismatch = bool(expected_nonce and request_nonce and expected_nonce != request_nonce)
    valid_sequence = bool(
        rid
        and prepare_present
        and approve_present
        and commit_present
        and not same_operator
        and not out_of_order
        and not nonce_mismatch
        and approved_by
        and commit_by == approved_by
    )

    violations: list[str] = []
    if rid:
        if not prepare_present:
            violations.append("request_log_prepare_missing")
        if not approve_present:
            violations.append("request_log_approve_missing")
        if not commit_present:
            violations.append("request_log_commit_missing")
    if same_operator:
        violations.append("request_log_same_operator_violation")
    if out_of_order:
        violations.append("request_log_out_of_order")
    if nonce_mismatch:
        violations.append("request_log_nonce_mismatch")
    if control_request_mismatch:
        violations.append("control_request_mismatch")
    if not violations:
        violations = ["none"]

    return FreezeRequestLogResult(
        request_id=rid,
        request_nonce=request_nonce,
        prepare_present=prepare_present,
        approve_present=approve_present,
        commit_present=commit_present,
        prepare_event_id=prepare_eid,
        approve_event_id=approve_eid,
        commit_event_id=commit_eid,
        prepared_by=prepared_by,
        approved_by=approved_by,
        commit_by=commit_by,
        valid_sequence=valid_sequence,
        same_operator_violation=same_operator,
        control_request_mismatch=control_request_mismatch,
        violation_kinds=violations,
    )


def mapping_to_argv(mapping: Mapping[str, Any]) -> list[str]:
    flat: list[str] = []
    for k, v in dict(mapping).items():
        flat.extend([str(k), str(v)])
    return flat


# ─── Lua CAS scripts ──────────────────────────────────────────────────────────
# P10: atomic compare-and-set for prepare/approve/commit projection writes.
# Each script returns:
#   1  → success
#   0  → nonce/request_id mismatch (stale update rejected)
#  -2  → wrong state (wrong status for this step)
#  -3  → same-operator dual-control violation (approve/commit)

PREPARE_CAS_LUA = r'''
local ctl=KEYS[1]
local st=KEYS[2]
local expected_nonce=ARGV[1]
local request_id=ARGV[2]
local n=tonumber(ARGV[3]) or 0
local cur_expected=redis.call('HGET', ctl, 'expected_ack_nonce') or ''
if cur_expected == '' then
  cur_expected = redis.call('HGET', st, 'expected_ack_nonce') or ''
end
if cur_expected ~= '' and expected_nonce ~= cur_expected then
  return 0
end
local active_id=redis.call('HGET', ctl, 'active_thaw_request_id') or ''
local active_status=redis.call('HGET', ctl, 'thaw_request_status') or ''
if active_id ~= '' and active_id ~= request_id and (active_status == 'prepared' or active_status == 'approved') then
  return -2
end
for i=0,n-1 do
  local field=ARGV[4 + i*2]
  local value=ARGV[5 + i*2]
  redis.call('HSET', ctl, field, value)
  redis.call('HSET', st, field, value)
end
return 1
'''

APPROVE_CAS_LUA = r'''
local ctl=KEYS[1]
local st=KEYS[2]
local request_id=ARGV[1]
local approver=ARGV[2]
local n=tonumber(ARGV[3]) or 0
local active_id=redis.call('HGET', ctl, 'active_thaw_request_id') or ''
local active_status=redis.call('HGET', ctl, 'thaw_request_status') or ''
local prepared_by=redis.call('HGET', ctl, 'thaw_prepared_by') or ''
if active_id ~= request_id then return 0 end
if active_status ~= 'prepared' then return -2 end
if prepared_by ~= '' and prepared_by == approver then return -3 end
for i=0,n-1 do
  local field=ARGV[4 + i*2]
  local value=ARGV[5 + i*2]
  redis.call('HSET', ctl, field, value)
  redis.call('HSET', st, field, value)
end
return 1
'''

COMMIT_CAS_LUA = r'''
local ctl=KEYS[1]
local st=KEYS[2]
local request_id=ARGV[1]
local operator=ARGV[2]
local n=tonumber(ARGV[3]) or 0
local active_id=redis.call('HGET', ctl, 'active_thaw_request_id') or ''
local active_status=redis.call('HGET', ctl, 'thaw_request_status') or ''
local approved_by=redis.call('HGET', ctl, 'thaw_approved_by') or ''
if active_id ~= request_id then return 0 end
if active_status ~= 'approved' then return -2 end
if approved_by == '' or approved_by ~= operator then return -3 end
for i=0,n-1 do
  local field=ARGV[4 + i*2]
  local value=ARGV[5 + i*2]
  redis.call('HSET', ctl, field, value)
  redis.call('HSET', st, field, value)
end
return 1
'''


def _eval_status(value: Any) -> int:
    if isinstance(value, (list, tuple)) and value:
        try:
            return int(value[0])
        except Exception:
            pass
    try:
        return int(value)
    except Exception:
        return -999


def eval_prepare_cas(redis_client: Any, *, control_key: str, state_key: str, expected_nonce: str, request_id: str, mapping: Mapping[str, Any]) -> int:
    """Atomically write prepare-thaw projection if nonce + request_id match. Returns 1 on success.

    P11: пробуем FCALL первым (ACL-whitelist путь), fallback на EVAL для совместимости.
    """
    argv = [str(expected_nonce), str(request_id), str(len(dict(mapping)))] + mapping_to_argv(mapping)
    rc = _call_request_function(redis_client, FN_PREPARE_CAS, control_key, state_key, argv)
    if rc != -999:
        return _eval_status(rc)
    return _eval_status(redis_client.eval(PREPARE_CAS_LUA, 2, control_key, state_key, *argv))


def eval_approve_cas(redis_client: Any, *, control_key: str, state_key: str, request_id: str, approver: str, mapping: Mapping[str, Any]) -> int:
    """Atomically write approve-thaw projection if request_id matches and not same operator. Returns 1 on success.

    P11: пробуем FCALL первым (ACL-whitelist путь), fallback на EVAL для совместимости.
    """
    argv = [str(request_id), str(approver), str(len(dict(mapping)))] + mapping_to_argv(mapping)
    rc = _call_request_function(redis_client, FN_APPROVE_CAS, control_key, state_key, argv)
    if rc != -999:
        return _eval_status(rc)
    return _eval_status(redis_client.eval(APPROVE_CAS_LUA, 2, control_key, state_key, *argv))


def eval_commit_cas(redis_client: Any, *, control_key: str, state_key: str, request_id: str, operator: str, mapping: Mapping[str, Any]) -> int:
    """Atomically write commit-thaw projection if request_id matches and operator == approved_by. Returns 1 on success.

    P11: пробуем FCALL первым (ACL-whitelist путь), fallback на EVAL для совместимости.
    """
    argv = [str(request_id), str(operator), str(len(dict(mapping)))] + mapping_to_argv(mapping)
    rc = _call_request_function(redis_client, FN_COMMIT_CAS, control_key, state_key, argv)
    if rc != -999:
        return _eval_status(rc)
    return _eval_status(redis_client.eval(COMMIT_CAS_LUA, 2, control_key, state_key, *argv))
