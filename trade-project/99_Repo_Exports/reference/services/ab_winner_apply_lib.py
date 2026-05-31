from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any


def _now_ms() -> int:
    return int(time.time() * 1000)


def norm_sym(s: str) -> str:
    return (s or "").strip().upper()


def norm_rg(s: str) -> str:
    return (s or "na").strip().lower()


def norm_grp(s: str) -> str:
    return (s or "default").strip().lower()


def norm_arm(a: str) -> str:
    a = (a or "").strip().upper()
    return a if a in ("A", "B", "C") else ""


def key_latest(prefix: str, *, symbol: str, regime: str, group: str) -> str:
    return f"{prefix}:{norm_sym(symbol)}:{norm_rg(regime)}:{norm_grp(group)}"


def key_meta(prefix: str, sid: str) -> str:
    return f"{prefix}:{sid}"


def key_approvals(prefix: str, sid: str) -> str:
    return f"{prefix}:{sid}"


def key_applied(prefix: str, sid: str) -> str:
    return f"{prefix}:{sid}"


def key_active_arm(*, symbol: str, regime: str, group: str) -> str:
    return f"cfg:entry_policy:active_arm:{norm_sym(symbol)}:{norm_rg(regime)}:{norm_grp(group)}"


def key_lock(*, symbol: str, regime: str, group: str) -> str:
    return f"cfg:entry_policy:active_arm_lock:{norm_sym(symbol)}:{norm_rg(regime)}:{norm_grp(group)}"


# Atomic apply script:
# KEYS:
#  1 approvals_key
#  2 applied_key
#  3 active_arm_key
#  4 lock_key
#  5 override_unlock_key
# ARGV:
#  1 approvals_required
#  2 winner_arm
#  3 applied_payload_json
#  4 lock_sec
#  5 active_ttl_sec (0 => persistent)
#  6 applied_ttl_sec
#
# Lock value: JSON payload (sid/winner/ts/by) for traceability.
LUA_APPLY = r"""
local approvals_key = KEYS[1]
local applied_key   = KEYS[2]
local active_key    = KEYS[3]
local lock_key      = KEYS[4]
local override_key  = KEYS[5]

local need          = tonumber(ARGV[1])
local winner        = tostring(ARGV[2])
local applied_val   = tostring(ARGV[3])
local lock_sec      = tonumber(ARGV[4])
local active_ttl    = tonumber(ARGV[5])
local applied_ttl   = tonumber(ARGV[6])

-- Lock gate: allow override if override_key exists
if redis.call('EXISTS', lock_key) == 1 then
  if override_key and redis.call('EXISTS', override_key) == 1 then
    -- allowed to override lock
  else
    return {0, 'locked'}
  end
end

local n = redis.call('SCARD', approvals_key)
if n < need then
  return {0, 'not_enough', n}
end

local ok = redis.call('SET', applied_key, applied_val, 'NX', 'EX', applied_ttl)
if not ok then
  return {0, 'already_applied'}
end

if active_ttl and active_ttl > 0 then
  redis.call('SET', active_key, winner, 'EX', active_ttl)
else
  redis.call('SET', active_key, winner)
end

-- Store informative lock payload (same applied payload is ok)
redis.call('SET', lock_key, applied_val, 'EX', lock_sec)
return {1, 'applied', n}
"""


@dataclass
class ApplyResult:
    applied: bool
    skipped: bool
    reason: str
    sid: str
    symbol: str
    regime: str
    group: str
    winner: str
    approvals_n: int = 0


async def apply_sid_if_ready(
    *,
    r: Any,
    sid: str,
    meta_prefix: str,
    approvals_prefix: str,
    applied_prefix: str,
    approvals_required: int,
    lock_sec: int,
    active_ttl_sec: int,
    applied_ttl_sec: int,
    audit_stream: str,
    by: str = "apply_runner",
) -> ApplyResult:
    """
    Loads meta:{sid} -> derives keys -> calls atomic Lua apply.
    Audit is best-effort after success.
    """
    sid = (sid or "").strip()
    if not sid:
        return ApplyResult(False, True, "no_sid", "", "", "", "", "")

    meta_key = key_meta(meta_prefix, sid)
    raw = await r.get(meta_key)
    if not raw:
        return ApplyResult(False, True, "no_meta", sid, "", "", "", "")

    try:
        meta = json.loads(raw)
    except Exception:
        return ApplyResult(False, True, "bad_meta_json", sid, "", "", "", "")

    sym = norm_sym(str(meta.get("symbol") or ""))
    rg = norm_rg(str(meta.get("regime") or "na"))
    grp = norm_grp(str(meta.get("group") or "default"))
    winner = norm_arm(str(meta.get("winner_arm") or ""))
    if not sym or not winner:
        return ApplyResult(False, True, "meta_missing_fields", sid, sym, rg, grp, winner)

    approvals_key = key_approvals(approvals_prefix, sid)
    applied_key = key_applied(applied_prefix, sid)
    active_key = key_active_arm(symbol=sym, regime=rg, group=grp)
    lock_key = key_lock(symbol=sym, regime=rg, group=grp)
    override_key = f"cfg:entry_policy:active_arm_override_unlock:{sym}:{rg}:{grp}"

    applied_payload = {
        "sid": sid,
        "ts_ms": _now_ms(),
        "by": by,
        "symbol": sym,
        "regime": rg,
        "group": grp,
        "winner_arm": winner,
        "meta_key": meta_key,
        "active_arm_key": active_key,
        "lock_key": lock_key,
        "note": "applied_by_lua",
    }

    try:
        res = await r.eval(
            LUA_APPLY,
            5,
            approvals_key,
            applied_key,
            active_key,
            lock_key,
            override_key,
            str(int(approvals_required)),
            str(winner),
            json.dumps(applied_payload, ensure_ascii=False, separators=(",", ":")),
            str(int(lock_sec)),
            str(int(active_ttl_sec)),
            str(int(applied_ttl_sec)),
        )
    except Exception as e:
        return ApplyResult(False, False, f"eval_failed:{e}", sid, sym, rg, grp, winner)

    # res is array-like: [ok_flag, reason, n?]
    try:
        ok_flag = int(res[0] or 0)
        reason = str(res[1] or "")
        n_appr = int(res[2] or 0) if len(res) >= 3 else 0
    except Exception:
        return ApplyResult(False, False, "bad_eval_result", sid, sym, rg, grp, winner)

    if ok_flag != 1:
        return ApplyResult(False, True, reason or "skipped", sid, sym, rg, grp, winner, n_appr)

    # best-effort audit
    try:
        msg = {
            "type": "cfg_apply_active_arm",
            "ts_ms": str(_now_ms()),
            "symbol": sym,
            "regime": rg,
            "group": grp,
            "arm": winner,
            "sid": sid,
            "payload": json.dumps({"sid": sid, "approvals_n": n_appr, "key": active_key}, separators=(",", ":")),
        }
        await r.xadd(audit_stream, msg, maxlen=50000, approximate=True)
    except Exception:
        pass

    return ApplyResult(True, False, "applied", sid, sym, rg, grp, winner, n_appr)
