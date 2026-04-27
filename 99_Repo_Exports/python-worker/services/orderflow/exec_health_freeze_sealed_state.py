from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Redis-side sealed-state helpers for ExecHealth freeze control.

P11 hardens mutable freeze-control/state hashes in two ways:
1. services no longer write the projection with direct HSET/HDEL/DEL paths
2. trusted writers go through a whitelist Redis Function (FCALL) that replaces the
   whole hash and updates a deterministic seal/version chain

The seal is not meant to be a public cryptographic signature. It is an
operator/service-side tamper-evidence digest over the materialized projection.
Unauthorized direct hash edits should be blocked by Redis ACL. Authorized but
non-entrypoint edits will fail seal verification and be escalated by integrity
checks / tamper-guard.
"""

import hashlib
import os
import time
from typing import Any, Dict, Iterable, Mapping, Sequence

SEAL_SECRET_ENV = "EXEC_HEALTH_FREEZE_SEAL_SECRET"
SEAL_ENFORCE_ENV = "EXEC_HEALTH_FREEZE_SEAL_ENFORCE"
SEAL_BOOTSTRAP_ENV = "EXEC_HEALTH_FREEZE_SEAL_ALLOW_UNSEALED_BOOTSTRAP"
LIBRARY_NAME = "exec_health_freeze_sealed_v1"
FN_SET = "exec_health_freeze_sealed_set"
FN_FORCE_SET = "exec_health_freeze_sealed_force_set"

_SEAL_META_FIELDS = {
    "seal_digest",
    "seal_algorithm",
    "seal_version",
    "seal_prev_digest",
    "seal_entrypoint",
    "sealed_at_ts_ms",
    "seal_force_reason",
}

# Redis Functions are available since Redis 7.0, and loaded libraries are then
# callable via FCALL. ACL rules can separately constrain key patterns and the
# allowed command surface, while ACL LOG reports denied attempts. See the
# official Redis docs for FUNCTION LOAD / FCALL / ACL rules / ACL LOG.
REDIS_FUNCTION_LIBRARY = r'''
#!lua name=exec_health_freeze_sealed_v1
local function _sealed_write(keys, args, force)
  local key = keys[1]
  local expected_prev_seal = args[1] or ''
  local expected_prev_version = tonumber(args[2] or '0') or 0
  local ttl_s = tonumber(args[3] or '0') or 0
  local n = tonumber(args[4] or '0') or 0
  local cur_seal = redis.call('HGET', key, 'seal_digest') or ''
  local cur_ver = tonumber(redis.call('HGET', key, 'seal_version') or '0') or 0
  if not force then
    if cur_seal ~= expected_prev_seal then
      return 0
    end
    if cur_ver ~= expected_prev_version then
      return -2
    end
  end
  redis.call('DEL', key)
  local idx = 5
  for i=1,n do
    local field = args[idx]
    local value = args[idx+1]
    idx = idx + 2
    redis.call('HSET', key, field, value)
  end
  if ttl_s > 0 then
    redis.call('EXPIRE', key, ttl_s)
  end
  return 1
end

redis.register_function('exec_health_freeze_sealed_set', function(keys, args)
  return _sealed_write(keys, args, false)
end)

redis.register_function('exec_health_freeze_sealed_force_set', function(keys, args)
  return _sealed_write(keys, args, true)
end)
'''


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


def _secret(explicit: str | None = None) -> str:
    return str(explicit if explicit is not None else os.getenv(SEAL_SECRET_ENV, "") or "")


def seal_enforced() -> bool:
    raw = str(os.getenv(SEAL_ENFORCE_ENV, "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def allow_unsealed_bootstrap() -> bool:
    raw = str(os.getenv(SEAL_BOOTSTRAP_ENV, "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _canonical_items(mapping: Mapping[str, Any]) -> Sequence[tuple[str, str]]:
    pairs = []
    for k, v in dict(mapping or {}).items():
        if str(k) in _SEAL_META_FIELDS:
            continue
        pairs.append((str(k), _s(v)))
    pairs.sort(key=lambda kv: kv[0])
    return pairs


def build_seal_payload(mapping: Mapping[str, Any]) -> str:
    return "|".join(f"{k}={v}" for k, v in _canonical_items(mapping))


def compute_seal_digest(mapping: Mapping[str, Any], *, secret: str | None = None) -> str:
    sec = _secret(secret)
    if not sec:
        return ""
    payload = build_seal_payload(mapping)
    msg = f"exec_health_freeze_sealed_v1|{sec}|{payload}".encode("utf-8")
    return hashlib.sha1(msg).hexdigest()


def verify_sealed_hash(raw: Mapping[str, Any] | None, *, secret: str | None = None) -> bool:
    obj = dict(raw or {})
    if not obj:
        return True
    digest = _s(obj.get("seal_digest"), "")
    version = _i(obj.get("seal_version"), 0)
    if version <= 0 or not digest:
        return False
    exp = compute_seal_digest(obj, secret=secret)
    if not exp:
        return False
    return digest == exp


def prepare_sealed_mapping(
    *,
    prev_raw: Mapping[str, Any] | None,
    mapping: Mapping[str, Any],
    entrypoint: str,
    now_ms: int | None = None,
    secret: str | None = None,
    force_reason: str = "",
) -> Dict[str, Any]:
    sec = _secret(secret)
    prev = dict(prev_raw or {})
    prev_valid = verify_sealed_hash(prev, secret=sec) if prev else True
    version = (_i(prev.get("seal_version"), 0) + 1) if prev_valid else 1
    out = dict(mapping)
    out.update(
        {
            "seal_algorithm": "sha1_v1",
            "seal_version": int(version),
            "seal_prev_digest": _s(prev.get("seal_digest"), "") if prev_valid else "",
            "seal_entrypoint": str(entrypoint or ""),
            "sealed_at_ts_ms": int(now_ms or _now_ms()),
            "seal_force_reason": str(force_reason or ""),
        }
    )
    out["seal_digest"] = compute_seal_digest(out, secret=secret)
    return out


def _flat_pairs(mapping: Mapping[str, Any]) -> list[str]:
    flat: list[str] = []
    for k, v in dict(mapping).items():
        flat.extend([str(k), _s(v)])
    return flat


def ensure_sealed_functions_loaded(redis_client: Any, *, replace: bool = False) -> bool:
    try:
        argv = ["FUNCTION", "LOAD"]
        if replace:
            argv.append("REPLACE")
        argv.append(REDIS_FUNCTION_LIBRARY)
        redis_client.execute_command(*argv)
        return True
    except Exception as exc:
        msg = str(exc).lower()
        if "already exists" in msg or "library name is already taken" in msg or "busy" in msg:
            return True
        return False


def _call_function(redis_client: Any, fn: str, key: str, args: Sequence[Any]) -> int:
    if hasattr(redis_client, "execute_command"):
        try:
            return int(redis_client.execute_command("FCALL", fn, 1, key, *list(args)))
        except Exception as exc:
            msg = str(exc).lower()
            if ("unknown function" in msg or "function not found" in msg) and ensure_sealed_functions_loaded(redis_client):
                return int(redis_client.execute_command("FCALL", fn, 1, key, *list(args)))
    if hasattr(redis_client, "fcall"):
        try:
            return int(redis_client.fcall(fn, 1, key, *list(args)))
        except Exception:
            pass
    raise RuntimeError("Redis client does not support FCALL/execute_command")


def sealed_set_sync(
    redis_client: Any,
    *,
    key: str,
    prev_raw: Mapping[str, Any] | None,
    mapping: Mapping[str, Any],
    entrypoint: str,
    ttl_s: int,
    force: bool = False,
    secret: str | None = None,
    force_reason: str = "",
) -> Dict[str, Any]:
    sec = _secret(secret)
    if not sec and seal_enforced():
        return {"ok": False, "rc": -93, "error": "missing_seal_secret", "mapping": dict(mapping)}
    prev = dict(prev_raw or {})
    prev_valid = verify_sealed_hash(prev, secret=sec) if prev else True
    prev_missing_seal = bool(prev) and (not _s(prev.get("seal_digest")) or _i(prev.get("seal_version"), 0) <= 0)
    if prev and not prev_valid and prev_missing_seal and allow_unsealed_bootstrap():
        prev_valid = False
    elif prev and not prev_valid and not force and seal_enforced():
        return {"ok": False, "rc": -91, "error": "invalid_prev_seal", "mapping": dict(mapping)}
    sealed = prepare_sealed_mapping(prev_raw=prev if (prev_valid and not prev_missing_seal) else {}, mapping=mapping, entrypoint=entrypoint, now_ms=_now_ms(), secret=sec, force_reason=force_reason)
    args = [
        _s(prev.get("seal_digest"), "") if (prev_valid and not prev_missing_seal) else "",
        _i(prev.get("seal_version"), 0) if (prev_valid and not prev_missing_seal) else 0,
        int(ttl_s or 0),
        len(sealed),
        *_flat_pairs(sealed),
    ]
    fn = FN_FORCE_SET if force else FN_SET
    rc = int(_call_function(redis_client, fn, key, args))
    return {"ok": rc == 1, "rc": rc, "mapping": sealed}




async def _aensure_loaded(redis_client: Any) -> bool:
    try:
        await redis_client.execute_command("FUNCTION", "LOAD", REDIS_FUNCTION_LIBRARY)
        return True
    except Exception as exc:
        msg = str(exc).lower()
        return "already exists" in msg or "library name is already taken" in msg or "busy" in msg

async def asealed_set(
    redis_client: Any,
    *,
    key: str,
    prev_raw: Mapping[str, Any] | None,
    mapping: Mapping[str, Any],
    entrypoint: str,
    ttl_s: int,
    force: bool = False,
    secret: str | None = None,
    force_reason: str = "",
) -> Dict[str, Any]:
    sec = _secret(secret)
    if not sec and seal_enforced():
        return {"ok": False, "rc": -93, "error": "missing_seal_secret", "mapping": dict(mapping)}
    prev = dict(prev_raw or {})
    prev_valid = verify_sealed_hash(prev, secret=sec) if prev else True
    prev_missing_seal = bool(prev) and (not _s(prev.get("seal_digest")) or _i(prev.get("seal_version"), 0) <= 0)
    if prev and not prev_valid and prev_missing_seal and allow_unsealed_bootstrap():
        prev_valid = False
    elif prev and not prev_valid and not force and seal_enforced():
        return {"ok": False, "rc": -91, "error": "invalid_prev_seal", "mapping": dict(mapping)}
    sealed = prepare_sealed_mapping(prev_raw=prev if (prev_valid and not prev_missing_seal) else {}, mapping=mapping, entrypoint=entrypoint, now_ms=_now_ms(), secret=sec, force_reason=force_reason)
    args = [
        _s(prev.get("seal_digest"), "") if (prev_valid and not prev_missing_seal) else "",
        _i(prev.get("seal_version"), 0) if (prev_valid and not prev_missing_seal) else 0,
        int(ttl_s or 0),
        len(sealed),
        *_flat_pairs(sealed),
    ]
    fn = FN_FORCE_SET if force else FN_SET
    try:
        rc = int(await redis_client.execute_command("FCALL", fn, 1, key, *list(args)))
    except Exception as exc:
        msg = str(exc).lower()
        if ("unknown function" in msg or "function not found" in msg) and await _aensure_loaded(redis_client):
            rc = int(await redis_client.execute_command("FCALL", fn, 1, key, *list(args)))
        else:
            return {"ok": False, "rc": -92, "error": "function_load_failed", "mapping": dict(mapping)}
    return {"ok": rc == 1, "rc": rc, "mapping": sealed}
