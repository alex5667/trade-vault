from __future__ import annotations
"""OFInputs V3 circuit breaker (P100).

Goal
- Deterministic V3->V2 fallback when V3 book/LOB inputs degrade.
- Persist disable state in Redis under cfg:* (TTL) so it survives state:* cleanups.
- Track downgrades in per-reason ZSET keys (state:*), enabling efficient ZCOUNT.
- Optionally set global/per-symbol auto-apply blocks (fail-closed) on trip.

Keys
- cfg disable:   cfg:of_inputs:v3_disabled:{sym} (TTL)
- downgrades:    state:of_inputs:v3_downgrades:{reason}:{sym} (ZSET)
- seq counter:   state:of_inputs:v3_downgrades_seq:{reason}:{sym} (STRING INT)
- auto-apply:    cfg:of_inputs_v3:auto_apply_block_global:{reason} (TTL)
                cfg:of_inputs_v3:auto_apply_block:{sym}:{reason} (TTL)

Notes
- This module is designed for use in the tick path: all redis ops must be best-effort
  and optionally time-bounded by the caller (see call_with_timeout).
- Time source must be deterministic: pass tick_ts_ms as now_ms.
"""


import asyncio
import json
import re
from typing import Any, Dict, Optional, Tuple


_CFG_DISABLED_PREFIX = "cfg:of_inputs:v3_disabled"
_STATE_DOWGRADES_PREFIX = "state:of_inputs:v3_downgrades"
_STATE_DOWGRADES_SEQ_PREFIX = "state:of_inputs:v3_downgrades_seq"

_AUTO_APPLY_GLOBAL_PREFIX = "cfg:of_inputs_v3:auto_apply_block_global"
_AUTO_APPLY_SYM_PREFIX = "cfg:of_inputs_v3:auto_apply_block"

_REASON_SAFE_RE = re.compile(r"[^a-z0-9_]+")


def _norm_reason(reason: str) -> str:
    r = str(reason or "").strip().lower()
    r = r.replace("-", "_")
    r = _REASON_SAFE_RE.sub("_", r)
    r = re.sub(r"_+", "_", r).strip("_")
    return r or "unknown"


def _cfg_disabled_key(sym: str) -> str:
    return f"{_CFG_DISABLED_PREFIX}:{sym}"


def _downgrades_zset_key(sym: str, reason: str) -> str:
    rsn = _norm_reason(reason)
    return f"{_STATE_DOWGRADES_PREFIX}:{rsn}:{sym}"


def _downgrades_seq_key(sym: str, reason: str) -> str:
    rsn = _norm_reason(reason)
    return f"{_STATE_DOWGRADES_SEQ_PREFIX}:{rsn}:{sym}"


def _auto_apply_global_key(reason: str) -> str:
    rsn = _norm_reason(reason)
    return f"{_AUTO_APPLY_GLOBAL_PREFIX}:{rsn}"


def _auto_apply_sym_key(sym: str, reason: str) -> str:
    rsn = _norm_reason(reason)
    return f"{_AUTO_APPLY_SYM_PREFIX}:{sym}:{rsn}"


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return "{}"


def _json_loads(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    try:
        x = json.loads(s)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


async def call_with_timeout(awaitable, timeout_ms: int) -> Any:
    """Best-effort: return None on timeout/exception."""
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_ms / 1000.0)
    except Exception:
        return None


async def refresh_disabled_state(
    redis,
    runtime,
    now_ms: int,
    refresh_every_ms: int = 10_000,
) -> Tuple[bool, int, str]:
    """Refresh runtime cache from cfg disable key.

    Returns: (disabled, disabled_until_ms, disabled_reason)

    Value semantics:
    - If cfg key contains JSON with {"until_ms": ...}, we prefer it.
    - Otherwise, we derive until_ms from PTTL.
    """
    try:
        last = int(getattr(runtime, "of_inputs_v3_cb_last_refresh_ts_ms", 0) or 0)
        if last > 0 and (now_ms - last) < int(refresh_every_ms):
            until_ms = int(getattr(runtime, "of_inputs_v3_disabled_until_ms", 0) or 0)
            rsn = str(getattr(runtime, "of_inputs_v3_disabled_reason", "") or "")
            return (until_ms > now_ms, until_ms, rsn)

        setattr(runtime, "of_inputs_v3_cb_last_refresh_ts_ms", int(now_ms))
        sym = str(getattr(runtime, "symbol", "") or "")
        if not sym:
            return (False, 0, "")

        key = _cfg_disabled_key(sym)

        # Pipeline GET + PTTL to keep 1 RTT.
        pipe = redis.pipeline(transaction=False)
        pipe.get(key)
        pipe.pttl(key)
        raw, pttl = await pipe.execute()

        until_ms = 0
        hard_until_ms = 0
        rsn = ""

        if raw is not None:
            meta = _json_loads(raw)
            try:
                until_ms = int(meta.get("until_ms") or 0)
            except Exception:
                until_ms = 0
            try:
                hard_until_ms = int(meta.get("hard_until_ms") or 0)
            except Exception:
                hard_until_ms = 0
            rsn = str(meta.get("reason") or meta.get("dq_code") or "cfg")

            # If no until_ms in value, derive from TTL.
            try:
                pttl_i = int(pttl) if pttl is not None else -2
            except Exception:
                pttl_i = -2
            if until_ms <= 0:
                if pttl_i is None or pttl_i < 0:
                    # No TTL (manual hard disable) -> represent as far future.
                    until_ms = now_ms + 10 * 365 * 24 * 3600 * 1000
                    rsn = rsn or "manual_no_ttl"
                else:
                    until_ms = now_ms + max(0, pttl_i)
                    rsn = rsn or "cfg_ttl"

        else:
            # No key.
            until_ms = 0
            rsn = ""

        if int(hard_until_ms or 0) <= 0:
            hard_until_ms = int(until_ms)
        setattr(runtime, "of_inputs_v3_disabled_until_ms", int(until_ms))
        setattr(runtime, "of_inputs_v3_disabled_hard_until_ms", int(hard_until_ms or until_ms or 0))
        setattr(runtime, "of_inputs_v3_disabled_reason", str(rsn or ""))

        # Phase: hard vs cooldown. We remain disabled until `until_ms`.
        phase = ""
        try:
            hu = int(hard_until_ms or until_ms or 0)
            if hu > 0 and int(now_ms) < hu:
                phase = "hard"
            elif int(until_ms or 0) > 0 and hu > 0 and int(now_ms) < int(until_ms) and hu < int(until_ms):
                phase = "cooldown"
        except Exception:
            phase = ""
        setattr(runtime, "of_inputs_v3_disabled_phase", phase)

        return (int(until_ms) > now_ms, int(until_ms), str(rsn or ""))

    except Exception:
        return (False, int(getattr(runtime, "of_inputs_v3_disabled_until_ms", 0) or 0), str(getattr(runtime, "of_inputs_v3_disabled_reason", "") or ""))


async def record_downgrade_and_maybe_trip(
    redis,
    sym: str,
    now_ms: int,
    downgrade_reason: str,
    window_ms: int,
    max_downgrades_in_window: int,
    disable_ms: int,
    cooldown_ms: int = 0,
    block_auto_apply: bool = True,
    auto_apply_reason: str = "of_inputs_v3",
) -> Dict[str, Any]:
    """Record a V3->V2 downgrade and trip circuit if threshold exceeded.

    Returns dict:
      {"tripped": 0/1, "count": int, "disabled_until_ms": int}

    Redis usage is per-reason ZSET for O(1) ZCOUNT.
    """
    rsn = _norm_reason(downgrade_reason)
    sym_s = str(sym or "")
    if not sym_s:
        return {"tripped": 0, "count": 0, "disabled_until_ms": 0}

    try:
        key = _downgrades_zset_key(sym_s, rsn)
        seq_key = _downgrades_seq_key(sym_s, rsn)

        # Add member with deterministic seq
        seq = await redis.incr(seq_key)
        member = f"{int(now_ms)}:{int(seq)}"

        # Maintain window
        lo = int(now_ms) - int(window_ms)
        pipe = redis.pipeline(transaction=False)
        pipe.zadd(key, {member: int(now_ms)})
        pipe.zremrangebyscore(key, 0, lo - 1)
        pipe.zcount(key, lo, int(now_ms))
        _, _, count = await pipe.execute()

        try:
            c = int(count)
        except Exception:
            c = 0

        if c < int(max_downgrades_in_window):
            return {"tripped": 0, "count": c, "disabled_until_ms": 0}

        # Trip
        hard_until_ms = int(now_ms) + int(disable_ms)
        cd_ms = int(cooldown_ms) if int(cooldown_ms) > 0 else 0
        until_ms = int(hard_until_ms) + int(cd_ms)
        disable_key = _cfg_disabled_key(sym_s)
        payload = {
            "until_ms": int(until_ms),
            "hard_until_ms": int(hard_until_ms),
            "cooldown_ms": int(cd_ms),
            "reason": str(rsn),
            "trip_ts_ms": int(now_ms),
            "count": int(c),
            "window_ms": int(window_ms),
        }

        pipe2 = redis.pipeline(transaction=False)
        ttl_ms = max(1, int(until_ms) - int(now_ms))
        pipe2.set(disable_key, _json_dumps(payload), px=int(ttl_ms))

        if bool(block_auto_apply):
            gk = _auto_apply_global_key(auto_apply_reason)
            sk = _auto_apply_sym_key(sym_s, auto_apply_reason)
            pipe2.set(gk, _json_dumps({"ts_ms": int(now_ms), "reason": str(auto_apply_reason), "src": "of_inputs_v3_circuit"}), px=int(ttl_ms))
            pipe2.set(sk, _json_dumps({"ts_ms": int(now_ms), "symbol": sym_s, "reason": str(auto_apply_reason), "src": "of_inputs_v3_circuit"}), px=int(ttl_ms))

        await pipe2.execute()
        return {
            "tripped": 1,
            "count": c,
            "disabled_until_ms": int(until_ms),
            "hard_until_ms": int(hard_until_ms),
            "cooldown_ms": int(cd_ms),
        }

    except Exception:
        return {"tripped": 0, "count": 0, "disabled_until_ms": 0}
