from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


async def _read_recent_audit(r: aioredis.Redis, stream: str, since_ms: int, limit: int = 2000) -> list[dict[str, Any]]:
    """
    Reads from stream newest-first until older than since_ms or limit.
    Assumes xrevrange is available.
    """
    out: list[dict[str, Any]] = []
    try:
        entries = await r.xrevrange(stream, max="+", min="-", count=limit)
    except Exception:
        return out
    for msg_id, fields in entries:
        try:
            p = json.loads(fields.get("payload", "") or "{}")
            ts = int(p.get("ts_ms") or fields.get("ts_ms") or 0)
            if ts < since_ms:
                break
            out.append(p if isinstance(p, dict) else {})
        except Exception:
            continue
    return out


def _audit_health(aud: list[dict[str, Any]]) -> tuple[bool, str]:
    """
    Very conservative promotion rule:
      - need >= N audit events in shadow window
      - deny_rate should not explode
    """
    n = len(aud)
    if n < int(os.getenv("EP_PROMOTE_MIN_AUDIT_N", "50")):
        return False, f"too_few_audits n={n}"
    ok = sum(1 for x in aud if int(x.get("ok", 0) or 0) == 1)
    deny = n - ok
    deny_rate = (deny / max(n, 1)) * 100.0
    max_deny = float(os.getenv("EP_PROMOTE_MAX_DENY_RATE", "95.0"))
    if deny_rate > max_deny:
        return False, f"deny_rate_high {deny_rate:.2f}%>max {max_deny:.2f}%"
    return True, f"ok n={n} deny_rate={deny_rate:.2f}%"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sid", default="latest", help="rollout sid or 'latest' to find newest rollout key")
    ap.add_argument("--rollout-prefix", default="cfg:entry_policy:rollout:")
    ap.add_argument("--overrides-key", default=os.getenv("CFG_ENTRY_POLICY_OVERRIDES_KEY", "cfg:entry_policy:overrides"))
    ap.add_argument("--audit-stream", default=os.getenv("TRADE_ENTRY_AUDIT_STREAM", "stream:trade:entry_audit"))
    ap.add_argument("--rollback", action="store_true", help="force rollback")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

    sid = args.sid
    if sid == "latest":
        # find newest rollout key by scanning limited set
        try:
            keys = await r.keys(f"{args.rollout_prefix}*")
            keys = sorted(keys)
            if not keys:
                print("no rollout keys")
                return 2
            key = keys[-1]
        except Exception:
            return 2
    else:
        key = f"{args.rollout_prefix}{sid}"

    raw = await r.get(key)
    if not raw:
        print("no rollout state")
        return 2
    st = json.loads(raw)
    if not isinstance(st, dict):
        return 2
    state = _s(st.get("state", ""))
    if state not in ("shadow", "enforced", "rolled_back"):
        print(f"unexpected state={state}")
        return 2

    # Check time window
    shadow_until = _i(st.get("shadow_until_ts_ms", 0), 0)
    applied_ts = _i(st.get("applied_ts_ms", 0), 0)
    now = _now_ms()
    if not args.rollback and now < shadow_until:
        print(f"shadow window not finished: now={now} < until={shadow_until}")
        return 3

    # Promote decision: use audit stream from applied_ts to now
    aud = await _read_recent_audit(r, args.audit_stream, since_ms=applied_ts, limit=int(os.getenv("EP_PROMOTE_AUDIT_LIMIT", "2000")))
    ok, note = _audit_health(aud)

    if args.rollback or (not ok):
        # rollback to previous overrides
        prev_raw = _s(st.get("prev_overrides_raw", ""), "")
        if prev_raw:
            await r.set(args.overrides_key, prev_raw)
        else:
            # if no prev, at least set shadow=1 to prevent trades
            await r.set(args.overrides_key, json.dumps({"version": _now_ms(), "updated_ts_ms": _now_ms(), "overrides": {"ENTRY_POLICY_SHADOW": "1"}}, separators=(",", ":")))
        st["state"] = "rolled_back"
        st["rollback_ts_ms"] = now
        st["rollback_reason"] = note if not args.rollback else "forced"
        await r.set(key, json.dumps(st, ensure_ascii=False, separators=(",", ":")))
        print(f"ROLLED_BACK: {note}")
        return 2

    # promote: set shadow=0 but keep thresholds
    new_doc = st.get("new_overrides", {}) or {}
    ov = (new_doc.get("overrides", {}) or {}) if isinstance(new_doc, dict) else {}
    ov = {str(k): str(v) for k, v in ov.items()}
    ov["ENTRY_POLICY_SHADOW"] = "0"
    promoted = {"version": _now_ms(), "updated_ts_ms": _now_ms(), "overrides": ov}
    await r.set(args.overrides_key, json.dumps(promoted, ensure_ascii=False, separators=(",", ":")))
    st["state"] = "enforced"
    st["promote_ts_ms"] = now
    st["promote_note"] = note
    await r.set(key, json.dumps(st, ensure_ascii=False, separators=(",", ":")))
    print(f"ENFORCED: {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(__import__("asyncio").run(main()))
