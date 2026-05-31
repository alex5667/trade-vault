from __future__ import annotations
from core.redis_keys import RedisStreams as RS

# -*- coding: utf-8 -*-
"""enforce_bucket_promoter_rollback_controller_v1.py

Rollback controller for bucket-aware enforcement.

Purpose:
- If promoter expanded enforcement to additional buckets and post-change QA worsens,
  rollback to previous allowlists and block further auto-apply.

Inputs:
- Redis state keys written by nightly_enforce_bucket_promoter_v1:
    state:enforce_bucket_promoter:last_apply_ts_ms
    state:enforce_bucket_promoter:prev_slippage_decomp_enforce_buckets
    state:enforce_bucket_promoter:prev_taker_flow_gate_enforce_buckets
- Current cfg keys:
    cfg:slippage_decomp_enforce_buckets
    cfg:taker_flow_gate_enforce_buckets

DB:
- Uses v_exec_slippage_eval for residual quantiles.

ENV (defaults are conservative):
  ENFORCE_BUCKET_ROLLBACK_MIN_AGE_SEC=1800
  ENFORCE_BUCKET_ROLLBACK_LOOKBACK_H=4
  ENFORCE_BUCKET_ROLLBACK_MIN_DB_SAMPLES=80
  ENFORCE_BUCKET_ROLLBACK_MAX_P95_RESID_BPS=5.0
  ENFORCE_BUCKET_ROLLBACK_MAX_P99_RESID_BPS=12.0
  ENFORCE_BUCKET_ROLLBACK_MAX_EDGE_NEG_SHARE=0.35
  ENFORCE_BUCKET_ROLLBACK_APPLY=0|1   (or --apply)

  ENFORCE_BUCKET_EVENT_STREAM=events:enforce_bucket_promoter
  ENFORCE_BUCKET_EVENT_STREAM_MAXLEN=20000

Auto-apply block (to stop further promotions):
  AUTO_APPLY_BLOCK_PREFIX=cfg:suggestions:entry_policy:auto_apply_block
  ENFORCE_BUCKET_ROLLBACK_BLOCK_REASON=enforce_bucket_promoter
  ENFORCE_BUCKET_ROLLBACK_BLOCK_TTL_SEC=7200

Usage:
  python -m orderflow_services.enforce_bucket_promoter_rollback_controller_v1 --apply 1,
""",
import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

try:
    # Prefer local package when running as -m ...
    from orderflow_services.redis_lock_v1 import acquire_lock as _acquire_lock  # type: ignore
    from orderflow_services.redis_lock_v1 import release_lock as _release_lock
except Exception:  # pragma: no cover
    try:
        from .redis_lock_v1 import acquire_lock as _acquire_lock  # type: ignore
        from .redis_lock_v1 import release_lock as _release_lock
    except Exception:  # pragma: no cover
        _acquire_lock = None
        _release_lock = None


def _now_ms() -> int:
    return get_ny_time_millis()


def _notify_stream_name() -> str:
    return (
        os.getenv("ENFORCE_BUCKET_NOTIFY_STREAM")
        or os.getenv("NOTIFY_TELEGRAM_STREAM")
        or os.getenv("CRYPTO_NOTIFY_STREAM")
        or RS.NOTIFY_TELEGRAM
    )


def _notify_enabled() -> bool:
    return (os.getenv("ENFORCE_BUCKET_NOTIFY", "1") or "1").strip().lower() in ("1", "true", "yes", "on")


async def _notify_once(r: Any, text: str, *, cooldown_key: str, cooldown_sec: int) -> None:
    if not _notify_enabled():
        return
    try:
        now = _now_ms()
        last = await r.get(cooldown_key)
        if last:
            try:
                if (now - int(float(last))) < (int(cooldown_sec) * 1000):
                    return
            except Exception:
                pass
        fields = {"type": "report", "text": str(text)[:3500], "ts": str(now)}
        await r.xadd(_notify_stream_name(), fields, maxlen=200000, approximate=True)
        with contextlib.suppress(Exception):
            await r.set(cooldown_key, str(now), ex=int(cooldown_sec))
    except Exception:
        return


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return default


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default).strip())
    except Exception:
        return default


def _norm_bucket(b: Any) -> str:
    s = (b or "").strip().upper()
    return s or "NORMAL"


def _parse_allowlist(raw: Any) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    xs: list[str] = []
    for p in raw.replace(";", ",").split(","):
        s = p.strip().upper()
        if s and s not in xs:
            xs.append(s)
    return xs


def _allowlist_to_str(xs: list[str]) -> str:
    return ",".join(xs)


@dataclass(frozen=True)
class BucketStats:
    bucket: str
    db_n: int
    resid_p95: float
    resid_p99: float
    edge_neg_share: float


@dataclass(frozen=True)
class RollbackDecision:
    rollback: bool
    reasons: list[str]
    target_slip: str
    target_taker: str


def decide_rollback(
    *,
    added_buckets: list[str],
    stats_by_bucket: dict[str, BucketStats],
    min_db_n: int,
    max_p95: float,
    max_p99: float,
    max_edge_neg_share: float,
    target_slip: str,
    target_taker: str,
) -> RollbackDecision:
    reasons: list[str] = []
    for b in added_buckets:
        bb = _norm_bucket(b)
        st = stats_by_bucket.get(bb)
        if not st:
            reasons.append(f"{bb}:no_stats")
            continue
        if st.db_n < min_db_n:
            reasons.append(f"{bb}:low_n:{st.db_n}")
            continue
        if st.resid_p95 > max_p95:
            reasons.append(f"{bb}:p95_high:{st.resid_p95:.2f}")
        if st.resid_p99 > max_p99:
            reasons.append(f"{bb}:p99_high:{st.resid_p99:.2f}")
        if st.edge_neg_share > max_edge_neg_share:
            reasons.append(f"{bb}:edge_neg_high:{st.edge_neg_share:.3f}")

    do = any((":p95_high:" in r or ":p99_high:" in r or ":edge_neg_high:" in r) for r in reasons)
    return RollbackDecision(rollback=do, reasons=reasons[:12], target_slip=target_slip, target_taker=target_taker)


async def _xadd_event(r: Any, *, stream: str, fields: dict[str, Any], maxlen: int) -> None:
    try:
        payload = {str(k): ("" if v is None else str(v)) for k, v in (fields or {}).items()}
        await r.xadd(stream, payload, maxlen=maxlen, approximate=True)
    except Exception:
        return


async def _fetch_bucket_stats(conn: Any, *, since_ts_ms: int, lookback_h: int) -> dict[str, BucketStats]:
    # We include a lookback cap to limit scans.
    q = f"""
    SELECT
      exec_regime_bucket,
      count(*) as n,
      percentile_cont(0.95) within group (order by slippage_residual_bps) as p95_resid,
      percentile_cont(0.99) within group (order by slippage_residual_bps) as p99_resid,
      avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
    FROM v_exec_slippage_eval
    WHERE ts >= greatest(to_timestamp($1::double precision/1000.0), now() - interval '{int(lookback_h)} hours')
    GROUP BY exec_regime_bucket
    """
    rows = await conn.fetch(q, float(since_ts_ms))
    out: dict[str, BucketStats] = {}
    for r in rows:
        b = _norm_bucket(r.get("exec_regime_bucket") or "NORMAL")
        out[b] = BucketStats(
            bucket=b,
            db_n=int(r.get("n") or 0),
            resid_p95=float(r.get("p95_resid") or 0.0),
            resid_p99=float(r.get("p99_resid") or 0.0),
            edge_neg_share=float(r.get("edge_neg_share") or 0.0),
        )
    return out


async def run(apply: bool) -> int:
    if aioredis is None:
        print("FATAL: redis.asyncio unavailable")
        return 2

    redis_url = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or ""
    db_url = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or ""
    if not redis_url or not db_url:
        print(json.dumps({"ok": False, "error": "missing REDIS_URL or ANALYTICS_DB_DSN"}, ensure_ascii=False))
        return 2

    min_age_sec = _env_int("ENFORCE_BUCKET_ROLLBACK_MIN_AGE_SEC", "1800")
    lookback_h = _env_int("ENFORCE_BUCKET_ROLLBACK_LOOKBACK_H", "4")

    min_db_n = _env_int("ENFORCE_BUCKET_ROLLBACK_MIN_DB_SAMPLES", "80")
    max_p95 = _env_float("ENFORCE_BUCKET_ROLLBACK_MAX_P95_RESID_BPS", "5.0")
    max_p99 = _env_float("ENFORCE_BUCKET_ROLLBACK_MAX_P99_RESID_BPS", "12.0")
    max_edge_neg = _env_float("ENFORCE_BUCKET_ROLLBACK_MAX_EDGE_NEG_SHARE", "0.35")

    block_prefix = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
    block_reason = os.getenv("ENFORCE_BUCKET_ROLLBACK_BLOCK_REASON", "enforce_bucket_promoter")
    block_ttl = _env_int("ENFORCE_BUCKET_ROLLBACK_BLOCK_TTL_SEC", "7200")

    ev_stream = os.getenv("ENFORCE_BUCKET_EVENT_STREAM", "events:enforce_bucket_promoter")
    ev_maxlen = _env_int("ENFORCE_BUCKET_EVENT_STREAM_MAXLEN", "20000")

    r = aioredis.Redis.from_url(redis_url, decode_responses=True)

    lock_key = os.getenv('ENFORCE_BUCKET_ROLLBACK_LOCK_KEY', 'lock:enforce_bucket_promoter:rollback')
    lock_ttl = _env_int('ENFORCE_BUCKET_ROLLBACK_LOCK_TTL_SEC', '900')
    lock_token = ''
    if _acquire_lock is not None:
        lock_token = await _acquire_lock(r, key=lock_key, ttl_sec=lock_ttl)
        if not lock_token:
            # Another rollback instance is running. No-op.
            await r.aclose()
            return 0

    # Load state
    pipe = r.pipeline(transaction=False)
    pipe.get("state:enforce_bucket_promoter:last_apply_ts_ms")
    pipe.get("state:enforce_bucket_promoter:last_rollback_ts_ms")
    pipe.get("state:enforce_bucket_promoter:prev_slippage_decomp_enforce_buckets")
    pipe.get("state:enforce_bucket_promoter:prev_taker_flow_gate_enforce_buckets")
    pipe.get("cfg:slippage_decomp_enforce_buckets")
    pipe.get("cfg:taker_flow_gate_enforce_buckets")
    res = await pipe.execute()

    last_apply_ts = int(float(res[0] or 0)) if res[0] else 0
    last_rb_ts = int(float(res[1] or 0)) if res[1] else 0
    prev_slip = str(res[2] or "")
    prev_taker = str(res[3] or "")
    cur_slip = str(res[4] or "")
    cur_taker = str(res[5] or "")

    now = _now_ms()

    if last_apply_ts <= 0:
        try:
            if lock_token and _release_lock is not None:
                await _release_lock(r, key=lock_key, token=lock_token)
        except Exception:
            pass
        await r.aclose()
        return 0

    if last_rb_ts > last_apply_ts:
        # Already rolled back after the last apply
        try:
            if lock_token and _release_lock is not None:
                await _release_lock(r, key=lock_key, token=lock_token)
        except Exception:
            pass
        await r.aclose()
        return 0

    if (now - last_apply_ts) < (min_age_sec * 1000):
        try:
            if lock_token and _release_lock is not None:
                await _release_lock(r, key=lock_key, token=lock_token)
        except Exception:
            pass
        await r.aclose()
        return 0

    # If prev values are empty, rollback is unsafe
    if not prev_slip and not prev_taker:
        try:
            if lock_token and _release_lock is not None:
                await _release_lock(r, key=lock_key, token=lock_token)
        except Exception:
            pass
        await r.aclose()
        return 0

    added: list[str] = []
    # Determine added buckets (union across components)
    cur_s = set(_parse_allowlist(cur_slip))
    prev_s = set(_parse_allowlist(prev_slip))
    cur_t = set(_parse_allowlist(cur_taker))
    prev_t = set(_parse_allowlist(prev_taker))
    for b in sorted((cur_s - prev_s) | (cur_t - prev_t)):
        added.append(_norm_bucket(b))

    if not added:
        try:
            if lock_token and _release_lock is not None:
                await _release_lock(r, key=lock_key, token=lock_token)
        except Exception:
            pass
        await r.aclose()
        return 0

    # DB stats after apply
    try:
        import asyncpg  # type: ignore
        conn = await asyncpg.connect(db_url)
        stats = await _fetch_bucket_stats(conn, since_ts_ms=last_apply_ts, lookback_h=lookback_h)
        await conn.close()
    except Exception as e:
        await r.aclose()
        print(json.dumps({"ok": False, "error": f"db_error:{e}"}, ensure_ascii=False))
        return 2

    dec = decide_rollback(
        added_buckets=added,
        stats_by_bucket=stats,
        min_db_n=min_db_n,
        max_p95=max_p95,
        max_p99=max_p99,
        max_edge_neg_share=max_edge_neg,
        target_slip=prev_slip,
        target_taker=prev_taker,
    )

    out = {
        "ok": True,
        "ts_ms": now,
        "last_apply_ts_ms": last_apply_ts,
        "added_buckets": added,
        "decision": {
            "rollback": dec.rollback,
            "reasons": dec.reasons,
            "target_slip": dec.target_slip,
            "target_taker": dec.target_taker,
        },
        "bucket_stats": {k: vars(v) for k, v in stats.items()},
        "apply": bool(apply),
    }

    if not dec.rollback:
        await r.aclose()
        print(json.dumps(out, ensure_ascii=False))
        return 0

    if not apply:
        await r.aclose()
        print(json.dumps(out, ensure_ascii=False))
        return 2

    # Apply rollback
    rb_ts = now
    try:
        pipe2 = r.pipeline(transaction=False)
        if prev_slip:
            pipe2.set("cfg:slippage_decomp_enforce_buckets", prev_slip)
        if prev_taker:
            pipe2.set("cfg:taker_flow_gate_enforce_buckets", prev_taker)
        pipe2.set("state:enforce_bucket_promoter:last_rollback_ts_ms", str(rb_ts))
        pipe2.set("state:enforce_bucket_promoter:last_rollback_reason", ";".join(dec.reasons) or "unknown")
        pipe2.set("state:enforce_bucket_promoter:last_rollback_meta", json.dumps(out, separators=(",", ":")))

        # Block further auto-apply until reviewed
        block_key = f"{block_prefix}:{block_reason}"
        meta_key = f"{block_prefix}:{block_reason}:meta"
        ts_key = f"{block_prefix}:{block_reason}:ts_ms"
        meta = {
            "blocked": True,
            "reason": "rollback_triggered",
            "rollback_ts_ms": rb_ts,
            "last_apply_ts_ms": last_apply_ts,
            "reasons": dec.reasons,
        }
        pipe2.set(block_key, "1")
        pipe2.set(meta_key, json.dumps(meta, separators=(",", ":")))
        pipe2.set(ts_key, str(rb_ts))
        pipe2.expire(block_key, block_ttl)
        pipe2.expire(meta_key, block_ttl)
        pipe2.expire(ts_key, block_ttl)

        await pipe2.execute()

        await _xadd_event(
            r,
            stream=ev_stream,
            fields={
                "type": "rollback",
                "ts_ms": rb_ts,
                "added_buckets": _allowlist_to_str(added),
                "target_slip": prev_slip,
                "target_taker": prev_taker,
                "reasons": ";".join(dec.reasons),
            }, maxlen=ev_maxlen,
        )

        # Notify ops channel (with cooldown)
        try:
            cd = _env_int("ENFORCE_BUCKET_NOTIFY_COOLDOWN_SEC", "1800")
            txt = ("[EnforceBucketRollback] ROLLBACK added=" + _allowlist_to_str(added) +
                   " -> slip='" + (prev_slip or "") + "' taker='" + (prev_taker or "") +
                   "' reasons=" + (";".join(dec.reasons) or "unknown"))
            await _notify_once(r, txt, cooldown_key="notify:enforce_bucket_rollback:cooldown", cooldown_sec=cd)
        except Exception:
            pass
    except Exception as e:
        await r.aclose()
        print(json.dumps({"ok": False, "error": f"rollback_apply_failed:{e}"}, ensure_ascii=False))
        return 2

    try:
        if lock_token and _release_lock is not None:
            await _release_lock(r, key=lock_key, token=lock_token)
    except Exception:
        pass
    await r.aclose()
    out["rollback_applied"] = True
    out["rollback_ts_ms"] = rb_ts
    print(json.dumps(out, ensure_ascii=False))
    return 2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", type=int, default=int(os.getenv("ENFORCE_BUCKET_ROLLBACK_APPLY", "0") or 0))
    args = ap.parse_args()
    return asyncio.run(run(apply=bool(args.apply)))


if __name__ == "__main__":
    raise SystemExit(main())
