#!/usr/bin/env python3
"""enforce_bucket_slo_freezer_p80.py

P80: Freeze auto-apply for enforce-bucket promoter if SLO degrades on enforced buckets.

This tool sets auto-apply block keys (same namespace as rollback controller) but does NOT rollback.

ENV:
  ENABLE_ENFORCE_BUCKET_SLO_FREEZER=1 (gate)
  ENFORCE_FREEZE_SYMBOLS=BTCUSDT,ETHUSDT (required; empty => no-op)

  ANALYTICS_DB_DSN or DATABASE_URL (required)
  REDIS_URL or CRYPTO_NOTIFY_REDIS_URL (required)

  ENFORCE_STATS_MV (default mv_exec_slippage_eval_1h_stats)
  ENFORCE_STATS_VIEW (fallback v_exec_slippage_eval)

  ENFORCE_FREEZE_LOOKBACK_H (default 4)
  ENFORCE_FREEZE_MIN_DB_SAMPLES (default 80)
  ENFORCE_FREEZE_MAX_P95_RESID_BPS (default 6.0)
  ENFORCE_FREEZE_MAX_P99_RESID_BPS (default 15.0)
  ENFORCE_FREEZE_MAX_EDGE_NEG_SHARE (default 0.40)

  AUTO_APPLY_BLOCK_PREFIX (default cfg:suggestions:entry_policy:auto_apply_block)
  ENFORCE_FREEZE_BLOCK_REASON (default enforce_bucket_promoter)
  ENFORCE_FREEZE_BLOCK_TTL_SEC (default 7200)

Status/notify:
  ENFORCE_FREEZER_STATUS_PATH
    default: /var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json

  ENFORCE_BUCKET_NOTIFY=1
  ENFORCE_BUCKET_NOTIFY_COOLDOWN_SEC (default 1800)
  ENFORCE_BUCKET_NOTIFY_STREAM or NOTIFY_TELEGRAM_STREAM (default notify:telegram)

Audit:
  ENFORCE_FREEZER_EVENTS_STREAM (default events:enforce_bucket_slo_freezer)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None

try:
    import redis  # type: ignore
except Exception:
    redis = None


def _now_ms() -> int:
    return get_ny_time_millis()


def _env_int(name: str, default: str) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return float(default)


def _env_list(name: str, default: str) -> List[str]:
    raw = str(os.getenv(name, default) or "").strip()
    if not raw:
        return []
    out = []
    for x in raw.replace(";", ",").split(","):
        s = x.strip().upper()
        if s:
            out.append(s)
    return out


def _write_status(path: str, obj: dict) -> None:
    try:
        p = str(path or "").strip()
        if not p:
            return
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, p)
    except Exception:
        return


def _get_redis_cli() -> Any:
    if redis is None:
        raise RuntimeError("redis-py missing")
    url = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or ""
    if not url:
        raise RuntimeError("missing REDIS_URL/CRYPTO_NOTIFY_REDIS_URL")
    return redis.Redis.from_url(url, decode_responses=True)


def _get_db_dsn() -> str:
    dsn = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or ""
    if not dsn:
        raise RuntimeError("missing ANALYTICS_DB_DSN/DATABASE_URL")
    return dsn


def _read_pref(r: Any, base: str, sym: str) -> str:
    v = r.get(f"{base}:{sym}")
    if v:
        return str(v)
    v = r.get(base)
    return str(v or "")


def _query_stats(dsn: str, *, sym: str, lookback_h: int) -> Dict[str, Tuple[int, float, float, float]]:
    mv = os.getenv("ENFORCE_STATS_MV", "mv_exec_slippage_eval_1h_stats").strip() or "mv_exec_slippage_eval_1h_stats"
    view = os.getenv("ENFORCE_STATS_VIEW", "v_exec_slippage_eval").strip() or "v_exec_slippage_eval"

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    out: Dict[str, Tuple[int, float, float, float]] = {}
    try:
        cur.execute(
            f"""
            select exec_regime_bucket,
                   sum(n)::bigint as n,
                   max(resid_p95_bps) as resid_p95_bps,
                   max(resid_p99_bps) as resid_p99_bps,
                   max(edge_neg_share) as edge_neg_share
            from {mv}
            where sym=%s and t >= now() - (%s || ' hours')::interval
            group by 1
            """,
            (sym, int(lookback_h)),
        )
        rows = cur.fetchall()
        for b, n, p95, p99, neg in rows:
            out[str(b).upper()] = (int(n or 0), float(p95 or 0.0), float(p99 or 0.0), float(neg or 0.0))
        return out
    except Exception:
        cur.execute(
            f"""
            select exec_regime_bucket,
                   count(*)::bigint as n,
                   percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
                   percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
                   avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
            from {view}
            where sym=%s and ts >= now() - (%s || ' hours')::interval
            group by 1
            """,
            (sym, int(lookback_h)),
        )
        rows = cur.fetchall()
        for b, n, p95, p99, neg in rows:
            out[str(b).upper()] = (int(n or 0), float(p95 or 0.0), float(p99 or 0.0), float(neg or 0.0))
        return out
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _allow_bucket(allow: str, bucket: str, default_bucket: str = "HIGH_VOL_LOW_LIQ") -> bool:
    s = (allow or "").strip().upper()
    b = (bucket or "").strip().upper() or "NORMAL"
    if s in ("ALL", "*", "ANY"):
        return True
    if not s:
        return b == default_bucket
    parts = [x.strip().upper() for x in s.replace(";", ",").split(",") if x.strip()]
    return b in parts


def _notify_freeze(r: Any, *, text: str) -> None:
    if _env_int("ENFORCE_BUCKET_NOTIFY", "0") != 1:
        return
    cooldown = _env_int("ENFORCE_BUCKET_NOTIFY_COOLDOWN_SEC", "1800")
    now = _now_ms()
    key = "state:enforce_bucket_slo_freezer:last_notify_ts_ms"
    try:
        last = int(r.get(key) or "0")
    except Exception:
        last = 0
    if last and (now - last) < (cooldown * 1000):
        return

    stream = os.getenv("ENFORCE_BUCKET_NOTIFY_STREAM") or os.getenv("NOTIFY_TELEGRAM_STREAM") or "notify:telegram"
    try:
        r.set(key, str(now))
        r.expire(key, max(60, cooldown))
        r.xadd(stream, {"type": "report", "ts_ms": str(now), "text": text[:3500]})
    except Exception:
        return


def _xadd_event(r: Any, *, sym: str, bucket: str, meta: dict) -> None:
    stream = os.getenv("ENFORCE_FREEZER_EVENTS_STREAM", "events:enforce_bucket_slo_freezer")
    now = _now_ms()
    try:
        fields = {
            "type": "freeze",
            "ts_ms": str(now),
            "sym": str(sym),
            "bucket": str(bucket),
            "meta": json.dumps(meta, separators=(",", ":"))[:3500],
        }
        r.xadd(stream, fields)
    except Exception:
        return


def main() -> int:
    status_path = os.getenv(
        "ENFORCE_FREEZER_STATUS_PATH",
        "/var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json",
    )

    if _env_int("ENABLE_ENFORCE_BUCKET_SLO_FREEZER", "0") != 1:
        _write_status(status_path, {"ok": True, "ts_ms": _now_ms(), "enabled": False})
        return 0
    if psycopg2 is None:
        _write_status(status_path, {"ok": False, "ts_ms": _now_ms(), "error": "psycopg2_missing"})
        print("FATAL: psycopg2 missing", file=sys.stderr)
        return 2

    syms = _env_list("ENFORCE_FREEZE_SYMBOLS", "")
    if not syms:
        _write_status(status_path, {"ok": True, "ts_ms": _now_ms(), "enabled": True, "note": "no_symbols"})
        return 0

    lookback_h = _env_int("ENFORCE_FREEZE_LOOKBACK_H", "4")
    min_db_n = _env_int("ENFORCE_FREEZE_MIN_DB_SAMPLES", "80")
    max_p95 = _env_float("ENFORCE_FREEZE_MAX_P95_RESID_BPS", "6.0")
    max_p99 = _env_float("ENFORCE_FREEZE_MAX_P99_RESID_BPS", "15.0")
    max_neg = _env_float("ENFORCE_FREEZE_MAX_EDGE_NEG_SHARE", "0.40")
    ttl = _env_int("ENFORCE_FREEZE_BLOCK_TTL_SEC", "7200")

    block_prefix = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
    block_reason = os.getenv("ENFORCE_FREEZE_BLOCK_REASON", "enforce_bucket_promoter")

    r = _get_redis_cli()
    dsn = _get_db_dsn()
    now = _now_ms()

    # default status
    _write_status(
        status_path,
        {
            "ok": True,
            "ts_ms": now,
            "enabled": True,
            "blocked": False,
            "symbols": syms,
            "lookback_h": lookback_h,
        },
    )

    for sym in syms:
        slip_allow = _read_pref(r, "cfg:slippage_decomp_enforce_buckets", sym)
        taker_allow = _read_pref(r, "cfg:taker_flow_gate_enforce_buckets", sym)
        allow = slip_allow or taker_allow
        if not allow:
            continue

        stats = _query_stats(dsn, sym=sym, lookback_h=lookback_h)
        for bucket, (n, p95, p99, neg) in stats.items():
            if n < min_db_n:
                continue
            if not _allow_bucket(allow, bucket):
                continue
            if (p95 > max_p95) or (p99 > max_p99) or (neg > max_neg):
                block_key = f"{block_prefix}:{block_reason}"
                meta_key = f"{block_prefix}:{block_reason}:meta"
                ts_key = f"{block_prefix}:{block_reason}:ts_ms"
                meta = {
                    "blocked": True,
                    "reason": "slo_freeze",
                    "ts_ms": now,
                    "sym": sym,
                    "bucket": bucket,
                    "lookback_h": lookback_h,
                    "n": n,
                    "resid_p95_bps": p95,
                    "resid_p99_bps": p99,
                    "edge_neg_share": neg,
                }
                pipe = r.pipeline(transaction=False)
                pipe.set(block_key, "1")
                pipe.set(meta_key, json.dumps(meta, separators=(",", ":")))
                pipe.set(ts_key, str(now))
                pipe.expire(block_key, ttl)
                pipe.expire(meta_key, ttl)
                pipe.expire(ts_key, ttl)
                pipe.execute()

                _xadd_event(r, sym=sym, bucket=bucket, meta=meta)
                _notify_freeze(
                    r,
                    text=(
                        f"ENFORCE SLO FREEZE: sym={sym} bucket={bucket} lookback_h={lookback_h} "
                        f"n={n} resid_p95={p95:.2f} resid_p99={p99:.2f} edge_neg_share={neg:.3f} "
                        f"(ttl={ttl}s)"
                    ),
                )

                _write_status(
                    status_path,
                    {
                        "ok": True,
                        "ts_ms": now,
                        "enabled": True,
                        "blocked": True,
                        "sym": sym,
                        "bucket": bucket,
                        "meta": meta,
                    },
                )
                return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
