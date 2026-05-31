#!/usr/bin/env python3
"""pit_priors_rolling_v1.py — Rolling 7d/30d PIT priors service.

Reads `trades:closed` and materialises per-(symbol, kind, session) rolling
aggregates with a strict embargo so no look-ahead leakage is possible.

Redis output:
  pit_priors:rolling:7d:{symbol}:{kind}:{session}  HASH  — 7d session-specific
  pit_priors:rolling:7d:{symbol}:{kind}:all         HASH  — 7d cross-session
  pit_priors:rolling:30d:{symbol}:{kind}:all        HASH  — 30d MAE/MFE/giveback

HASH fields (7d):
  winrate, ev_r, ev_r_median, sample_count, sl_hit_rate,
  profit_factor, tp1_hit_rate, ts_ms

HASH fields (30d, additional):
  median_mae_r_winners, p90_mae_r_winners, median_mfe_r,
  giveback_p75, sample_count, ts_ms,
  p50_mae_bps_30d, p75_mae_bps_30d, p90_mae_bps_30d  # all-samples MAE in bps (for bounded SL floor)

ENV
  PIT_ROLLING_INTERVAL_S          (default 3600)
  PIT_ROLLING_EMBARGO_MS          (default 3600000 = 1h)
  PIT_ROLLING_MIN_SAMPLES         (default 20)
  PIT_ROLLING_COLD_START_MIN_SAMPLES (default 10) — symbol:default:all only
  PIT_ROLLING_TTL_SEC             (default 90000 = 25h)
  PIT_ROLLING_MAX_TRADES          (default 200000)
  PIT_ROLLING_PG_BOOTSTRAP        (default 1) — merge trades_closed PG history
  PIT_ROLLING_PG_MAX_ROWS         (default 50000)
  REDIS_URL                       (default redis://redis-worker-1:6379/0)
"""
from __future__ import annotations

import logging
import math
import os
import signal
import time
from collections import defaultdict
from typing import Any

logger = logging.getLogger("pit_priors_rolling")

_EMBARGO_MS: int = int(os.getenv("PIT_ROLLING_EMBARGO_MS", "3_600_000").replace("_", ""))
_MIN_SAMPLES: int = int(os.getenv("PIT_ROLLING_MIN_SAMPLES", "20"))
_COLD_START_MIN_SAMPLES: int = int(os.getenv("PIT_ROLLING_COLD_START_MIN_SAMPLES", "10"))
_TTL_SEC: int = int(os.getenv("PIT_ROLLING_TTL_SEC", "90000"))
_MAX_TRADES: int = int(os.getenv("PIT_ROLLING_MAX_TRADES", "200000"))
_PG_BOOTSTRAP: bool = os.getenv("PIT_ROLLING_PG_BOOTSTRAP", "1").strip().lower() in ("1", "true", "yes", "on")
_PG_MAX_ROWS: int = int(os.getenv("PIT_ROLLING_PG_MAX_ROWS", "50000"))
_7D_MS: int = 7 * 86_400_000
_30D_MS: int = 30 * 86_400_000
_SESS_MAP = {"asian": "asia", "european": "europe", "us_main": "us", "overnight": "asia"}
_ROLLING_SESSIONS = ("asia", "europe", "us", "all")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _f(v: Any) -> float:
    try:
        if v is None:
            return float("nan")
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        r = float(v)
        return r if math.isfinite(r) else float("nan")
    except Exception:
        return float("nan")


def _s(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _session(ts_ms: int) -> str:
    h = (ts_ms // 3_600_000) % 24
    if 13 <= h < 22:
        return "us"
    if 7 <= h < 16:
        return "europe"
    return "asia"


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) * 0.5


def _percentile(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = min(int(len(s) * p / 100.0), len(s) - 1)
    return s[idx]


def _kind_label(t: dict[str, str]) -> str:
    """Align with build_pit_priors_v1 + of_confirm_engine scenario key."""
    return (t.get("scenario") or t.get("kind") or "").strip() or "default"


def _session_label(t: dict[str, str], ts_close: int) -> str:
    sess_raw = (t.get("session") or "").strip().lower()
    if sess_raw:
        return _SESS_MAP.get(sess_raw, "asia")
    return _session(ts_close)


def _seed_symbols_from_env() -> list[str]:
    for env_name in ("PIT_ROLLING_SYMBOLS", "PIT_PRIORS_SYMBOLS", "CANARY_SYMBOLS", "CRYPTO_SYMBOLS"):
        raw = (os.getenv(env_name) or "").strip()
        if not raw:
            continue
        out = [s.strip().upper() for s in raw.replace(";", ",").split(",") if s.strip()]
        if out:
            return out
    return ["BTCUSDT", "ETHUSDT"]


def _rolling_placeholder(now_ms: int) -> dict[str, str]:
    return {
        "winrate": "0.000000",
        "ev_r": "0.000000",
        "ev_r_median": "0.000000",
        "sample_count": "0.000000",
        "sl_hit_rate": "0.000000",
        "profit_factor": "0.000000",
        "tp1_hit_rate": "0.000000",
        "slippage_p95_bps": "0.000000",
        "timeout_rate": "0.000000",
        "tp1_before_timeout_rate": "0.000000",
        "trailing_success_rate": "0.000000",
        "be_stopout_rate": "0.000000",
        "hold_time_p50_ms": "0.000000",
        "hold_time_p90_ms": "0.000000",
        "median_mae_r_winners": "0.000000",
        "p90_mae_r_winners": "0.000000",
        "median_mfe_r": "0.000000",
        "giveback_p75": "0.000000",
        "p50_mae_bps_30d": "0.000000",
        "p75_mae_bps_30d": "0.000000",
        "p90_mae_bps_30d": "0.000000",
        "ts_ms": f"{float(now_ms):.6f}",
    }


def _derive_r_from_bps(bps: float, t: dict[str, str]) -> float:
    """Convert bps excursion to R using one_r_money or sl distance in bps."""
    if not math.isfinite(bps) or bps <= 0.0:
        return float("nan")
    one_r = _f(t.get("one_r_money"))
    entry = _f(t.get("entry_px") or t.get("entry_price"))
    sl = _f(t.get("sl_price") or t.get("sl"))
    if math.isfinite(one_r) and one_r > 1e-9 and math.isfinite(entry) and entry > 0:
        return (bps / 10_000.0) * entry / one_r
    if math.isfinite(entry) and entry > 0 and math.isfinite(sl) and sl > 0:
        risk_bps = abs(entry - sl) / entry * 10_000.0
        if risk_bps > 1e-6:
            return bps / risk_bps
    return float("nan")


def _derive_mfe_r(t: dict[str, str]) -> float:
    raw = t.get("mfe_r") or t.get("max_favorable_r")
    if raw not in (None, ""):
        v = _f(raw)
        if math.isfinite(v):
            return v
    mfe_pnl = _f(t.get("mfe_pnl"))
    one_r = _f(t.get("one_r_money"))
    if math.isfinite(mfe_pnl) and math.isfinite(one_r) and one_r > 1e-9:
        return mfe_pnl / one_r
    return _derive_r_from_bps(_f(t.get("mfe_bps")), t)


def _derive_mae_r(t: dict[str, str]) -> float:
    raw = t.get("mae_r")
    if raw not in (None, ""):
        v = _f(raw)
        if math.isfinite(v):
            return v
    mae_pnl = _f(t.get("mae_pnl"))
    one_r = _f(t.get("one_r_money"))
    if math.isfinite(mae_pnl) and math.isfinite(one_r) and one_r > 1e-9:
        return abs(mae_pnl) / one_r
    return _derive_r_from_bps(_f(t.get("mae_bps")), t)


def _slippage_bps_sample(t: dict[str, str]) -> float:
    for key in ("realized_slippage_bps", "slippage_bps_est", "p0_slippage_bps_est", "is_bps"):
        v = _f(t.get(key))
        if math.isfinite(v) and v >= 0.0:
            return v
    return float("nan")


def _enrich_trade_fields(t: dict[str, str]) -> None:
    """Normalize trades:closed rows for aggregation (in-place)."""
    mfe_r = _derive_mfe_r(t)
    if math.isfinite(mfe_r):
        t["mfe_r"] = f"{mfe_r:.6f}"
    mae_r = _derive_mae_r(t)
    if math.isfinite(mae_r):
        t["mae_r"] = f"{mae_r:.6f}"


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------

def read_trades(redis_client: Any, since_ms: int, now_ms: int) -> list[dict[str, str]]:
    """Read trades:closed [since_ms, now_ms] in batches."""
    from core.redis_keys import RedisStreams as RS
    out: list[dict[str, str]] = []
    last_id = f"{since_ms - 1}-0"
    while len(out) < _MAX_TRADES:
        entries = redis_client.xrange(
            RS.TRADES_CLOSED,
            min=f"({last_id}",
            max=str(now_ms),
            count=5_000,
        )
        if not entries:
            break
        for msg_id, fields in entries:
            out.append({_s(k): _s(v) for k, v in fields.items()})
            last_id = _s(msg_id)
        if len(entries) < 5_000:
            break
    return out


def read_trades_postgres(since_ms: int, now_ms: int) -> list[dict[str, str]]:
    """Bootstrap from trades_closed when Redis stream is thin (cold start)."""
    if not _PG_BOOTSTRAP:
        return []
    try:
        from psycopg2.extras import RealDictCursor
        from services.analytics_db import get_conn
    except Exception as e:
        logger.debug("PG bootstrap unavailable: %s", e)
        return []

    sql = """
        SELECT symbol,
               NULL::text   AS scenario,
               NULL::text   AS session,
               exit_ts_ms,
               r_multiple,
               NULL::text   AS result,
               mfe_pnl      AS mfe_bps,
               mae_pnl      AS mae_bps,
               mfe_pnl,
               mae_pnl,
               one_r_money,
               NULL::double precision AS slippage_bps_est,
               NULL::double precision AS realized_slippage_bps,
               tp1_hit,
               close_reason,
               sid
        FROM trades_closed
        WHERE exit_ts_ms >= %s AND exit_ts_ms < %s
          AND symbol IS NOT NULL
        ORDER BY exit_ts_ms ASC
        LIMIT %s
    """
    out: list[dict[str, str]] = []
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (since_ms, now_ms, _PG_MAX_ROWS))
            for row in cur.fetchall() or []:
                rec = {str(k): _s(v) for k, v in dict(row).items()}
                rec["ts_close"] = rec.get("exit_ts_ms") or rec.get("ts_close") or "0"
                if not (rec.get("result") or "").strip():
                    r = _f(rec.get("r_multiple"))
                    if math.isfinite(r) and r != 0.0:
                        rec["result"] = "WIN" if r > 0 else "LOSS"
                out.append(rec)
    except Exception as e:
        logger.warning("PG bootstrap read failed: %s", e)
    return out


def merge_trades_dedup(
    redis_trades: list[dict[str, str]],
    pg_trades: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Prefer Redis rows; fill gaps from Postgres by sid."""
    seen: set[str] = set()
    merged: list[dict[str, str]] = []
    for t in redis_trades:
        sid = (t.get("sid") or t.get("signal_id") or "").strip()
        if sid:
            seen.add(sid)
        merged.append(t)
    for t in pg_trades:
        sid = (t.get("sid") or t.get("signal_id") or "").strip()
        if sid and sid in seen:
            continue
        merged.append(t)
    return merged


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------

def _tp1_hit(t: dict[str, str]) -> bool:
    v = (t.get("tp1_hit") or "").lower()
    if v in ("1", "true"):
        return True
    reason = (t.get("close_reason") or "").lower()
    return reason.startswith("tp1") or reason == "tp" or reason == "tp_1"


def _is_timeout(t: dict[str, str]) -> bool:
    reason = (t.get("close_reason") or "").lower()
    return reason.startswith("timeout") or reason in ("forced_timeout", "no_followthrough")


def _is_tp1_before_timeout(t: dict[str, str]) -> bool:
    return _tp1_hit(t) and _is_timeout(t)


def _is_trailing_exit(t: dict[str, str]) -> bool:
    reason = (t.get("close_reason") or "").lower()
    return "trail" in reason or reason in ("trail_sl", "trailing_sl", "trail_stop")


def _is_be_stopout(t: dict[str, str]) -> bool:
    reason = (t.get("close_reason") or "").lower()
    return reason.startswith("be") or "breakeven" in reason or reason in ("be_sl", "be_stop")


def _hold_time_ms(t: dict[str, str]) -> float:
    """Hold time in ms from explicit field or exit_ts_ms - open_ts_ms."""
    raw = t.get("hold_time_ms") or t.get("duration_ms")
    if raw not in (None, ""):
        v = _f(raw)
        if math.isfinite(v) and v > 0:
            return v
    exit_ms = _f(t.get("exit_ts_ms") or t.get("ts_close") or t.get("close_ts"))
    open_ms = _f(t.get("open_ts_ms") or t.get("entry_ts_ms"))
    if math.isfinite(exit_ms) and math.isfinite(open_ms) and exit_ms > open_ms:
        return exit_ms - open_ms
    return float("nan")


def _result(t: dict[str, str]) -> str:
    """Derive WIN/LOSS/SKIP from r_multiple + close_reason.

    Live trades:closed uses close_reason ∈ {TP1,TP2,SL,TIMEOUT,...} and
    r_multiple (signed). We treat: r_multiple > 0 → WIN, r_multiple < 0 → LOSS,
    r_multiple == 0 → SKIP (timeout at breakeven).
    """
    r = _f(t.get("r_multiple"))
    if not math.isfinite(r) or r == 0.0:
        return "SKIP"
    return "WIN" if r > 0 else "LOSS"


def compute_rolling_priors(
    trades: list[dict[str, str]],
    now_ms: int,
) -> tuple[
    dict[tuple[str, str, str], dict[str, float]],  # 7d (symbol, kind, session|"all")
    dict[tuple[str, str, str], dict[str, float]],  # 30d (symbol, kind, "all")
]:
    cutoff_7d = now_ms - _7D_MS - _EMBARGO_MS
    cutoff_30d = now_ms - _30D_MS - _EMBARGO_MS
    now_cutoff = now_ms - _EMBARGO_MS

    # Buckets: (symbol, kind, session) → list of trade dicts
    b7: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    b30: dict[tuple[str, str, str], list[dict]] = defaultdict(list)

    for t in trades:
        _enrich_trade_fields(t)
        ts_close = _f(t.get("ts_close") or t.get("close_ts") or t.get("exit_ts_ms"))
        if not math.isfinite(ts_close) or ts_close >= now_cutoff:
            continue
        result = (t.get("result") or "").upper() or _result(t)
        t["result"] = result  # cache derived field for downstream _agg7/_agg30
        if result not in ("WIN", "LOSS"):
            continue
        sym = (t.get("symbol") or "").upper()
        if not sym:
            continue
        kind = _kind_label(t)
        sess = _session_label(t, int(ts_close))

        if cutoff_7d <= ts_close < now_cutoff:
            # Write to scenario-specific bucket
            b7[(sym, kind, sess)].append(t)
            b7[(sym, kind, "all")].append(t)
            # Also write to "default" alias — engine falls back to this when scenario is unknown
            b7[(sym, "default", sess)].append(t)
            b7[(sym, "default", "all")].append(t)
        if cutoff_30d <= ts_close < now_cutoff:
            b30[(sym, kind, "all")].append(t)
            b30[(sym, "default", "all")].append(t)

    def _min_for_key(key: tuple[str, str, str]) -> int:
        sym, kind, sess = key
        if kind == "default" and sess == "all":
            return _COLD_START_MIN_SAMPLES
        return _MIN_SAMPLES

    def _agg7(samples: list[dict], *, min_samples: int) -> dict[str, float] | None:
        n = len(samples)
        if n < min_samples:
            return None
        wins = [s for s in samples if (s.get("result") or "").upper() == "WIN"]
        losses = [s for s in samples if (s.get("result") or "").upper() == "LOSS"]
        r_vals = [_f(s.get("r_multiple")) for s in samples]
        r_vals = [r for r in r_vals if math.isfinite(r)]
        winrate = len(wins) / n
        ev_r = sum(r_vals) / len(r_vals) if r_vals else 0.0
        r_sorted = sorted(r_vals)
        ev_r_median = _median(r_sorted)
        sl_hit = len(losses) / n
        gross_p = sum(_f(s.get("r_multiple")) for s in wins if math.isfinite(_f(s.get("r_multiple"))))
        gross_l = abs(sum(_f(s.get("r_multiple")) for s in losses if math.isfinite(_f(s.get("r_multiple")))))
        pf = gross_p / gross_l if gross_l > 1e-8 else (10.0 if gross_p > 0 else 0.0)
        tp1_hits = sum(1 for s in wins if _tp1_hit(s))
        tp1_rate = tp1_hits / len(wins) if wins else 0.0
        slip_vals = [_slippage_bps_sample(s) for s in samples]
        slip_vals = [v for v in slip_vals if math.isfinite(v)]
        slip_p95 = _percentile(slip_vals, 95) if slip_vals else 0.0
        # P1 #24-25 — timeout / tp1-before-timeout rates
        timeout_count = sum(1 for s in samples if _is_timeout(s))
        tp1_timeout_count = sum(1 for s in samples if _is_tp1_before_timeout(s))
        # P2 Group G — trailing / BE stopout / hold time
        trailing_count = sum(1 for s in wins if _is_trailing_exit(s))
        be_count = sum(1 for s in samples if _is_be_stopout(s))
        hold_times = [_hold_time_ms(s) for s in samples]
        hold_times = [v for v in hold_times if math.isfinite(v) and v > 0]
        return {
            "winrate": winrate,
            "ev_r": ev_r,
            "ev_r_median": ev_r_median,
            "sample_count": float(n),
            "sl_hit_rate": sl_hit,
            "profit_factor": pf,
            "tp1_hit_rate": tp1_rate,
            "slippage_p95_bps": slip_p95,
            "timeout_rate": timeout_count / n,
            "tp1_before_timeout_rate": tp1_timeout_count / n,
            "trailing_success_rate": trailing_count / len(wins) if wins else 0.0,
            "be_stopout_rate": be_count / n,
            "hold_time_p50_ms": _median(hold_times),
            "hold_time_p90_ms": _percentile(hold_times, 90) if hold_times else 0.0,
            "ts_ms": float(now_ms),
        }

    def _agg30(samples: list[dict], *, min_samples: int) -> dict[str, float] | None:
        n = len(samples)
        if n < min_samples:
            return None
        wins = [s for s in samples if (s.get("result") or "").upper() == "WIN"]
        mae_winners = [_f(s.get("mae_r")) for s in wins]
        mae_winners = [v for v in mae_winners if math.isfinite(v)]
        mfe_all = [_f(s.get("mfe_r")) for s in samples]
        mfe_all = [v for v in mfe_all if math.isfinite(v)]
        r_wins = [_f(s.get("r_multiple")) for s in wins]
        r_wins = [v for v in r_wins if math.isfinite(v)]
        mfe_wins = [_f(s.get("mfe_r")) for s in wins]
        mfe_wins = [v for v in mfe_wins if math.isfinite(v)]
        # giveback = mfe - r (how much peak gain was surrendered)
        givebacks = [
            mfe - r
            for mfe, r in zip(mfe_wins, r_wins)
            if math.isfinite(mfe) and math.isfinite(r)
        ]
        # MAE in bps across ALL samples (winners + losers) — used as a floor for
        # bounded SL = max(k*ATR, p75(MAE_30d_bps)). Includes losers because
        # the floor must account for noise that *was* taken out historically.
        mae_bps_all = [_f(s.get("mae_bps")) for s in samples]
        mae_bps_all = [v for v in mae_bps_all if math.isfinite(v) and v >= 0.0]
        return {
            "median_mae_r_winners": _median(mae_winners),
            "p90_mae_r_winners": _percentile(mae_winners, 90),
            "median_mfe_r": _median(mfe_all),
            "giveback_p75": _percentile(givebacks, 75),
            "p50_mae_bps_30d": _median(mae_bps_all),
            "p75_mae_bps_30d": _percentile(mae_bps_all, 75),
            "p90_mae_bps_30d": _percentile(mae_bps_all, 90),
            "sample_count": float(n),
            "ts_ms": float(now_ms),
        }

    out7: dict[tuple[str, str, str], dict[str, float]] = {}
    for key, samples in b7.items():
        agg = _agg7(samples, min_samples=_min_for_key(key))
        if agg is not None:
            out7[key] = agg

    out30: dict[tuple[str, str, str], dict[str, float]] = {}
    for key, samples in b30.items():
        agg = _agg30(samples, min_samples=_min_for_key(key))
        if agg is not None:
            out30[key] = agg

    return out7, out30


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------

def write_rolling_priors(
    redis_client: Any,
    priors_7d: dict[tuple[str, str, str], dict[str, float]],
    priors_30d: dict[tuple[str, str, str], dict[str, float]],
    *,
    ttl_sec: int = _TTL_SEC,
) -> int:
    written = 0
    for (sym, kind, sess), data in priors_7d.items():
        key = f"pit_priors:rolling:7d:{sym}:{kind}:{sess}"
        try:
            redis_client.hset(key, mapping={k: f"{v:.6f}" for k, v in data.items()})
            redis_client.expire(key, ttl_sec)
            written += 1
        except Exception as e:
            logger.warning("HSET %s failed: %s", key, e)
    for (sym, kind, _), data in priors_30d.items():
        key = f"pit_priors:rolling:30d:{sym}:{kind}:all"
        try:
            redis_client.hset(key, mapping={k: f"{v:.6f}" for k, v in data.items()})
            redis_client.expire(key, ttl_sec)
            written += 1
        except Exception as e:
            logger.warning("HSET %s failed: %s", key, e)
    return written


def seed_rolling_placeholders(
    redis_client: Any,
    symbols: list[str],
    *,
    ttl_sec: int = _TTL_SEC,
    now_ms: int | None = None,
) -> int:
    ts_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    payload = _rolling_placeholder(ts_ms)
    written = 0
    for sym in symbols:
        sym = (sym or "").strip().upper()
        if not sym:
            continue
        for sess in _ROLLING_SESSIONS:
            if sess == "all":
                key = f"pit_priors:rolling:30d:{sym}:default:all"
            else:
                key = f"pit_priors:rolling:7d:{sym}:default:{sess}"
            try:
                if redis_client.exists(key):
                    continue
                redis_client.hset(key, mapping=payload)
                redis_client.expire(key, ttl_sec)
                written += 1
            except Exception as e:
                logger.debug("seed placeholder failed for %s: %s", key, e)
    return written


# ---------------------------------------------------------------------------
# service loop
# ---------------------------------------------------------------------------

def run_once(redis_client: Any) -> int:
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - _30D_MS - _EMBARGO_MS - 3_600_000
    trades_redis = read_trades(redis_client, since_ms, now_ms)
    trades_pg = read_trades_postgres(since_ms, now_ms)
    trades = merge_trades_dedup(trades_redis, trades_pg)
    logger.info(
        "Read %d trades for rolling priors (redis=%d pg=%d merged=%d)",
        len(trades), len(trades_redis), len(trades_pg), len(trades),
    )
    if not trades:
        seeded = seed_rolling_placeholders(redis_client, _seed_symbols_from_env(), ttl_sec=_TTL_SEC, now_ms=now_ms)
        logger.info("No trades for rolling priors; seeded %d placeholder hashes", seeded)
        return seeded
    p7, p30 = compute_rolling_priors(trades, now_ms)
    written = write_rolling_priors(redis_client, p7, p30)
    seeded = seed_rolling_placeholders(redis_client, _seed_symbols_from_env(), ttl_sec=_TTL_SEC, now_ms=now_ms)
    logger.info(
        "Written %d rolling prior hashes (7d=%d buckets, 30d=%d buckets, seeded=%d)",
        written, len(p7), len(p30), seeded,
    )
    return written + seeded


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    interval_s = int(os.getenv("PIT_ROLLING_INTERVAL_S", "3600"))

    from core.redis_client import get_redis
    redis_client = get_redis()

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "pit_priors_rolling_v1 starting (interval=%ds embargo=%dms min_samples=%d cold_start=%d pg_bootstrap=%s)",
        interval_s, _EMBARGO_MS, _MIN_SAMPLES, _COLD_START_MIN_SAMPLES, _PG_BOOTSTRAP,
    )

    while not stop["flag"]:
        try:
            run_once(redis_client)
        except Exception as e:
            logger.error("run_once failed: %s", e)
        for _ in range(interval_s):
            if stop["flag"]:
                break
            time.sleep(1)

    logger.info("pit_priors_rolling_v1 stopped")


if __name__ == "__main__":
    main()
