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
  PIT_ROLLING_TTL_SEC             (default 90000 = 25h)
  PIT_ROLLING_MAX_TRADES          (default 200000)
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
_TTL_SEC: int = int(os.getenv("PIT_ROLLING_TTL_SEC", "90000"))
_MAX_TRADES: int = int(os.getenv("PIT_ROLLING_MAX_TRADES", "200000"))
_7D_MS: int = 7 * 86_400_000
_30D_MS: int = 30 * 86_400_000


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


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------

def _tp1_hit(t: dict[str, str]) -> bool:
    v = (t.get("tp1_hit") or "").lower()
    if v in ("1", "true"):
        return True
    reason = (t.get("close_reason") or "").lower()
    return reason.startswith("tp1") or reason == "tp" or reason == "tp_1"


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

    _SESS_MAP = {"asian": "asia", "european": "europe", "us_main": "us", "overnight": "asia"}

    for t in trades:
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
        # Engine's _kind_label = signal.scenario (continuation/na/range_meanrev).
        # trades:closed.scenario matches. "kind" is a per-pipeline tag — ignore.
        kind = (t.get("scenario") or "").strip() or "default"
        # Engine's _session_label ∈ {asia,europe,us}. trades:closed uses asian/european/us_main/overnight.
        sess_raw = (t.get("session") or "").strip().lower()
        sess = _SESS_MAP.get(sess_raw, "asia")

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

    def _agg7(samples: list[dict]) -> dict[str, float] | None:
        n = len(samples)
        if n < _MIN_SAMPLES:
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
        return {
            "winrate": winrate,
            "ev_r": ev_r,
            "ev_r_median": ev_r_median,
            "sample_count": float(n),
            "sl_hit_rate": sl_hit,
            "profit_factor": pf,
            "tp1_hit_rate": tp1_rate,
            "ts_ms": float(now_ms),
        }

    def _agg30(samples: list[dict]) -> dict[str, float] | None:
        n = len(samples)
        if n < _MIN_SAMPLES:
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
        agg = _agg7(samples)
        if agg is not None:
            out7[key] = agg

    out30: dict[tuple[str, str, str], dict[str, float]] = {}
    for key, samples in b30.items():
        agg = _agg30(samples)
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


# ---------------------------------------------------------------------------
# service loop
# ---------------------------------------------------------------------------

def run_once(redis_client: Any) -> int:
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - _30D_MS - _EMBARGO_MS - 3_600_000
    trades = read_trades(redis_client, since_ms, now_ms)
    logger.info("Read %d trades from trades:closed (30d window)", len(trades))
    if not trades:
        return 0
    p7, p30 = compute_rolling_priors(trades, now_ms)
    written = write_rolling_priors(redis_client, p7, p30)
    logger.info(
        "Written %d rolling prior hashes (7d=%d buckets, 30d=%d buckets)",
        written, len(p7), len(p30),
    )
    return written


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

    logger.info("pit_priors_rolling_v1 starting (interval=%ds embargo=%dms min_samples=%d)",
                interval_s, _EMBARGO_MS, _MIN_SAMPLES)

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
