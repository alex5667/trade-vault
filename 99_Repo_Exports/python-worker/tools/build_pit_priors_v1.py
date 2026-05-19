#!/usr/bin/env python3
"""build_pit_priors_v1.py — ADR-0007 skeleton.

Point-in-time materializer for historical priors. Reads `trades:closed`
history and builds per-{symbol, kind, session} prior aggregates that are
SAFE for training (no look-ahead leakage).

Embargo rule: aggregates at time T include ONLY trades that closed at
ts_close < T - EMBARGO_MS. The embargo eliminates outcome-echo from
recent same-symbol trades that may still be referenced by signals at T.

Output:
  Redis hash `pit_priors:{symbol}:{kind}:{session}:{as_of_date}`
  fields:
    winrate, ev_r, sample_count, oldest_ts_ms, newest_ts_ms

  Latest pointer:
  Redis string `pit_priors:latest:{symbol}:{kind}:{session}` → as_of_date

STATUS: SKELETON. Aggregation logic implemented; backfill window + emit
contract are fixed. Production use requires:
  - Replay-safe time partition (build daily, never include same-day trades)
  - Purged CV + embargo wired into train_ml_scorer.py loop
  - Leakage unit test (test_pit_priors_no_leakage_v1.py)

See /home/alex/Apps/Obsidian/trade-vault/80_Research/ADR-0007 PIT Historical Priors.md

USAGE
  python -m tools.build_pit_priors_v1 \\
      --start-ts-ms 1715000000000 \\
      --end-ts-ms   1716000000000 \\
      --embargo-ms 3600000

ENV
  PIT_PRIOR_MIN_SAMPLES   (default 30)
  PIT_PRIOR_STALE_MS      (default 86400000 — 24h)
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from collections import defaultdict
from typing import Any

from core.redis_client import get_redis
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger("build_pit_priors")

PIT_PRIOR_MIN_SAMPLES = int(os.getenv("PIT_PRIOR_MIN_SAMPLES", "30"))


def _session_bucket(ts_ms: int) -> str:
    h = (ts_ms // 3_600_000) % 24
    if 13 <= h < 22:
        return "us"
    if 7 <= h < 16:
        return "europe"
    return "asia"


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return float("nan")
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        f = float(v)
        return f if math.isfinite(f) else float("nan")
    except Exception:
        return float("nan")


def read_closed_trades(
    redis_client: Any,
    start_ts_ms: int,
    end_ts_ms: int,
    *,
    batch_size: int = 10_000,
) -> list[dict[str, Any]]:
    """Read all trades:closed between [start, end]."""
    out: list[dict[str, Any]] = []
    last_id = f"{start_ts_ms - 1}-0"
    while True:
        entries = redis_client.xrange(RS.TRADES_CLOSED, min=f"({last_id}", max=str(end_ts_ms), count=batch_size)
        if not entries:
            break
        for msg_id, fields in entries:
            fields = {_decode(k): _decode(v) for k, v in fields.items()}
            ts_close = _safe_float(fields.get("ts_close") or fields.get("close_ts"))
            if math.isfinite(ts_close) and start_ts_ms <= ts_close <= end_ts_ms:
                out.append(fields)
            last_id = _decode(msg_id)
        if len(entries) < batch_size:
            break
    return out


def build_pit_priors(
    trades: list[dict[str, Any]],
    *,
    as_of_ts_ms: int,
    embargo_ms: int,
) -> dict[tuple[str, str, str], dict[str, float]]:
    """Aggregate priors by (symbol, kind, session) using only trades that closed
    before `as_of_ts_ms - embargo_ms`. Returns dict keyed by tuple."""
    cutoff_ms = as_of_ts_ms - embargo_ms
    buckets: dict[tuple[str, str, str], list[tuple[float, float, int]]] = defaultdict(list)
    # entry: (is_win [0/1], r_multiple, ts_close)

    for t in trades:
        ts_close = _safe_float(t.get("ts_close") or t.get("close_ts"))
        if not math.isfinite(ts_close) or ts_close >= cutoff_ms:
            continue
        result = (t.get("result") or "").upper()
        if result not in ("WIN", "LOSS"):
            continue  # BE excluded for binary win-rate
        r_mult = _safe_float(t.get("r_multiple"))
        if not math.isfinite(r_mult):
            r_mult = 1.0 if result == "WIN" else -1.0
        symbol = (t.get("symbol") or "").upper()
        kind = t.get("kind") or t.get("scenario") or "default"
        ts_decision = _safe_float(t.get("ts_decision"))
        session = _session_bucket(int(ts_decision if math.isfinite(ts_decision) else ts_close))
        win = 1 if result == "WIN" else 0
        buckets[(symbol, kind, session)].append((float(win), r_mult, int(ts_close)))

    out: dict[tuple[str, str, str], dict[str, float]] = {}
    for key, samples in buckets.items():
        n = len(samples)
        if n == 0:
            continue
        wins = [s for s in samples if s[0] == 1.0]
        losses = [s for s in samples if s[0] == 0.0]
        winrate = len(wins) / n
        ev_r = sum(s[1] for s in samples) / n
        oldest = min(s[2] for s in samples)
        newest = max(s[2] for s in samples)
        sl_hit_rate = len(losses) / n  # = 1 - winrate, explicit for training

        # profit_factor: gross_profit / gross_loss (>1 = positive expectancy)
        gross_profit = sum(s[1] for s in wins)
        gross_loss = abs(sum(s[1] for s in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 1e-8 else (10.0 if gross_profit > 0 else 0.0)

        # r_multiple spread stats (robust; model sees consistency)
        r_vals = [s[1] for s in samples]
        r_mean = ev_r
        r_var = sum((r - r_mean) ** 2 for r in r_vals) / n
        r_std = r_var ** 0.5
        r_sorted = sorted(r_vals)
        ev_r_median = r_sorted[n // 2]

        out[key] = {
            "winrate": winrate,
            "ev_r": ev_r,
            "ev_r_median": ev_r_median,
            "sample_count": float(n),
            "oldest_ts_ms": float(oldest),
            "newest_ts_ms": float(newest),
            "sl_hit_rate": sl_hit_rate,
            "profit_factor": profit_factor,
            "r_std": r_std,
        }
    return out


def write_priors_to_redis(
    redis_client: Any,
    priors: dict[tuple[str, str, str], dict[str, float]],
    as_of_ts_ms: int,
    *,
    ttl_sec: int = 86400 * 90,
) -> int:
    """Persist priors. Returns number of hashes written."""
    as_of_date = _date_str(as_of_ts_ms)
    written = 0
    for (symbol, kind, session), data in priors.items():
        if data["sample_count"] < PIT_PRIOR_MIN_SAMPLES:
            continue  # cold-start guard
        redis_key = f"pit_priors:{symbol}:{kind}:{session}:{as_of_date}"
        try:
            redis_client.hset(redis_key, mapping={k: f"{v:.6f}" for k, v in data.items()})
            redis_client.expire(redis_key, ttl_sec)
            # Update latest pointer
            redis_client.set(
                f"pit_priors:latest:{symbol}:{kind}:{session}",
                as_of_date,
                ex=ttl_sec,
            )
            written += 1
        except Exception as e:
            logger.warning("HSET pit_priors failed for %s: %s", redis_key, e)
    return written


def _date_str(ts_ms: int) -> str:
    import datetime as _dt
    dt = _dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=_dt.timezone.utc)
    return dt.strftime("%Y%m%d")


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(description="Build point-in-time priors from trades:closed")
    p.add_argument("--start-ts-ms", type=int, required=True)
    p.add_argument("--end-ts-ms", type=int, required=True)
    p.add_argument("--embargo-ms", type=int, default=3_600_000, help="Cutoff before as-of date")
    p.add_argument("--ttl-sec", type=int, default=86400 * 90)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    redis_client = get_redis()
    logger.info(
        "Reading trades:closed window [%d, %d] (embargo=%d ms)",
        args.start_ts_ms, args.end_ts_ms, args.embargo_ms,
    )
    trades = read_closed_trades(redis_client, args.start_ts_ms, args.end_ts_ms)
    logger.info("Read %d trades", len(trades))

    if not trades:
        logger.warning("No trades found in window — nothing to materialize")
        return 0

    priors = build_pit_priors(trades, as_of_ts_ms=args.end_ts_ms, embargo_ms=args.embargo_ms)
    logger.info(
        "Built %d prior buckets (>= %d samples each filtered before write)",
        len(priors), PIT_PRIOR_MIN_SAMPLES,
    )

    if args.dry_run:
        for key, data in sorted(priors.items()):
            logger.info("DRY %s -> %s", "/".join(key), {k: f"{v:.4f}" for k, v in data.items()})
        return 0

    written = write_priors_to_redis(redis_client, priors, args.end_ts_ms, ttl_sec=args.ttl_sec)
    logger.info("Wrote %d prior hashes to Redis (as_of=%s)", written, _date_str(args.end_ts_ms))
    return 0


if __name__ == "__main__":
    sys.exit(main())
