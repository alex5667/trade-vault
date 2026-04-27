#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nightly_meta_unfreeze_cells_bundle.py

Auto-unfreeze proposal: если клетка SYMBOL|bucket была заморожена, и 7 дней по ней "хорошая статистика" → предложить bundle на восстановление share (ручной Confirm в Telegram).

Flow:
  1. Читает freeze registry (meta:freeze:cells)
  2. Берёт клетки старше UNFREEZE_MIN_DAYS=7
  3. Экспортирует events:trades за 7–10 дней
  4. Проверяет по каждой клетке (symbol×bucket) enforce vs control:
     - tail_enf <= cap
     - tail_improve >= min
     - mean_delta >= -0.02
     - bootstrap: tail_delta_p95 < 0
  5. Если проходит — создаёт bundle на восстановление share до meta:ramp:last_share (или per-bucket meta:ramp:last_share_trend/range)

Usage:
  python -m tools.nightly_meta_unfreeze_cells_bundle
  (reads ENV vars for thresholds, symbols, bootstrap params)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import hmac
import hashlib
import random
from typing import Any, Dict, List, Tuple

import redis

from common.log import setup_logger

logger = setup_logger("NightlyMetaUnfreezeCells")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bid: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _event_ts_ms(r: Dict[str, Any]) -> int:
    """Extract timestamp in milliseconds from trade record."""
    for k in ("ts_ms", "ts", "exit_ts_ms", "event_ts_ms"):
        if k in r:
            v = r.get(k)
            try:
                vv = int(float(v))
                if vv > 10_000_000_000:
                    return vv
                if 1_000_000_000 < vv < 10_000_000_000:
                    return vv * 1000
            except Exception:
                pass
    return 0


def regime_bucket(r: Dict[str, Any]) -> str:
    """
    Classify regime bucket from trade record.
    
    Prefer explicit regime_group; fallback to regime; then scenario_v4.
    """
    g = str(r.get("regime_group", "") or r.get("regime", "") or r.get("scenario_v4", "") or "")
    s = g.lower()
    if "news" in s:
        return "news"
    if "trend" in s or "bull" in s or "bear" in s:
        return "trend"
    from common.market_mode import is_range_regime; _r = is_range_regime(s)
    if _r:
        return "range"
    if "thin" in s or "illiquid" in s:
        return "thin"
    return "other"


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return int(d)


def pctl(xs: List[float], q: float) -> float:
    """Calculate percentile from sorted list."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def stats(rs: List[float]) -> Dict[str, float]:
    """Calculate basic statistics for returns list."""
    n = len(rs)
    if n == 0:
        return {"n": 0.0}
    mean = sum(rs) / n
    tail = sum(1 for x in rs if x <= -1.0) / n
    return {"n": float(n), "meanR": float(mean), "tail_rate": float(tail), "medianR": float(pctl(rs, 0.5))}


def bootstrap_tail_delta(enf: List[float], ctl: List[float], iters: int, seed: int) -> Dict[str, float]:
    """
    Bootstrap tail rate delta (enf - ctl) to get confidence interval.
    
    Returns dict with ok, tail_delta_p50, tail_delta_p95.
    """
    rng = random.Random(seed)
    if len(enf) < 30 or len(ctl) < 30:
        return {"ok": 0.0}

    def samp_tail(xs: List[float]) -> float:
        c = 0
        for _ in range(len(xs)):
            if xs[rng.randrange(0, len(xs))] <= -1.0:
                c += 1
        return c / len(xs)

    deltas = []
    for _ in range(iters):
        deltas.append(samp_tail(enf) - samp_tail(ctl))
    deltas.sort()
    return {
        "ok": 1.0,
        "tail_delta_p50": float(deltas[int(0.50 * (iters - 1))]),
        "tail_delta_p95": float(deltas[int(0.95 * (iters - 1))]),
    }


def main() -> None:
    """Main entry point: check eligible frozen cells, evaluate stats, propose unfreeze bundle."""
    ap = argparse.ArgumentParser(description="Nightly meta unfreeze proposal (7d good stats)")
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"))
    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # params
    min_days = int(os.getenv("META_UNFREEZE_MIN_DAYS", "7") or 7)
    since_hours = float(os.getenv("META_UNFREEZE_EXPORT_HOURS", str(min_days * 24 + 24)) or (min_days * 24 + 24))
    min_enf_n = int(os.getenv("META_UNFREEZE_MIN_ENF_N", "50") or 50)
    min_ctl_n = int(os.getenv("META_UNFREEZE_MIN_CTL_N", "50") or 50)

    tail_cap = float(os.getenv("META_UNFREEZE_TAIL_ENF_MAX", "0.18") or 0.18)
    tail_improve_min = float(os.getenv("META_UNFREEZE_TAIL_IMPROVE_MIN", "0.01") or 0.01)
    mean_delta_min = float(os.getenv("META_UNFREEZE_MEAN_DELTA_MIN", "-0.02") or -0.02)

    iters = int(os.getenv("META_UNFREEZE_BOOT_ITERS", "800") or 800)
    seed = int(os.getenv("META_UNFREEZE_SEED", "42") or 42)

    registry = os.getenv("META_FREEZE_REGISTRY_KEY", "meta:freeze:cells")
    raw = r.hgetall(registry) or {}
    if not raw:
        logger.info("No frozen cells in registry")
        return

    # build eligible cells
    eligible = []
    cutoff_ms = now_ms() - int(min_days * 24 * 3600_000)
    for ck, v in raw.items():
        try:
            rec = json.loads(v)
        except Exception:
            continue
        applied_ms = int(rec.get("applied_ms", 0) or 0)
        if applied_ms and applied_ms <= cutoff_ms:
            eligible.append(rec)

    if not eligible:
        logger.info(f"No eligible cells (need {min_days} days old)")
        return

    logger.info(f"Found {len(eligible)} eligible frozen cells")

    # export trades once
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.out_dir}/meta_unfreeze_{ts}"
    os.makedirs(run_dir, exist_ok=True)
    trades_out = f"{run_dir}/trades.ndjson"

    trades_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    logger.info(f"Exporting trades from {trades_stream} (since {since_hours}h)")
    subprocess.check_call([
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(since_hours),
        "--out", trades_out,
        "--stream", trades_stream,
        "--redis-url", redis_url,
        "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
    ])

    # load trades into memory filtered to needed symbols
    sym_set = {s.strip().upper() for s in (args.symbols or "").split(",") if s.strip()}
    trades = []
    with open(trades_out, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            t = json.loads(line)
            sym = str(t.get("symbol", "") or "").upper()
            if sym_set and sym not in sym_set:
                continue
            trades.append(t)

    logger.info(f"Loaded {len(trades)} trades for evaluation")

    # decide which cells to unfreeze
    to_unfreeze = []
    reasons_map = {}

    # restore share reference
    last_share = float(r.get(os.getenv("META_RAMP_LAST_SHARE_KEY", "meta:ramp:last_share")) or 0.0)
    last_share_trend = float(r.get("meta:ramp:last_share_trend") or last_share)
    last_share_range = float(r.get("meta:ramp:last_share_range") or last_share)

    for rec in eligible:
        sym = str(rec.get("symbol", "") or "").upper()
        bucket = str(rec.get("bucket", "") or "").lower()
        ck = f"{sym}|{bucket}"

        enf = []
        ctl = []
        for t in trades:
            if str(t.get("symbol", "") or "").upper() != sym:
                continue
            if regime_bucket(t) != bucket:
                continue
            if t.get("meta_enforce_applied", None) is None:
                continue
            rm = t.get("r_mult", None)
            if rm is None:
                continue
            if _i(t.get("meta_enforce_applied", 0), 0) == 1:
                enf.append(_f(rm, 0.0))
            else:
                ctl.append(_f(rm, 0.0))

        if len(enf) < min_enf_n or len(ctl) < min_ctl_n:
            reasons_map[ck] = f"insufficient_n(enf={len(enf)},ctl={len(ctl)})"
            continue

        se = stats(enf)
        sc = stats(ctl)
        mean_delta = se["meanR"] - sc["meanR"]
        tail_improve = sc["tail_rate"] - se["tail_rate"]
        ci = bootstrap_tail_delta(enf, ctl, iters=iters, seed=seed)

        reasons = []
        if se["tail_rate"] > tail_cap:
            reasons.append(f"tail_cap({se['tail_rate']:.2f}>{tail_cap:.2f})")
        if tail_improve < tail_improve_min:
            reasons.append(f"tail_improve({tail_improve:.3f}<{tail_improve_min:.3f})")
        if mean_delta < mean_delta_min:
            reasons.append(f"mean_delta({mean_delta:.3f}<{mean_delta_min:.3f})")
        if ci.get("ok", 0.0) != 1.0 or float(ci.get("tail_delta_p95", 0.0)) >= 0.0:
            reasons.append("tail_ci_not_strict")

        if reasons:
            reasons_map[ck] = ",".join(reasons)
            continue

        # compute restore target
        if bucket == "trend":
            restore = last_share_trend
        elif bucket == "range":
            restore = last_share_range
        else:
            restore = last_share

        # fallback to prev_share if last_share missing
        if restore <= 0.0:
            try:
                restore = float(rec.get("prev_share") or 0.0)
            except Exception:
                restore = 0.0
        restore = max(0.0, min(1.0, restore))

        # only unfreeze if restore is meaningfully higher than current frozen value
        try:
            frozen_to = float(rec.get("freeze_to") or 0.0)
        except Exception:
            frozen_to = 0.0
        if restore <= frozen_to + 1e-9:
            reasons_map[ck] = f"restore_not_higher(restore={restore:.2f},frozen={frozen_to:.2f})"
            continue

        to_unfreeze.append({"cell": ck, "symbol": sym, "bucket": bucket, "restore_to": restore})

    if not to_unfreeze:
        logger.info(f"No cells eligible for unfreeze (checked {len(eligible)} cells)")
        if reasons_map:
            logger.debug(f"Reasons: {reasons_map}")
        return

    logger.info(f"Found {len(to_unfreeze)} cells eligible for unfreeze")

    # build bundle (manual approve)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    ops = []
    cells = []
    max_cells = int(os.getenv("META_UNFREEZE_MAX_CELLS", "5") or 5)
    for x in to_unfreeze[:max_cells]:
        sym = x["symbol"]
        bucket = x["bucket"]
        restore = x["restore_to"]
        hk = f"{prefix}{sym}"

        # restore only that bucket (per-regime setup)
        ops.append({"op": "HSET", "key": hk, "field": f"meta_enforce_share_{bucket}", "value": f"{restore:.2f}"})
        cells.append(f"{sym}|{bucket}")

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_unfreeze_cells_bundle",
        "ops": ops,
        "meta": {
            "kind": "meta_enforce_unfreeze_cells",
            "cells": cells,
            "restore": {c: next(x["restore_to"] for x in to_unfreeze if x["cell"] == c) for c in cells},
            "reasons_skipped": reasons_map,
        },
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    msg = (
        "<b>Meta UNFREEZE proposal</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"cells=<code>{cells}</code>\n"
        f"min_days=<code>{min_days}</code> window_hours=<code>{since_hours:.0f}</code>\n"
        f"note=<code>restore uses meta:ramp:last_share(_trend/_range)</code>"
    )
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
        "type": "report",
        "text": msg,
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
        "ts": str(now_ms()),
    }, maxlen=200000, approximate=True)

    logger.info(f"Unfreeze bundle proposed: {bundle_id}, cells={cells}")


if __name__ == "__main__":
    main()

