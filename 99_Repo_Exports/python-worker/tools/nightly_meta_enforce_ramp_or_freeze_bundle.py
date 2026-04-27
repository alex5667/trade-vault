#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nightly_meta_enforce_ramp_or_freeze_bundle.py

Nightly meta ENFORCE ramp-or-freeze proposal with stratified worst-case DiD guard.

Flow:
  1. Check gates (streak PASS, no recent emergency, has meta:ramp:last_applied_ms)
  2. Export trades NDJSON (covers before+after windows)
  3. Evaluate outcomes using stratified DiD (symbol × regime_bucket)
  4. If OK → propose ramp bundle
  5. If worst_case_failed → propose freeze bundle for failed cells
  6. Otherwise → skip

Freeze bundle sets meta_enforce_share_<bucket>=0.00 for specific symbol×bucket cells
that failed DiD gates, allowing ramp to continue on next run after freeze is applied.

Usage:
  python -m tools.nightly_meta_enforce_ramp_or_freeze_bundle
  (reads ENV vars for schedule, thresholds, symbols, DiD params, freeze policy)
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
from typing import Dict, List, Tuple

import redis

from common.log import setup_logger

logger = setup_logger("NightlyMetaEnforceRampOrFreeze")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bid: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _notify(r: redis.Redis, text: str, buttons: List[List[Dict[str, str]]] | None = None) -> None:
    """Send notification to Telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def _read_float_h(r: redis.Redis, key: str, field: str, default: float = 0.0) -> float:
    """Read float value from Redis hash field."""
    v = r.hget(key, field)
    try:
        return float(v) if v is not None else float(default)
    except Exception:
        return float(default)


def main() -> None:
    """Main entry point: check gates, evaluate DiD, propose ramp or freeze bundle."""
    ap = argparse.ArgumentParser(description="Nightly meta ENFORCE ramp-or-freeze proposal with stratified DiD guard")
    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--schedule", default=os.getenv("META_ENFORCE_SHARE_SCHEDULE", "0.10,0.25,0.50,1.00"))
    ap.add_argument("--window-hours", type=float, default=float(os.getenv("META_RAMP_DID_WINDOW_HOURS", "72") or 72))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("META_RAMP_EXPORT_HOURS", "180") or 180))
    ap.add_argument("--min-streak", type=int, default=int(os.getenv("META_ENFORCE_MIN_STREAK", "3") or 3))
    ap.add_argument("--notify-on-skip", type=int, default=int(os.getenv("META_ENFORCE_RAMP_NOTIFY_ON_SKIP", "1") or 1))
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not syms:
        logger.warning("No symbols provided, skipping")
        return

    # ---------------- Gates ----------------
    streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
    max_age_h = float(os.getenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30") or 30.0)

    try:
        streak = int(r.get(streak_key) or "0")
    except Exception:
        streak = 0
    last_status = str(r.get(last_status_key) or "")
    try:
        last_ts = int(r.get(last_ts_key) or "0")
    except Exception:
        last_ts = 0
    age_ok = True
    if last_ts > 0:
        age_ok = (now_ms() - last_ts) <= int(max_age_h * 3600_000)

    if not (last_status == "PASS" and age_ok and streak >= args.min_streak):
        if args.notify_on_skip == 1:
            _notify(r, f"<b>Meta ramp/freeze skipped</b>\nreason=<code>streak_gate</code>\nstreak=<code>{streak}</code> need=<code>{args.min_streak}</code> last=<code>{last_status}</code>")
        logger.info(f"Streak gate failed: streak={streak}, need={args.min_streak}, last_status={last_status}")
        return

    emerg_key = os.getenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    try:
        last_em = int(r.get(emerg_key) or "0")
    except Exception:
        last_em = 0
    min_hours = float(os.getenv("META_ENFORCE_RAMP_MIN_HOURS_SINCE_LAST_EMERG", "24") or 24)
    if last_em > 0 and (now_ms() - last_em) < int(min_hours * 3600_000):
        if args.notify_on_skip == 1:
            _notify(r, "<b>Meta ramp/freeze skipped</b>\nreason=<code>recent_emergency</code>")
        logger.info(f"Recent emergency gate failed: last_em={last_em}, min_hours={min_hours}")
        return

    ramp_ts_key = os.getenv("META_RAMP_LAST_APPLIED_MS_KEY", "meta:ramp:last_applied_ms")
    try:
        ramp_ts = int(r.get(ramp_ts_key) or "0")
    except Exception:
        ramp_ts = 0
    if ramp_ts <= 0:
        if args.notify_on_skip == 1:
            _notify(r, f"<b>Meta ramp/freeze blocked</b>\nreason=<code>missing_ramp_ts</code>\nneed=<code>{ramp_ts_key}</code>")
        logger.warning(f"Missing ramp timestamp: {ramp_ts_key}")
        return

    # ---------------- Determine current share + next share ----------------
    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    use_per_regime = int(os.getenv("META_ENFORCE_PER_REGIME", "1") or 1) == 1

    if use_per_regime:
        # Read per-regime shares (trend/range for ramp, news always 0)
        fields = ["meta_enforce_share_trend", "meta_enforce_share_range"]
        cur = 1.0
        for sym in syms:
            hk = f"{prefix}{sym}"
            for f in fields:
                cur = min(cur, _read_float_h(r, hk, f, 0.0))
    else:
        # Legacy: single meta_enforce_share
        cur = 1.0
        for sym in syms:
            cur = min(cur, _read_float_h(r, f"{prefix}{sym}", "meta_enforce_share", 0.0))

    sched = sorted(set([max(0.0, min(1.0, float(x))) for x in args.schedule.split(",") if x.strip()]))
    nxt = None
    for s in sched:
        if s > cur + 1e-9:
            nxt = s
            break
    if nxt is None:
        logger.info(f"Already at max share: {cur}")
        return  # already max

    # ---------------- Export trades ----------------
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.out_dir}/meta_ramp_or_freeze_{ts}"
    os.makedirs(run_dir, exist_ok=True)

    trades_out = f"{run_dir}/trades.ndjson"
    eval_out = f"{run_dir}/eval_strat.json"

    trades_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    export_hours = max(args.since_hours, 2.0 * args.window_hours + 12.0)

    logger.info(f"Exporting trades from {trades_stream} (since {export_hours}h)")
    subprocess.check_call([
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(export_hours),
        "--out", trades_out,
        "--stream", trades_stream,
        "--redis-url", redis_url,
        "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
    ])

    # ---------------- Stratified DiD evaluation ----------------
    min_n_per_cell = int(os.getenv("META_RAMP_MIN_N_PER_CELL", "120") or 120)
    min_cells = int(os.getenv("META_RAMP_MIN_CELLS", "3") or 3)
    after_tail_cap = float(os.getenv("META_RAMP_AFTER_TAIL_ENF_MAX", "0.18") or 0.18)
    did_tail_p95_max = float(os.getenv("META_RAMP_DID_TAIL_P95_MAX", "0.0") or 0.0)
    did_mean_p05_min = float(os.getenv("META_RAMP_DID_MEAN_P05_MIN", "-0.03") or -0.03)

    logger.info(f"Running stratified DiD evaluation: ramp_ts={ramp_ts}, window={args.window_hours}h")
    subprocess.check_call([
        sys.executable, "-m", "tools.eval_meta_ramp_outcomes_did_stratified",
        "--trades", trades_out,
        "--out", eval_out,
        "--symbols", ",".join(syms),
        "--ramp-ts-ms", str(ramp_ts),
        "--window-hours", str(args.window_hours),
        "--min-n-per-cell", str(min_n_per_cell),
        "--min-cells", str(min_cells),
        "--after-tail-enf-max", str(after_tail_cap),
        "--did-tail-p95-max", str(did_tail_p95_max),
        "--did-mean-p05-min", str(did_mean_p05_min),
    ])

    rep = json.loads(open(eval_out, "r", encoding="utf-8").read())
    dec = rep.get("decision") or {}

    # ---------------- If OK -> propose ramp bundle ----------------
    if dec.get("ok_to_ramp", False):
        secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
        ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
        bundle_id = secrets.token_hex(6)
        sig = sign(bundle_id, secret)

        salt = str(os.getenv("META_ENFORCE_SALT", "enf_v1"))
        ops = []
        for sym in syms:
            hk = f"{prefix}{sym}"
            ops += [
                {"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"},
                {"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"},
                {"op": "HSET", "key": hk, "field": "meta_enforce_salt", "value": salt},
            ]
            if use_per_regime:
                ops += [
                    {"op": "HSET", "key": hk, "field": "meta_enforce_share_news", "value": "0.00"},
                    {"op": "HSET", "key": hk, "field": "meta_enforce_share_trend", "value": f"{nxt:.2f}"},
                    {"op": "HSET", "key": hk, "field": "meta_enforce_share_range", "value": f"{nxt:.2f}"},
                    {"op": "HSET", "key": hk, "field": "meta_enforce_share_other", "value": "0.00"},
                ]
            else:
                ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share", "value": f"{nxt:.2f}"})

        bundle = {
            "id": bundle_id,
            "created_ms": now_ms(),
            "ttl_sec": ttl,
            "who": "nightly_meta_enforce_ramp_or_freeze_bundle",
            "ops": ops,
            "meta": {
                "kind": "meta_enforce_ramp",
                "from_share": cur,
                "to_share": nxt,
                "symbols": syms,
                "streak": streak,
                "strat_eval": rep,
                "use_per_regime": use_per_regime,
            },
        }
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
        r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

        buttons = [[
            {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
        ]]
        _notify(
            r,
            f"<b>Meta ENFORCE ramp proposal (OK)</b>\n"
            f"id=<code>{bundle_id}</code>\n"
            f"share: <code>{cur:.2f}</code> → <code>{nxt:.2f}</code>\n"
            f"syms=<code>{','.join(syms)}</code>\n"
            f"eval_cells=<code>{rep.get('evaluated_cells')}</code>",
            buttons=buttons
        )
        logger.info(f"Ramp bundle proposed: {bundle_id}, {cur:.2f} → {nxt:.2f}")
        return

    # ---------------- If worst_case_failed -> propose freeze bundle ----------------
    if dec.get("reason") == "worst_case_failed":
        # Freeze floor: min(cur, 0.05) instead of 0.00
        floor = float(os.getenv("META_FREEZE_FLOOR", "0.05") or 0.05)
        max_freeze_cells = int(os.getenv("META_FREEZE_MAX_CELLS", "3") or 3)
        failed_top = rep.get("failed_top") or []
        if not failed_top:
            if args.notify_on_skip == 1:
                _notify(r, f"<b>Meta ramp blocked</b>\nreason=<code>worst_case_failed</code>\n(no failed_top)")
            logger.warning("worst_case_failed but no failed_top in eval report")
            return

        # Extract cells like "BTCUSDT|trend"
        cells = []
        for x in failed_top:
            ck = str(x.get("cell", "") or "")
            if ck and ck not in cells:
                cells.append(ck)
            if len(cells) >= max_freeze_cells:
                break

        if not cells:
            if args.notify_on_skip == 1:
                _notify(r, f"<b>Meta ramp blocked</b>\nreason=<code>worst_case_failed</code>\n(no valid cells to freeze)")
            logger.warning("worst_case_failed but no valid cells extracted")
            return

        # Read current share for each cell and compute freeze_to = min(cur, floor)
        # Skip cells where freeze_to >= cur (freeze would be ineffective)
        cells_to_freeze = []
        for ck in cells:
            parts = ck.split("|")
            if len(parts) != 2:
                logger.warning(f"Invalid cell format: {ck}, skipping")
                continue
            sym, bucket = parts[0].upper(), parts[1].lower()
            hk = f"{prefix}{sym}"

            # Read current share for this cell
            if use_per_regime:
                cur_cell = _read_float_h(r, hk, f"meta_enforce_share_{bucket}", 0.0)
            else:
                cur_cell = _read_float_h(r, hk, "meta_enforce_share", 0.0)

            freeze_to = min(cur_cell, floor)
            # если cur уже <= floor, freeze бессмысленен — можно не предлагать
            if freeze_to >= cur_cell - 1e-9:
                if args.notify_on_skip == 1:
                    _notify(r, f"<b>Freeze skipped</b>\nreason=<code>cur<=floor</code> cur=<code>{cur_cell:.2f}</code> floor=<code>{floor:.2f}</code>")
                logger.info(f"Freeze skipped for {ck}: cur={cur_cell:.2f} <= floor={floor:.2f}")
                continue

            cells_to_freeze.append((ck, sym, bucket, freeze_to))

        if not cells_to_freeze:
            if args.notify_on_skip == 1:
                _notify(r, f"<b>Meta ramp blocked</b>\nreason=<code>worst_case_failed</code>\n(all cells already <= floor)")
            logger.warning("worst_case_failed but all cells already <= floor")
            return

        # Build freeze ops per cell
        secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
        ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
        bundle_id = secrets.token_hex(6)
        sig = sign(bundle_id, secret)

        ops = []
        freeze_cells = []
        for ck, sym, bucket, freeze_to in cells_to_freeze:
            hk = f"{prefix}{sym}"

            ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
            ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
            ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_salt", "value": str(os.getenv("META_ENFORCE_SALT", "enf_v1"))})

            if use_per_regime:
                # freeze only this bucket
                ops.append({"op": "HSET", "key": hk, "field": f"meta_enforce_share_{bucket}", "value": f"{freeze_to:.2f}"})
                # keep news off always
                ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share_news", "value": "0.00"})
            else:
                # without per-regime: freeze whole symbol
                ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_share", "value": f"{freeze_to:.2f}"})
            
            freeze_cells.append(ck)

        if not ops:
            if args.notify_on_skip == 1:
                _notify(r, f"<b>Meta ramp blocked</b>\nreason=<code>worst_case_failed</code>\n(no ops generated)")
            logger.warning("worst_case_failed but no ops generated")
            return

        # Use first freeze_to for meta (all should be similar, but we store per-cell in registry)
        first_freeze_to = cells_to_freeze[0][3] if cells_to_freeze else floor
        bundle = {
            "id": bundle_id,
            "created_ms": now_ms(),
            "ttl_sec": ttl,
            "who": "nightly_meta_enforce_ramp_or_freeze_bundle",
            "ops": ops,
            "meta": {
                "kind": "meta_enforce_freeze_cells",
                "freeze_to": first_freeze_to,  # representative value (actual per-cell stored in registry)
                "cells": freeze_cells,
                "symbols": syms,
                "from_share": cur,
                "attempt_to_share": nxt,
                "strat_eval": rep,
                "use_per_regime": use_per_regime,
            },
        }
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
        r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

        buttons = [[
            {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
        ]]
        _notify(
            r,
            "<b>Meta ramp blocked → propose FREEZE</b>\n"
            f"id=<code>{bundle_id}</code>\n"
            f"freeze_to=<code>{first_freeze_to:.2f}</code> (floor=<code>{floor:.2f}</code>)\n"
            f"cells=<code>{freeze_cells}</code>\n"
            f"blocked_ramp: <code>{cur:.2f}</code> → <code>{nxt:.2f}</code>\n"
            f"failed_cells=<code>{rep.get('failed_cells')}</code> evaluated=<code>{rep.get('evaluated_cells')}</code>",
            buttons=buttons,
        )
        logger.info(f"Freeze bundle proposed: {bundle_id}, cells={freeze_cells}, freeze_to={first_freeze_to:.2f} (floor={floor:.2f})")
        return

    # ---------------- Otherwise: skip with optional notify ----------------
    reason = dec.get("reason") or "unknown"
    if args.notify_on_skip == 1:
        _notify(r, f"<b>Meta ramp skipped</b>\nreason=<code>{reason}</code>\ncur=<code>{cur:.2f}</code> nxt=<code>{nxt:.2f}</code>")
    logger.info(f"Ramp skipped: reason={reason}, cur={cur:.2f}, nxt={nxt:.2f}")


if __name__ == "__main__":
    main()

