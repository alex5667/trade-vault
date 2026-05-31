from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Nightly meta ENFORCE ramp proposal with stratified worst-case DiD guard.

Checks safety gates (streak + no recent emergency), evaluates stratified DiD
on outcomes, and proposes next share level only if worst-case passes.

Usage:
  python -m tools.nightly_meta_enforce_ramp_guarded_bundle
  (reads ENV vars for schedule, thresholds, symbols, DiD params)
"""

import argparse
import hashlib
import hmac
import json
import os
import secrets
import subprocess
import sys
import time

import redis

from common.log import setup_logger
from utils.time_utils import get_ny_time_millis

logger = setup_logger("NightlyMetaEnforceRampGuarded")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bid: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def main() -> None:
    """Main entry point: check gates, evaluate DiD, propose bundle if safe."""
    ap = argparse.ArgumentParser(description="Nightly meta ENFORCE ramp proposal with stratified DiD guard")
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

    # Gate 1: regress PASS streak
    streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
    max_age_h = float(os.getenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30") or 30.0)

    try:
        streak = int(r.get(streak_key) or "0")
    except Exception:
        streak = 0
    last_status = (r.get(last_status_key) or "")
    try:
        last_ts = int(r.get(last_ts_key) or "0")
    except Exception:
        last_ts = 0

    age_ok = True
    if last_ts > 0:
        age_ok = (now_ms() - last_ts) <= int(max_age_h * 3600_000)

    if not (last_status == "PASS" and age_ok and streak >= args.min_streak):
        if args.notify_on_skip == 1:
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": (
                    "<b>Meta ramp skipped</b>\n"
                    "reason=<code>streak_gate</code>\n"
                    f"streak=<code>{streak}</code> need=<code>{args.min_streak}</code> last=<code>{last_status}</code>"
                ),
                "ts": str(now_ms()),
            }, maxlen=50000, approximate=True)
        return

    # Gate 2: no recent emergency
    emerg_key = os.getenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    try:
        last_em = int(r.get(emerg_key) or "0")
    except Exception:
        last_em = 0
    min_hours = float(os.getenv("META_ENFORCE_RAMP_MIN_HOURS_SINCE_LAST_EMERG", "24") or 24)
    if last_em > 0 and (now_ms() - last_em) < int(min_hours * 3600_000):
        if args.notify_on_skip == 1:
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": "<b>Meta ramp skipped</b>\nreason=<code>recent_emergency</code>",
                "ts": str(now_ms()),
            }, maxlen=200000, approximate=True)
        return

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not syms:
        return

    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")

    # Current shares (we use per-regime keys if enabled)
    use_per_regime = int(os.getenv("META_ENFORCE_PER_REGIME", "0") or 0) == 1
    if use_per_regime:
        share_fields = ["meta_enforce_share_trend", "meta_enforce_share_range"]
        cur = 1.0
        for sym in syms:
            for f in share_fields:
                v = r.hget(f"{prefix}{sym}", f)
                try:
                    cur = min(cur, float(v)) if v is not None else min(cur, 0.0)
                except Exception:
                    cur = min(cur, 0.0)
    else:
        cur = 1.0
        for sym in syms:
            v = r.hget(f"{prefix}{sym}", "meta_enforce_share")
            try:
                cur = min(cur, float(v)) if v is not None else min(cur, 0.0)
            except Exception:
                cur = min(cur, 0.0)

    sched = sorted(set([max(0.0, min(1.0, float(x))) for x in args.schedule.split(",") if x.strip()]))
    nxt = None
    for s in sched:
        if s > cur + 1e-9:
            nxt = s
            break
    if nxt is None:
        return

    # Need last ramp timestamp for DiD
    ramp_ts_key = os.getenv("META_RAMP_LAST_APPLIED_MS_KEY", "meta:ramp:last_applied_ms")
    try:
        ramp_ts = int(r.get(ramp_ts_key) or "0")
    except Exception:
        ramp_ts = 0
    if ramp_ts <= 0:
        if args.notify_on_skip == 1:
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": (
                    "<b>Meta ramp blocked</b>\n"
                    "reason=<code>missing_ramp_ts</code>\n"
                    f"need=<code>{ramp_ts_key}</code>"
                ),
                "ts": str(now_ms()),
            }, maxlen=50000, approximate=True)
        return

    # Export trades (must cover before+after windows)
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.out_dir}/meta_ramp_strat_{ts}"
    os.makedirs(run_dir, exist_ok=True)

    trades_out = f"{run_dir}/trades.ndjson"
    eval_out = f"{run_dir}/eval_strat.json"

    trades_stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    export_hours = max(args.since_hours, 2.0 * args.window_hours + 12.0)

    logger.info("Exporting trades from stream=%s, since_hours=%.1f", trades_stream, export_hours)
    subprocess.check_call([
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(export_hours),
        "--out", trades_out,
        "--stream", trades_stream,
        "--redis-url", redis_url,
        "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
    ])

    # Stratified DiD evaluation (worst-case over symbol×regime_bucket)
    min_n_per_cell = int(os.getenv("META_RAMP_MIN_N_PER_CELL", "120") or 120)
    min_cells = int(os.getenv("META_RAMP_MIN_CELLS", "3") or 3)
    after_tail_cap = float(os.getenv("META_RAMP_AFTER_TAIL_ENF_MAX", "0.18") or 0.18)
    did_tail_p95_max = float(os.getenv("META_RAMP_DID_TAIL_P95_MAX", "0.0") or 0.0)
    did_mean_p05_min = float(os.getenv("META_RAMP_DID_MEAN_P05_MIN", "-0.03") or -0.03)

    logger.info("Evaluating stratified DiD: ramp_ts=%d, window_hours=%.1f", ramp_ts, args.window_hours)
    subprocess.check_call([
        sys.executable, "-m", "tools.eval_meta_ramp_outcomes_did_stratified",
        "--trades", trades_out,
        "--out", eval_out,
        "--symbols", ",".join(syms),
        "--ramp-ts-ms", str(ramp_ts),
        "--window-hours", str(args.window_hours),
        "--min-n-per-cell", str(min_n_per_cell),
        "--min-cells", str(min_cells),
        "--after_tail_enf_max", str(after_tail_cap),
        "--did_tail_p95_max", str(did_tail_p95_max),
        "--did_mean_p05_min", str(did_mean_p05_min),
    ])

    rep = json.loads(open(eval_out, encoding="utf-8").read())
    dec = rep.get("decision") or {}
    if not dec.get("ok_to_ramp", False):
        if args.notify_on_skip == 1:
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": (
                    "<b>Meta ramp blocked (stratified worst-case)</b>\n"
                    f"from=<code>{cur:.2f}</code> to=<code>{nxt:.2f}</code>\n"
                    f"decision=<code>{dec}</code>\n"
                    f"failed_top=<code>{rep.get('failed_top')}</code>\n"
                    f"skipped_top=<code>{rep.get('skipped_top')}</code>"
                ),
                "ts": str(now_ms()),
            }, maxlen=200000, approximate=True)
        return

    # Create bundle for share update
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    salt = os.getenv("META_ENFORCE_SALT", "enf_v1")

    ops = []
    for sym in syms:
        ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_enable", "value": "1"})
        ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_mode", "value": "ENFORCE"})
        ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_salt", "value": salt})

        if use_per_regime:
            # Keep news always 0.00 (fixed)
            ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share_news", "value": "0.00"})
            ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share_trend", "value": f"{nxt:.2f}"})
            ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share_range", "value": f"{nxt:.2f}"})
            ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share_other", "value": "0.00"})
        else:
            ops.append({"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share", "value": f"{nxt:.2f}"})

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_enforce_ramp_guarded_bundle",
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

    msg = (
        "<b>Meta ENFORCE ramp proposal (worst-case stratified DiD)</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"symbols=<code>{','.join(syms)}</code>\n"
        f"share: <code>{cur:.2f}</code> → <code>{nxt:.2f}</code>\n"
        f"cells_eval=<code>{rep.get('evaluated_cells')}</code> failed=<code>{rep.get('failed_cells')}</code> skipped=<code>{rep.get('skipped_cells')}</code>\n"
        f"use_per_regime=<code>{int(use_per_regime)}</code>"
    )

    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
        "type": "report",
        "text": msg,
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
        "ts": str(now_ms()),
    }, maxlen=200000, approximate=True)

    logger.info("Meta ENFORCE ramp proposal created: bundle_id=%s, share %s -> %s", bundle_id, cur, nxt)


if __name__ == "__main__":
    main()
