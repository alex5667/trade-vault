"""Nightly meta ENFORCE ramp proposal: progressive share increase (0.10→0.25→0.50→1.00).

Checks safety gates (streak + no recent emergency) and proposes next share level
in the schedule if current share is below max.

Usage:
  python -m tools.nightly_meta_enforce_ramp_bundle
  (reads ENV vars for schedule, thresholds, symbols)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import secrets
import time
import hmac
import hashlib
from typing import Dict, List, Tuple

import redis

from common.log import setup_logger
from tools import ml_calculate_recent_metrics

logger = setup_logger("NightlyMetaEnforceRamp")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bid: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def main() -> None:
    """Main entry point: check gates, determine next share, propose bundle."""
    ap = argparse.ArgumentParser(description="Nightly meta ENFORCE ramp proposal")
    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--schedule", default=os.getenv("META_ENFORCE_SHARE_SCHEDULE", "0.10,0.25,0.50,1.00"))
    ap.add_argument("--min-streak", type=int, default=int(os.getenv("META_ENFORCE_MIN_STREAK", "3") or 3))
    ap.add_argument("--min-hours-since-emerg", type=float, default=float(os.getenv("META_ENFORCE_RAMP_MIN_HOURS_SINCE_LAST_EMERG", "24") or 24))
    ap.add_argument("--notify-on-skip", type=int, default=int(os.getenv("META_ENFORCE_RAMP_NOTIFY_ON_SKIP", "0") or 0))
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
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                "type": "report",
                "text": f"<b>Meta ENFORCE ramp skipped</b>\nreason=<code>streak_gate</code>\nstreak=<code>{streak}</code> need=<code>{args.min_streak}</code> last=<code>{last_status}</code>",
                "ts": str(now_ms()),
            }, maxlen=200000, approximate=True)
        return

    # Gate 2: no recent emergency
    emerg_key = os.getenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    try:
        last_em = int(r.get(emerg_key) or "0")
    except Exception:
        last_em = 0
    if last_em > 0:
        min_ms = int(args.min_hours_since_emerg * 3600_000)
        if (now_ms() - last_em) < min_ms:
            if args.notify_on_skip == 1:
                r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                    "type": "report",
                    "text": f"<b>Meta ENFORCE ramp skipped</b>\nreason=<code>recent_emergency</code>\nlast_em_ms=<code>{last_em}</code>",
                    "ts": str(now_ms()),
                }, maxlen=200000, approximate=True)
            return

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not syms:
        return

    # Gate 3: Metrics check (Precision & Calibration)
    # We only block ramp-up if metrics are bad. We don't drop share here (that's for emergency guard).
    try:
        stats = ml_calculate_recent_metrics.calculate(window_hours=24, top_k_pct=0.05)
        
        # Hard-coded gates from policy
        GATE_PRECISION = 0.55
        GATE_ECE = 0.05
        
        if not stats.get("insufficient_data"):
            prec = float(stats.get("precision_top_k", 0.0))
            ece = float(stats.get("ece", 1.0))
            
            if prec < GATE_PRECISION:
                logger.warning(f"Ramp Halted: Low Precision {prec:.2f} < {GATE_PRECISION}")
                if args.notify_on_skip == 1:
                    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                        "type": "report",
                        "text": f"<b>Meta ENFORCE ramp skipped</b>\nreason=<code>low_precision</code>\nval=<code>{prec:.2f}</code> gate=<code>{GATE_PRECISION}</code>",
                        "ts": str(now_ms()),
                    }, maxlen=200000, approximate=True)
                return

            # Check ECE only if we are already at significant share (> 10%)
            # because at low share we might not have enough execution data, 
            # though here we use paper-trading labels so it should be fine.
            # Let's enforce ECE always for safety.
            if ece > GATE_ECE:
                logger.warning(f"Ramp Halted: Poor Calibration {ece:.3f} > {GATE_ECE}")
                if args.notify_on_skip == 1:
                    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                        "type": "report",
                        "text": f"<b>Meta ENFORCE ramp skipped</b>\nreason=<code>poor_calibration</code>\nval=<code>{ece:.3f}</code> gate=<code>{GATE_ECE}</code>",
                        "ts": str(now_ms()),
                    }, maxlen=200000, approximate=True)
                return
                
        else:
             logger.info("Metrics check skipped: insufficient data")

    except Exception as e:
        logger.error(f"Metrics check failed: {e}", exc_info=True)
        # Fail safe: if metrics calc fails, do we halt? 
        # Yes, safety first.
        if args.notify_on_skip == 1:
             r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                "type": "report",
                "text": f"<b>Meta ENFORCE ramp skipped</b>\nreason=<code>metrics_error</code>\nerr=<code>{str(e)[:50]}</code>",
                "ts": str(now_ms()),
            }, maxlen=200000, approximate=True)
        return

    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    # determine current share (min across symbols)
    cur = 1.0
    for sym in syms:
        v = r.hget(f"{prefix}{sym}", "meta_enforce_share")
        try:
            cur = min(cur, float(v)) if v is not None else min(cur, 0.0)
        except Exception:
            cur = min(cur, 0.0)

    sched = []
    for x in args.schedule.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            sched.append(float(x))
        except Exception:
            pass
    sched = sorted(set([max(0.0, min(1.0, s)) for s in sched]))

    # next share
    nxt = None
    for s in sched:
        if s > cur + 1e-9:
            nxt = s
            break
    if nxt is None:
        return  # already at max

    # create bundle to update meta_enforce_share (keep ENFORCE mode)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    ops = []
    for sym in syms:
        ops += [
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_enable", "value": "1"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_mode", "value": "ENFORCE"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share", "value": f"{nxt:.2f}"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_salt", "value": str(os.getenv("META_ENFORCE_SALT", "enf_v1"))},
        ]

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_enforce_ramp_bundle",
        "ops": ops,
        "meta": {"from_share": cur, "to_share": nxt, "symbols": syms, "streak": streak},
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    msg = (
        "<b>Meta ENFORCE ramp proposal</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"symbols=<code>{','.join(syms)}</code>\n"
        f"share: <code>{cur:.2f}</code> → <code>{nxt:.2f}</code>\n"
        f"streak=<code>{streak}</code>"
    )
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
        "type": "report",
        "text": msg,
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
        "ts": str(now_ms()),
    }, maxlen=200000, approximate=True)

    logger.info("Meta ENFORCE ramp proposal created: bundle_id=%s, share %s -> %s", bundle_id, cur, nxt)


if __name__ == "__main__":
    main()

