#!/usr/bin/env python3
from __future__ import annotations
"""
CrossVenue Gate Calibrator.

Evaluates CrossVenue Gate shadow performance over --hours:
  - Reads decisions:final stream → extracts crossvenue_flags, mode
  - Joins with trades:closed stream by sid → gets outcome (r_mult)
  - Computes hypothetical metrics if the mode was "veto":
    - saved_r (sum of -r_mult for vetoed trades)
    - veto_win_rate (how many vetoed trades were actually winners?)

If all thresholds are met (saved_r > MIN_SAVED_R, win_rate < MAX_VETO_WINRATE)
→ proposes cfg:crypto_of:crossvenue_ctx_profile=tighten or hard
via interactive Telegram (✅/❌).
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hmac
import hashlib
import json
import logging
import os
import secrets
import sys
import time
from typing import Any, Dict, List, Tuple, Optional

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("crossvenue_calibrator")

# ─────────────────────────────────────────────────────── helpers ──────────── #

def _now_ms() -> int:
    return get_ny_time_millis()

def _get_redis_url() -> str:
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    if "redis-worker-1" in url and not os.path.exists("/.dockerenv"):
        url = "redis://localhost:6379/0"
    return url

def _get_redis() -> redis.Redis:
    return redis.Redis.from_url(_get_redis_url(), decode_responses=True)

def _sign(bundle_id: str, secret: str) -> str:
    return hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]

def _f(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def _i(v, default: int = 0) -> int:
    try:
        return int(float(v)) if v is not None else default
    except Exception:
        return default

# ────────────────────────────────────────── stream readers ────────────────── #

def _read_stream_since(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    start_id = f"{since_ms}-0"
    page = 500
    last_id = start_id

    while len(results) < max_scan:
        batch = r.xrange(stream, min=last_id, count=page)
        if not batch:
            break
        for msg_id, fields in batch:
            results.append(fields)
        last_id_raw = batch[-1][0]
        last_ts, last_seq = last_id_raw.split("-")
        last_id = f"{last_ts}-{int(last_seq) + 1}"
        if len(batch) < page:
            break

    return results

def _read_stream_recent(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    last_id = "+"
    page = 500

    while len(results) < max_scan:
        batch = r.xrevrange(stream, max=last_id, count=page)
        if not batch:
            break
        for msg_id, fields in batch:
            ts_ms = _i(fields.get("ts_ms") or fields.get("closed_ms") or fields.get("ts"), 0)
            if ts_ms and ts_ms < since_ms:
                return results
            results.append(fields)
        last_id_raw = batch[-1][0]
        ts_part, seq_part = last_id_raw.split("-")
        last_id = f"{ts_part}-{max(0, int(seq_part) - 1)}"
        if len(batch) < page:
            break

    return results

# ─────────────────────────────────────── metrics collection ───────────────── #

_ADVERSE_FLAGS = frozenset({
    "venue_direction_disagree",
    "venue_dislocation",
    "venue_mid_spread_wide",
    "trade_imbalance_against_long",
    "trade_imbalance_against_short",
})

def _extract_cv_flags(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload_str = ev.get("payload", "")
    if payload_str:
        try:
            rec = json.loads(payload_str)
        except Exception:
            rec = ev
    else:
        rec = ev

    raw_indicators = rec.get("indicators")
    if isinstance(raw_indicators, str):
        try:
            indicators = json.loads(raw_indicators)
        except Exception:
            indicators = {}
    elif isinstance(raw_indicators, dict):
        indicators = raw_indicators
    else:
        indicators = {}

    flags_str = str(indicators.get("crossvenue_flags") or "").strip()
    if not flags_str:
        return None

    stale_count = int(indicators.get("venue_stale_count", 0))

    flags = [f.strip() for f in flags_str.split(",") if f.strip()]
    adverse = [f for f in flags if f in _ADVERSE_FLAGS]

    return {
        "sid": str(rec.get("sid") or rec.get("signal_id") or "").strip(),
        "flags": flags,
        "adverse_count": len(adverse),
        "stale_count": stale_count,
    }


def _collect_analytics(
    r: redis.Redis,
    hours: float,
    max_scan: int,
) -> Dict[str, float]:
    decisions_stream = os.getenv("DECISIONS_FINAL_STREAM", "decisions:final")
    trades_stream = os.getenv("ML_OUTCOME_STREAM", "trades:closed")

    since_ms = _now_ms() - int(hours * 3600 * 1000)
    logger.info(f"Reading {decisions_stream} since {hours}h ago | max_scan={max_scan}")

    decision_events = _read_stream_since(r, decisions_stream, since_ms, max_scan)
    trades_events = _read_stream_recent(r, trades_stream, since_ms, max_scan)

    trades_by_sid: Dict[str, Dict[str, str]] = {}
    for ev in trades_events:
        sid = str(ev.get("sid") or ev.get("signal_id") or "").strip()
        if sid and sid not in trades_by_sid:
            trades_by_sid[sid] = ev

    total_decisions = len(decision_events)
    cv_flagged_count = 0
    vetoed_count = 0
    vetoed_winners = 0
    saved_r = 0.0

    for ev in decision_events:
        cv_data = _extract_cv_flags(ev)
        if cv_data is None:
            continue

        cv_flagged_count += 1
        sid = cv_data["sid"]

        trade = trades_by_sid.get(sid)
        if not trade:
            continue

        r_mult = _f(trade.get("r_mult") or trade.get("pnl_r") or trade.get("pnl"))

        # Hypothetical VETO condition:
        # 1. len(adverse_flags) >= 2
        # 2. stale_count <= 1 (default max_stale_count)
        if cv_data["adverse_count"] >= 2 and cv_data["stale_count"] <= 1:
            vetoed_count += 1
            if r_mult > 0:
                vetoed_winners += 1
            # If r_mult < 0 (loss), vetoing it saves money (+). If r_mult > 0 (win), vetoing it loses money (-).
            saved_r -= r_mult

    vetoed_winrate = (vetoed_winners / vetoed_count) if vetoed_count > 0 else 0.0

    return {
        "total_decisions": total_decisions,
        "cv_flagged_count": cv_flagged_count,
        "vetoed_count": vetoed_count,
        "vetoed_winrate": vetoed_winrate,
        "saved_r": saved_r,
    }


def _holddown_ok(r: redis.Redis, step_ts_key: str, holddown_h: float) -> Tuple[bool, float]:
    raw = r.get(step_ts_key)
    if not raw:
        return True, 999.0
    try:
        last_ms = int(float(raw))
        elapsed_h = (_now_ms() - last_ms) / 3_600_000
        return elapsed_h >= holddown_h, elapsed_h
    except Exception:
        return True, 999.0

def _load_current_mode(r: redis.Redis, cfg_key: str) -> str:
    raw = r.get(cfg_key)
    if raw:
        return str(raw).strip().lower()
    return os.getenv("CROSSVENUE_CTX_PROFILE", "monitor").strip().lower()

def _build_proposal_bundle(
    cfg_key: str,
    secret: str,
    next_mode: str,
    ttl: int = 86400,
) -> Tuple[str, str, Dict[str, Any]]:
    bid = secrets.token_hex(6)
    sig = _sign(bid, secret)
    ts = _now_ms()

    ops = [{
        "op": "SET",
        "key": cfg_key,
        "value": next_mode,
    }]
    bundle = {
        "id": bid,
        "created_ms": ts,
        "ttl_sec": ttl,
        "who": "crossvenue_gate_calibrator",
        "ops": ops,
        "meta": {"kind": "crossvenue_mode_promote"},
    }
    return bid, sig, bundle

def _send_telegram_proposal(
    r: redis.Redis,
    *,
    bundle_id: str,
    sig: str,
    hours: float,
    stats: Dict[str, float],
    notify_stream: str,
    cur_mode: str,
    next_mode: str,
    is_reminder: bool = False,
) -> None:
    vetoed_count = int(stats["vetoed_count"])
    vetoed_winrate = stats["vetoed_winrate"]
    saved_r = stats["saved_r"]

    reminder_tag = "\n⏰ <i>Напоминание — ожидается ваше решение</i>\n" if is_reminder else ""

    text = (
        f"<b>🌍 CrossVenue Gate Calibrator</b>{reminder_tag}\n\n"
        f"За последние <b>{int(hours)}ч</b> shadow-режим выявил:\n"
        f"  • Теоретически заблокировано сделок: <b>{vetoed_count}</b>\n"
        f"  • WinRate среди заблокированных: <b>{vetoed_winrate:.1%}</b>\n"
        f"  • Спасенный профит (Saved R): <b>{saved_r:+.2f} R</b>\n\n"
        f"Предлагаю: <code>CROSSVENUE_CTX_PROFILE</code>\n<b>{cur_mode} → {next_mode}</b>\n"
        f"(Фильтр начнет применять штрафы к confidence или блокировать сделки)"
    )

    buttons = [[
        {"text": "✅ Применить", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
        {"text": "❌ Отклонить", "callback_data": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    r.xadd(notify_stream, {
        "type": "report",
        "subtype": "crossvenue_calibrator",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
    }, maxlen=50000)
    tag = "REMINDER" if is_reminder else "NEW"
    logger.info(f"Telegram proposal [{tag}]: bundle_id={bundle_id} {cur_mode}→{next_mode}")

def _should_propose(
    *,
    stats: Dict[str, float],
    holddown_ok: bool,
    min_vetoed: int,
    max_winrate: float,
    min_saved_r: float,
) -> Tuple[bool, str]:
    if not holddown_ok:
        return False, "holddown_not_expired"

    if stats["vetoed_count"] < min_vetoed:
        return False, f"vetoed={stats['vetoed_count']} < min={min_vetoed}"

    if stats["vetoed_winrate"] > max_winrate:
        return False, f"winrate={stats['vetoed_winrate']:.2f} > max={max_winrate:.2f}"

    if stats["saved_r"] < min_saved_r:
        return False, f"saved_r={stats['saved_r']:.2f} < min={min_saved_r:.2f}"

    return True, "ok"

def _wait_for_decision(
    r: redis.Redis,
    *,
    bundle_id: str,
    sig: str,
    hours: float,
    stats: Dict[str, float],
    notify_stream: str,
    reminder_sec: int,
    step_ts_key: str,
    pending_key: str,
    bundle_ttl: int,
    cur_mode: str,
    next_mode: str,
) -> None:
    max_reminders = max(1, bundle_ttl // max(1, reminder_sec))
    logger.info(
        f"Entering reminder loop: check every {reminder_sec}s, "
        f"max {max_reminders} reminders, bundle_ttl={bundle_ttl}s"
    )

    for i in range(max_reminders):
        time.sleep(reminder_sec)

        status = r.get(f"recs:status:{bundle_id}")
        logger.info(f"Reminder check #{i+1}: bundle={bundle_id} status={status}")

        if status is None:
            logger.warning(f"Bundle {bundle_id} expired (TTL). Exiting reminder loop.")
            r.delete(pending_key)
            return

        if status == "APPLIED":
            r.set(step_ts_key, str(_now_ms()))
            logger.info(f"Bundle {bundle_id} APPLIED. Holddown timestamp updated.")
            _send_decision_notification(
                r,
                notify_stream=notify_stream,
                decision="approved",
                bundle_id=bundle_id,
                cur_mode=cur_mode,
                next_mode=next_mode,
            )
            r.delete(pending_key)
            return

        if status in ("REJECTED", "ROLLED_BACK"):
            logger.info(f"Bundle {bundle_id} {status}. Exiting reminder loop.")
            _send_decision_notification(
                r,
                notify_stream=notify_stream,
                decision="rejected",
                bundle_id=bundle_id,
                cur_mode=cur_mode,
                next_mode=next_mode,
            )
            r.delete(pending_key)
            return

        logger.info(f"Bundle {bundle_id} still {status}. Sending reminder #{i+1}...")
        _send_telegram_proposal(
            r,
            bundle_id=bundle_id,
            sig=sig,
            hours=hours,
            stats=stats,
            notify_stream=notify_stream,
            cur_mode=cur_mode,
            next_mode=next_mode,
            is_reminder=True,
        )

    logger.warning(f"Max reminders ({max_reminders}) exhausted for bundle {bundle_id}. Exiting.")
    r.delete(pending_key)

def _send_decision_notification(
    r: redis.Redis,
    *,
    notify_stream: str,
    decision: str,
    bundle_id: str,
    cur_mode: str,
    next_mode: str,
) -> None:
    if decision == "approved":
        text = (
            f"<b>✅ CrossVenue Gate — режим обновлён</b>\n\n"
            f"<code>CROSSVENUE_CTX_PROFILE</code>: <b>{cur_mode} → {next_mode}</b>\n\n"
            f"Calibrator следующий раз проверит стабильность через ≥72ч.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    else:
        text = (
            f"<b>❌ CrossVenue Gate — предложение отклонено</b>\n\n"
            f"<code>CROSSVENUE_CTX_PROFILE</code> остаётся <b>{cur_mode}</b>\n"
            f"Calibrator предложит снова в следующем цикле\n"
            f"(если пороги по-прежнему выполнены).\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )

    r.xadd(notify_stream, {
        "type": "report",
        "subtype": "crossvenue_calibrator_decision",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
    }, maxlen=50000)
    logger.info(f"Decision notification sent: {decision} bundle={bundle_id}")

def main() -> None:
    ap = argparse.ArgumentParser(description="CrossVenue Gate Calibrator")
    ap.add_argument(
        "--hours", type=float,
        default=float(os.getenv("CVCAL_HOURS", "168")),
        help="Lookback window in hours (default 168 = 7 days)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Compute metrics but do not write bundle or send Telegram",
    )
    ap.add_argument(
        "--force-propose", action="store_true",
        help="Skip threshold checks and always propose enforce",
    )
    ap.add_argument(
        "--min-vetoed", type=int,
        default=int(os.getenv("CVCAL_MIN_VETOED", "10")),
        help="Minimum theoretically vetoed trades to consider proposing",
    )
    ap.add_argument(
        "--max-winrate", type=float,
        default=float(os.getenv("CVCAL_MAX_WINRATE", "0.35")),
        help="Maximum winrate among vetoed trades",
    )
    ap.add_argument(
        "--min-saved-r", type=float,
        default=float(os.getenv("CVCAL_MIN_SAVED_R", "5.0")),
        help="Minimum saved R from theoretical vetoes",
    )
    ap.add_argument(
        "--max-scan", type=int,
        default=int(os.getenv("CVCAL_MAX_SCAN", "500000")),
        help="Maximum stream entries to scan",
    )
    args = ap.parse_args()

    cfg_key = "cfg:crypto_of:crossvenue_ctx_profile"
    pending_key = "meta:crossvenue_cal:pending"
    step_ts_key = "meta:crossvenue_cal:last_step_ms"
    holddown_h = float(os.getenv("CVCAL_HOLDDOWN_H", "72"))
    
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_ttl = int(os.getenv("CVCAL_BUNDLE_TTL_SEC", "86400"))
    reminder_sec = int(os.getenv("CVCAL_REMINDER_SEC", "1800"))

    logger.info(
        f"CrossVenue Gate Calibrator | hours={args.hours} "
        f"min_vetoed={args.min_vetoed} max_winrate={args.max_winrate} "
        f"min_saved_r={args.min_saved_r} max_scan={args.max_scan} dry_run={args.dry_run}"
    )

    r = _get_redis()

    if not args.dry_run and not args.force_propose:
        if r.exists(pending_key):
            logger.info(f"Pending proposal already exists ({pending_key}). Skipping.")
            sys.exit(0)

    cur_mode = _load_current_mode(r, cfg_key)
    logger.info(f"Current CROSSVENUE_CTX_PROFILE: {cur_mode}")

    if cur_mode == "hard":
        logger.info("CROSSVENUE_CTX_PROFILE already 'hard'. Nothing to promote.")
        sys.exit(0)

    # Determine next mode (monitor -> tighten -> hard)
    next_mode = "tighten" if cur_mode in ("monitor", "default", "soft") else "hard"

    hd_ok, elapsed_h = _holddown_ok(r, step_ts_key, holddown_h)
    logger.info(f"Holddown: elapsed={elapsed_h:.1f}h required={holddown_h}h ok={hd_ok}")

    stats = _collect_analytics(r, args.hours, args.max_scan)

    logger.info(
        f"Stats over {args.hours}h:\n"
        f"  total_decisions      = {int(stats['total_decisions'])}\n"
        f"  cv_flagged_count     = {int(stats['cv_flagged_count'])}\n"
        f"  vetoed_count         = {int(stats['vetoed_count'])}\n"
        f"  vetoed_winrate       = {stats['vetoed_winrate']:.1%}\n"
        f"  saved_r              = {stats['saved_r']:+.2f}"
    )

    if args.force_propose:
        should_go = True
        reason = "force_propose"
        logger.info("Forcing proposal (--force-propose)")
    else:
        should_go, reason = _should_propose(
            stats=stats,
            holddown_ok=hd_ok,
            min_vetoed=args.min_vetoed,
            max_winrate=args.max_winrate,
            min_saved_r=args.min_saved_r,
        )

    logger.info(f"Decision: should_propose={should_go} reason={reason}")

    if not should_go:
        logger.info(f"Thresholds NOT met ({reason}). No proposal sent.")
        sys.exit(0)

    bid, sig, bundle = _build_proposal_bundle(
        cfg_key=cfg_key,
        secret=secret,
        next_mode=next_mode,
        ttl=bundle_ttl,
    )

    if args.dry_run:
        logger.info(
            f"DRY-RUN: Would create bundle {bid} and send Telegram proposal.\n"
            f"  CROSSVENUE_CTX_PROFILE: {cur_mode} → {next_mode}\n"
            f"  vetoed_winrate: {stats['vetoed_winrate']:.1%}\n"
            f"  saved_r: {stats['saved_r']:+.2f} R"
        )
        sys.exit(0)

    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=bundle_ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=bundle_ttl)
    logger.info(f"Bundle {bid} stored in Redis (TTL={bundle_ttl}s)")

    r.set(pending_key, json.dumps({
        "bundle_id": bid,
        "cur_mode": cur_mode,
        "next_mode": next_mode,
        "ts_ms": _now_ms(),
    }, separators=(",", ":")), ex=82800)

    _send_telegram_proposal(
        r,
        bundle_id=bid,
        sig=sig,
        hours=args.hours,
        stats=stats,
        notify_stream=notify_stream,
        cur_mode=cur_mode,
        next_mode=next_mode,
    )

    _wait_for_decision(
        r,
        bundle_id=bid,
        sig=sig,
        hours=args.hours,
        stats=stats,
        notify_stream=notify_stream,
        reminder_sec=reminder_sec,
        step_ts_key=step_ts_key,
        pending_key=pending_key,
        bundle_ttl=bundle_ttl,
        cur_mode=cur_mode,
        next_mode=next_mode,
    )

if __name__ == "__main__":
    main()
