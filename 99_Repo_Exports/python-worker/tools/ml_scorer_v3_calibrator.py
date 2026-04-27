#!/usr/bin/env python3
"""
ML Scorer V3 Calibrator.

Evaluates ML Scorer V3 shadow performance over --hours:
  - Reads decisions:final stream → extracts shadow ML scorer fields (ml_kind=ml_scorer_v3)
  - Joins with trades:closed stream by sid → gets outcome (r_mult)
  - Computes binary classification metrics: AUC-ROC, Brier Score
  - Target: r_mult >= 0.0 (binary win/loss)

If all thresholds are met → proposes ML_SCORER_MODE=shadow→enforce
via interactive Telegram (✅/❌).
"""

from __future__ import annotations
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
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import redis

# Use scikit-learn for correct binary metrics
from sklearn.metrics import roc_auc_score, brier_score_loss

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ml_scorer_v3_calibrator")

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

def _extract_shadow_fields(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # 1. Try to get indicators from payload (JSON) or directly from event
    payload_str = ev.get("payload", "")
    if payload_str:
        try:
            rec = json.loads(payload_str)
        except Exception:
            rec = ev
    else:
        rec = ev

    # 2. Get indicators (could be dict or JSON string in direct field)
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

    breakdown = indicators.get("confidence_breakdown") if isinstance(indicators.get("confidence_breakdown"), dict) else {}
    ml_kind = str(indicators.get("ml_kind") or "").strip().lower()
    
    # 3. Extract shadow score
    ml_shadow_conf = _f(
        breakdown.get("ml_shadow_conf01")
        or indicators.get("ml_shadow_conf01"),
        default=-1.0,
    )

    if ml_shadow_conf < 0:
        return None

    if ml_kind not in ("ml_scorer_v3",):
        return None

    return {
        "sid": str(rec.get("sid") or rec.get("signal_id") or "").strip(),
        "ml_shadow_conf01": ml_shadow_conf,
        "ml_kind": ml_kind,
    }


def _compute_ece(
    probs: List[float],
    labels: List[int],
    n_bins: int = 10,
) -> float:
    """Expected Calibration Error (equal-width binning).

    ECE = sum_b |accuracy_b - confidence_b| * n_b / N
    Returns 0.0 when probs is empty.
    """
    if not probs or len(probs) != len(labels):
        return 0.0
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.float64)
    n = len(p)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        if i == n_bins - 1:
            mask = (p >= lo) & (p <= hi)
        else:
            mask = (p >= lo) & (p < hi)
        bin_n = int(mask.sum())
        if bin_n == 0:
            continue
        avg_p = float(p[mask].mean())
        avg_y = float(y[mask].mean())
        ece += abs(avg_p - avg_y) * (bin_n / n)
    return float(ece)


def _collect_analytics(
    r: redis.Redis,
    hours: float,
    max_scan: int,
    min_r_target: float = 0.0,
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
    shadow_scored_count = 0
    scored_with_outcome = 0

    ml_scores: List[float] = []
    targets: List[int] = []

    for ev in decision_events:
        shadow = _extract_shadow_fields(ev)
        if shadow is None:
            continue

        shadow_scored_count += 1
        sid = shadow["sid"]

        trade = trades_by_sid.get(sid)
        if not trade:
            continue

        r_mult = _f(trade.get("r_mult") or trade.get("pnl_r") or trade.get("pnl"))

        scored_with_outcome += 1
        ml_scores.append(shadow["ml_shadow_conf01"])
        targets.append(1 if r_mult >= min_r_target else 0)

    roc_auc = 0.5
    brier = 0.0
    ece = 0.0

    if scored_with_outcome >= 10:
        # Check if we have both classes
        if sum(targets) > 0 and sum(targets) < len(targets):
            roc_auc = roc_auc_score(targets, ml_scores)
        brier = brier_score_loss(targets, ml_scores)
        ece = _compute_ece(ml_scores, targets, n_bins=10)

    return {
        "total_decisions": total_decisions,
        "shadow_scored_count": shadow_scored_count,
        "scored_with_outcome": scored_with_outcome,
        "roc_auc": roc_auc,
        "brier_score": brier,
        "ece": ece,
        "positive_class_ratio": sum(targets) / max(1, len(targets)),
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
    return os.getenv("ML_SCORER_MODE", "shadow").strip().lower()

def _build_proposal_bundle(
    cfg_key: str,
    secret: str,
    ttl: int = 86400,
) -> Tuple[str, str, Dict[str, Any]]:
    bid = secrets.token_hex(6)
    sig = _sign(bid, secret)
    ts = _now_ms()

    ops = [{
        "op": "SET",
        "key": cfg_key,
        "value": "enforce",
    }]
    bundle = {
        "id": bid,
        "created_ms": ts,
        "ttl_sec": ttl,
        "who": "ml_scorer_v3_calibrator",
        "ops": ops,
        "meta": {"kind": "ml_scorer_mode_promote"},
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
    is_reminder: bool = False,
) -> None:
    scored = int(stats["scored_with_outcome"])
    auc = stats["roc_auc"]
    brier = stats["brier_score"]
    ece = stats.get("ece", 0.0)
    win_rate = stats["positive_class_ratio"]

    reminder_tag = "\n⏰ <i>Напоминание — ожидается ваше решение</i>\n" if is_reminder else ""

    text = (
        f"<b>🧠 ML Scorer V3 Calibrator</b>{reminder_tag}\n\n"
        f"За последние <b>{int(hours)}ч</b> shadow-режим V3 собрал данные:\n"
        f"  • Сделок с ML score + outcome: <b>{scored}</b>\n"
        f"  • Base WinRate (target): <b>{win_rate:.1%}</b>\n"
        f"  • ROC-AUC: <b>{auc:.4f}</b>\n"
        f"  • Brier Score: <b>{brier:.4f}</b>\n"
        f"  • ECE (calibration): <b>{ece:.4f}</b>\n\n"
        f"Предлагаю: <code>ML_SCORER_MODE</code> <b>shadow → enforce</b>\n"
        f"(ML Scorer V3 начнёт влиять на confidence scoring)"
    )

    buttons = [[
        {"text": "✅ Применить", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
        {"text": "❌ Отклонить", "callback_data": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    r.xadd(notify_stream, {
        "type": "report",
        "subtype": "ml_scorer_calibrator",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
    }, maxlen=50000)
    tag = "REMINDER" if is_reminder else "NEW"
    logger.info(f"Telegram proposal [{tag}]: bundle_id={bundle_id} shadow→enforce")

def _should_propose(
    *,
    stats: Dict[str, float],
    holddown_ok: bool,
    min_scored_trades: int,
    min_auc: float,
    max_brier: float,
    max_ece: float,
) -> Tuple[bool, str]:
    scored = int(stats["scored_with_outcome"])
    auc = stats["roc_auc"]
    brier = stats["brier_score"]
    ece = stats.get("ece", 0.0)

    if not holddown_ok:
        return False, "holddown_not_expired"

    if scored < min_scored_trades:
        return False, f"scored_trades={scored} < min={min_scored_trades}"

    if auc < min_auc:
        return False, f"roc_auc={auc:.4f} < threshold={min_auc:.4f}"

    if brier > max_brier:
        return False, f"brier_score={brier:.4f} > threshold={max_brier:.4f}"

    if ece > max_ece:
        return False, f"ece={ece:.4f} > threshold={max_ece:.4f}"

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
) -> None:
    if decision == "approved":
        text = (
            f"<b>✅ ML Scorer V3 — режим обновлён</b>\n\n"
            f"<code>ML_SCORER_MODE</code>: <b>shadow → enforce</b>\n"
            f"ML Scorer V3 теперь влияет на confidence scoring.\n\n"
            f"Calibrator следующий раз проверит стабильность через ≥72ч.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    else:
        text = (
            f"<b>❌ ML Scorer V3 — предложение отклонено</b>\n\n"
            f"<code>ML_SCORER_MODE</code> остаётся <b>shadow</b>\n"
            f"Calibrator предложит снова в следующем цикле\n"
            f"(если пороги по-прежнему выполнены).\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )

    r.xadd(notify_stream, {
        "type": "report",
        "subtype": "ml_scorer_calibrator_decision",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
    }, maxlen=50000)
    logger.info(f"Decision notification sent: {decision} bundle={bundle_id}")

def main() -> None:
    ap = argparse.ArgumentParser(description="ML Scorer V3 Calibrator")
    ap.add_argument(
        "--hours", type=float,
        default=float(os.getenv("MLS_CAL_HOURS", "168")),
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
        "--min-scored-trades", type=int,
        default=int(os.getenv("MLS_CAL_MIN_SCORED_TRADES", "50")),
        help="Minimum scored trades with outcome to consider proposing",
    )
    ap.add_argument(
        "--max-scan", type=int,
        default=int(os.getenv("MLS_CAL_MAX_SCAN", "500000")),
        help="Maximum stream entries to scan",
    )
    args = ap.parse_args()

    cfg_key = os.getenv("MLS_CAL_CFG_KEY", "cfg:ml_scorer:mode")
    pending_key = os.getenv("MLS_CAL_PENDING_KEY", "meta:ml_scorer_cal:pending")
    step_ts_key = os.getenv("MLS_CAL_STEP_TS_KEY", "meta:ml_scorer_cal:last_step_ms")
    holddown_h = float(os.getenv("MLS_CAL_ENFORCE_HOLDDOWN_H", "72"))
    
    # New V3 thresholds
    min_auc = float(os.getenv("MLS_CAL_MIN_AUC", "0.52"))
    max_brier = float(os.getenv("MLS_CAL_MAX_BRIER", "0.25"))
    max_ece = float(os.getenv("MLS_CAL_MAX_ECE", "0.10"))
    
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_ttl = int(os.getenv("MLS_CAL_BUNDLE_TTL_SEC", "86400"))
    reminder_sec = int(os.getenv("MLS_CAL_REMINDER_SEC", "1800"))

    logger.info(
        f"ML Scorer V3 Calibrator | hours={args.hours} "
        f"min_scored_trades={args.min_scored_trades} "
        f"max_scan={args.max_scan} "
        f"dry_run={args.dry_run} "
        f"min_auc={min_auc} "
        f"max_brier={max_brier} "
        f"max_ece={max_ece}"
    )

    r = _get_redis()

    if not args.dry_run and not args.force_propose:
        if r.exists(pending_key):
            logger.info(f"Pending proposal already exists ({pending_key}). Skipping.")
            sys.exit(0)

    cur_mode = _load_current_mode(r, cfg_key)
    logger.info(f"Current ML_SCORER_MODE: {cur_mode}")

    if cur_mode == "enforce":
        logger.info("ML_SCORER_MODE already 'enforce'. Nothing to promote.")
        sys.exit(0)

    hd_ok, elapsed_h = _holddown_ok(r, step_ts_key, holddown_h)
    logger.info(f"Holddown: elapsed={elapsed_h:.1f}h required={holddown_h}h ok={hd_ok}")

    stats = _collect_analytics(r, args.hours, args.max_scan)

    logger.info(
        f"Stats over {args.hours}h:\n"
        f"  total_decisions      = {int(stats['total_decisions'])}\n"
        f"  shadow_scored_count  = {int(stats['shadow_scored_count'])}\n"
        f"  scored_with_outcome  = {int(stats['scored_with_outcome'])}\n"
        f"  positive_class_ratio = {stats['positive_class_ratio']:.1%}\n"
        f"  roc_auc              = {stats['roc_auc']:.4f}\n"
        f"  brier_score          = {stats['brier_score']:.4f}\n"
        f"  ece                  = {stats['ece']:.4f}"
    )

    if args.force_propose:
        should_go = True
        reason = "force_propose"
        logger.info("Forcing proposal (--force-propose)")
    else:
        should_go, reason = _should_propose(
            stats=stats,
            holddown_ok=hd_ok,
            min_scored_trades=args.min_scored_trades,
            min_auc=min_auc,
            max_brier=max_brier,
            max_ece=max_ece,
        )

    logger.info(f"Decision: should_propose={should_go} reason={reason}")

    if not should_go:
        logger.info(f"Thresholds NOT met ({reason}). No proposal sent.")
        sys.exit(0)

    bid, sig, bundle = _build_proposal_bundle(
        cfg_key=cfg_key,
        secret=secret,
        ttl=bundle_ttl,
    )

    if args.dry_run:
        logger.info(
            f"DRY-RUN: Would create bundle {bid} and send Telegram proposal.\n"
            f"  ML_SCORER_MODE: shadow → enforce\n"
            f"  AUC-ROC: {stats['roc_auc']:.4f}\n"
            f"  Brier: {stats['brier_score']:.4f}\n"
            f"  ECE: {stats['ece']:.4f}"
        )
        sys.exit(0)

    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=bundle_ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=bundle_ttl)
    logger.info(f"Bundle {bid} stored in Redis (TTL={bundle_ttl}s)")

    r.set(pending_key, json.dumps({
        "bundle_id": bid,
        "cur_mode": "shadow",
        "next_mode": "enforce",
        "ts_ms": _now_ms(),
    }, separators=(",", ":")), ex=82800)

    _send_telegram_proposal(
        r,
        bundle_id=bid,
        sig=sig,
        hours=args.hours,
        stats=stats,
        notify_stream=notify_stream,
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
    )

if __name__ == "__main__":
    main()
