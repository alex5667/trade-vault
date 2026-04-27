#!/usr/bin/env python3
"""
ML Scorer V2 Calibrator.

Evaluates ML Scorer V2 shadow performance over --hours (default 168h):
  - Reads decisions:final stream → extracts shadow ML scorer fields from payload
  - Joins with trades:closed stream by sid → gets outcome (r_mult)
  - Computes: spearman_corr, shadow_veto_precision, pnl_impact, rmse_improvement

If all thresholds are met → proposes ML_SCORER_MODE=shadow→enforce
via interactive Telegram (✅/❌).

Binary rollout: shadow → enforce (no intermediate steps).

Idempotency guard: Redis key meta:ml_scorer_cal:pending (TTL=23h).
Hold-down: meta:ml_scorer_cal:last_step_ms — minimum interval between proposals.

ENV vars:
  REDIS_URL                       redis://redis-worker-1:6379/0
  DECISIONS_FINAL_STREAM          decisions:final
  ML_OUTCOME_STREAM               trades:closed
  RECS_HMAC_SECRET                CHANGE_ME
  NOTIFY_STREAM                   notify:telegram
  MLS_CAL_HOURS                   168
  MLS_CAL_MIN_SCORED_TRADES       50
  MLS_CAL_MAX_SCAN                500000
  MLS_CAL_MIN_SPEARMAN            0.05    (min Spearman correlation)
  MLS_CAL_MIN_VETO_PRECISION      0.55    (shadow veto precision)
  MLS_CAL_ENFORCE_HOLDDOWN_H      72      (min hours between proposals)
  MLS_CAL_REMINDER_SEC            1800    (reminder interval)
  MLS_CAL_PENDING_KEY             meta:ml_scorer_cal:pending
  MLS_CAL_STEP_TS_KEY             meta:ml_scorer_cal:last_step_ms
  MLS_CAL_CFG_KEY                 cfg:ml_scorer:mode
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
from typing import Dict, List, Optional, Tuple

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ml_scorer_calibrator")

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
    """Read stream entries from `since_ms` timestamp onwards."""
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
    """XREVRANGE-based reader for trades:closed (newest first, filtered by ts_ms field)."""
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
                return results  # older than window → stop
            results.append(fields)
        last_id_raw = batch[-1][0]
        ts_part, seq_part = last_id_raw.split("-")
        last_id = f"{ts_part}-{max(0, int(seq_part) - 1)}"
        if len(batch) < page:
            break

    return results


# ─────────────────────────────────────── metrics collection ───────────────── #

def _extract_shadow_fields(payload_str: str) -> Optional[Dict[str, float]]:
    """Parse decisions:final payload JSON, extract ML scorer shadow fields.

    Looks for:
      - indicators.confidence_breakdown.ml_shadow_conf01
      - indicators.confidence_breakdown.ml_shadow_predicted_r
      - indicators.confidence_breakdown.scorer_mode == "shadow"
      - indicators.confidence_v1 (rule-based confidence)
      - indicators.ml_shadow_veto
    """
    try:
        rec = json.loads(payload_str)
    except Exception:
        return None

    indicators = rec.get("indicators") if isinstance(rec.get("indicators"), dict) else {}
    breakdown = indicators.get("confidence_breakdown") if isinstance(indicators.get("confidence_breakdown"), dict) else {}

    # Try multiple locations for shadow ML scorer fields
    ml_shadow_conf = _f(
        breakdown.get("ml_shadow_conf01")
        or indicators.get("ml_shadow_conf01"),
        default=-1.0,
    )

    ml_shadow_r = _f(
        breakdown.get("ml_shadow_predicted_r")
        or indicators.get("ml_shadow_predicted_r"),
        default=-999.0,
    )

    scorer_mode = str(
        breakdown.get("scorer_mode")
        or indicators.get("scorer_mode")
        or ""
    ).lower()

    rule_conf = _f(indicators.get("confidence_v1") or indicators.get("confidence"), 0.0)
    ml_shadow_veto = _i(indicators.get("ml_shadow_veto"), 0)

    sid = str(rec.get("sid") or "").strip()

    # Must have some shadow data to be useful
    if ml_shadow_conf < 0 and ml_shadow_r < -900 and ml_shadow_veto == 0:
        return None

    return {
        "sid": sid,
        "ml_shadow_conf01": ml_shadow_conf if ml_shadow_conf >= 0 else 0.0,
        "ml_shadow_predicted_r": ml_shadow_r if ml_shadow_r > -900 else 0.0,
        "rule_conf01": rule_conf,
        "scorer_mode": scorer_mode,
        "ml_shadow_veto": ml_shadow_veto,
    }


def _spearman_rank_corr(x: List[float], y: List[float]) -> float:
    """Manual Spearman rank correlation (no scipy dependency)."""
    n = len(x)
    if n < 3:
        return 0.0

    def _rank(vals: List[float]) -> List[float]:
        indexed = sorted(enumerate(vals), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and abs(indexed[j + 1][1] - indexed[i][1]) < 1e-12:
                j += 1
            avg_rank = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                ranks[indexed[k][0]] = avg_rank
            i = j + 1
        return ranks

    rx = _rank(x)
    ry = _rank(y)

    # Pearson on ranks
    mx = sum(rx) / n
    my = sum(ry) / n

    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    dx = sum((rx[i] - mx) ** 2 for i in range(n)) ** 0.5
    dy = sum((ry[i] - my) ** 2 for i in range(n)) ** 0.5

    if dx < 1e-12 or dy < 1e-12:
        return 0.0

    return num / (dx * dy)


def _rmse(predictions: List[float], actuals: List[float]) -> float:
    """Root Mean Square Error."""
    n = len(predictions)
    if n == 0:
        return 0.0
    return (sum((p - a) ** 2 for p, a in zip(predictions, actuals)) / n) ** 0.5


def _collect_analytics(
    r: redis.Redis,
    hours: float,
    max_scan: int,
) -> Dict[str, float]:
    """
    Read decisions:final and trades:closed, join by sid.

    Returns dict with:
      total_decisions      - total decision records in window
      shadow_scored_count  - decisions with ML shadow data
      scored_with_outcome  - shadow-scored decisions with closed trade outcome
      spearman_corr        - Spearman(ml_shadow_conf01, r_mult)
      rmse_rule            - RMSE of rule_conf01 vs r_mult
      rmse_ml              - RMSE of ml_shadow_conf01 vs r_mult
      rmse_improvement_pct - (rmse_rule - rmse_ml) / rmse_rule * 100
      shadow_veto_count    - signals where ml_shadow_veto=1
      shadow_veto_with_outcome - vetoed signals with closed trade
      shadow_veto_negative - vetoed signals with r_mult < 0
      shadow_veto_precision - shadow_veto_negative / shadow_veto_with_outcome
      shadow_veto_r_sum    - sum of r_mult for shadow-vetoed trades
      pnl_impact_r         - -shadow_veto_r_sum (positive = saved money)
      pass_r_sum           - sum of r_mult for non-vetoed trades
      pass_count           - non-vetoed trades with outcome
    """
    decisions_stream = os.getenv("DECISIONS_FINAL_STREAM", "decisions:final")
    trades_stream = os.getenv("ML_OUTCOME_STREAM", "trades:closed")

    since_ms = _now_ms() - int(hours * 3600 * 1000)
    logger.info(f"Reading {decisions_stream} since {hours}h ago | max_scan={max_scan}")

    decision_events = _read_stream_since(r, decisions_stream, since_ms, max_scan)
    logger.info(f"  → {len(decision_events)} decision events")

    trades_events = _read_stream_recent(r, trades_stream, since_ms, max_scan)
    logger.info(f"  → {len(trades_events)} trades:closed events")

    # Build trades lookup by sid
    trades_by_sid: Dict[str, Dict[str, str]] = {}
    for ev in trades_events:
        sid = str(ev.get("sid") or ev.get("signal_id") or "").strip()
        if sid and sid not in trades_by_sid:
            trades_by_sid[sid] = ev

    total_decisions = len(decision_events)
    shadow_scored_count = 0
    scored_with_outcome = 0

    # Vectors for Spearman/RMSE
    ml_scores: List[float] = []
    rule_scores: List[float] = []
    r_mults: List[float] = []

    # Veto analysis
    shadow_veto_count = 0
    shadow_veto_with_outcome = 0
    shadow_veto_negative = 0
    shadow_veto_r_sum = 0.0

    pass_count = 0
    pass_r_sum = 0.0

    for ev in decision_events:
        payload_str = ev.get("payload", "")
        if not payload_str:
            continue

        shadow = _extract_shadow_fields(payload_str)
        if shadow is None:
            continue

        shadow_scored_count += 1
        sid = shadow["sid"]
        is_veto = shadow["ml_shadow_veto"] == 1

        if is_veto:
            shadow_veto_count += 1

        trade = trades_by_sid.get(sid)
        if not trade:
            continue

        r_mult = _f(trade.get("r_mult") or trade.get("pnl_r") or trade.get("pnl"))

        if is_veto:
            shadow_veto_with_outcome += 1
            shadow_veto_r_sum += r_mult
            if r_mult < 0.0:
                shadow_veto_negative += 1
        else:
            pass_count += 1
            pass_r_sum += r_mult

        # Only include in correlation/RMSE if we have valid ml_shadow_conf01
        if shadow["ml_shadow_conf01"] > 0:
            scored_with_outcome += 1
            ml_scores.append(shadow["ml_shadow_conf01"])
            rule_scores.append(shadow["rule_conf01"])
            r_mults.append(r_mult)

    # Compute metrics
    spearman = _spearman_rank_corr(ml_scores, r_mults) if scored_with_outcome >= 3 else 0.0
    rmse_rule = _rmse(rule_scores, r_mults) if scored_with_outcome > 0 else 0.0
    rmse_ml = _rmse(ml_scores, r_mults) if scored_with_outcome > 0 else 0.0
    rmse_improvement_pct = ((rmse_rule - rmse_ml) / rmse_rule * 100) if rmse_rule > 1e-9 else 0.0

    veto_precision = (shadow_veto_negative / shadow_veto_with_outcome) if shadow_veto_with_outcome > 0 else 0.0
    pnl_impact_r = -shadow_veto_r_sum

    return {
        "total_decisions": total_decisions,
        "shadow_scored_count": shadow_scored_count,
        "scored_with_outcome": scored_with_outcome,
        "spearman_corr": spearman,
        "rmse_rule": rmse_rule,
        "rmse_ml": rmse_ml,
        "rmse_improvement_pct": rmse_improvement_pct,
        "shadow_veto_count": shadow_veto_count,
        "shadow_veto_with_outcome": shadow_veto_with_outcome,
        "shadow_veto_negative": shadow_veto_negative,
        "shadow_veto_precision": veto_precision,
        "shadow_veto_r_sum": shadow_veto_r_sum,
        "pnl_impact_r": pnl_impact_r,
        "pass_count": pass_count,
        "pass_r_sum": pass_r_sum,
    }


# ───────────────────────────────────────────── holddown check ─────────────── #

def _holddown_ok(r: redis.Redis, step_ts_key: str, holddown_h: float) -> Tuple[bool, float]:
    """Returns (ok, hours_since_last_step)."""
    raw = r.get(step_ts_key)
    if not raw:
        return True, 999.0
    try:
        last_ms = int(float(raw))
        elapsed_h = (_now_ms() - last_ms) / 3_600_000
        return elapsed_h >= holddown_h, elapsed_h
    except Exception:
        return True, 999.0


# ─────────────────────────────────────── current mode check ──────────────── #

def _load_current_mode(r: redis.Redis, cfg_key: str) -> str:
    """Load current ML_SCORER_MODE from Redis cfg key."""
    raw = r.get(cfg_key)
    if raw:
        return str(raw).strip().lower()
    # Fallback to ENV
    return os.getenv("ML_SCORER_MODE", "shadow").strip().lower()


# ─────────────────────────────────────── bundle + telegram ────────────────── #

def _build_proposal_bundle(
    cfg_key: str,
    secret: str,
    ttl: int = 86400,
) -> Tuple[str, str, Dict]:
    """Create recs bundle that sets ML_SCORER_MODE=enforce."""
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
        "who": "ml_scorer_calibrator",
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
    spearman = stats["spearman_corr"]
    rmse_rule = stats["rmse_rule"]
    rmse_ml = stats["rmse_ml"]
    rmse_impr = stats["rmse_improvement_pct"]
    veto_count = int(stats["shadow_veto_count"])
    veto_outcome = int(stats["shadow_veto_with_outcome"])
    veto_precision = stats["shadow_veto_precision"]
    veto_r = stats["shadow_veto_r_sum"]
    pnl_impact = stats["pnl_impact_r"]

    pnl_sign = "+" if pnl_impact >= 0 else ""
    reminder_tag = "\n⏰ <i>Напоминание — ожидается ваше решение</i>\n" if is_reminder else ""

    text = (
        f"<b>🧠 ML Scorer V2 Calibrator</b>{reminder_tag}\n\n"
        f"За последние <b>{int(hours)}ч</b> shadow-режим собрал данные:\n"
        f"  • Сделок с ML score + outcome: <b>{scored}</b>\n"
        f"  • Spearman(ml_score, r_mult): <b>{spearman:+.4f}</b>\n"
        f"  • RMSE rule: <b>{rmse_rule:.4f}</b> → ML: <b>{rmse_ml:.4f}</b> ({rmse_impr:+.1f}%)\n"
        f"  • Shadow-veto событий: <b>{veto_count}</b>\n"
        f"  • Veto с исходом: <b>{veto_outcome}</b>\n"
        f"  • Veto precision: <b>{veto_precision:.1%}</b>\n"
        f"  • Veto R sum: <b>{veto_r:+.2f}R</b>\n"
        f"  • PnL impact (сэкономлено): <b>{pnl_sign}{pnl_impact:.2f}R</b>\n\n"
        f"Предлагаю: <code>ML_SCORER_MODE</code> <b>shadow → enforce</b>\n"
        f"(ML Scorer V2 начнёт влиять на confidence scoring)"
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


# ──────────────────────────────────────────────────────── decision logic ───── #

def _should_propose(
    *,
    stats: Dict[str, float],
    holddown_ok: bool,
    min_scored_trades: int,
    min_spearman: float,
    min_veto_precision: float,
) -> Tuple[bool, str]:
    """
    Returns (should_propose, reason).
    All conditions must be satisfied.
    """
    scored = int(stats["scored_with_outcome"])
    spearman = stats["spearman_corr"]
    veto_precision = stats["shadow_veto_precision"]
    veto_with_outcome = int(stats["shadow_veto_with_outcome"])
    pnl_impact = stats["pnl_impact_r"]

    if not holddown_ok:
        return False, "holddown_not_expired"

    if scored < min_scored_trades:
        return False, f"scored_trades={scored} < min={min_scored_trades}"

    if spearman < min_spearman:
        return False, f"spearman={spearman:.4f} < threshold={min_spearman:.4f}"

    # Require some veto data if vetoes happened
    if veto_with_outcome > 0:
        if veto_precision < min_veto_precision:
            return False, f"veto_precision={veto_precision:.3f} < threshold={min_veto_precision:.3f}"
        if pnl_impact <= 0:
            return False, f"pnl_impact={pnl_impact:.3f} <= 0"

    return True, "ok"


# ────────────────────────────────────────────────────── reminder loop ──────── #

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
    """
    Reminder loop: polls recs:status:<bundle_id> every reminder_sec.
    - If still PENDING/PREVIEWED → resend Telegram notification with ⏰ tag.
    - If APPLIED → update holddown timestamp, send confirmation, exit.
    - If REJECTED/ROLLED_BACK → send rejection notice, exit.
    - If bundle TTL expires (status gone) → exit silently.
    """
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

        # Still PENDING or PREVIEWED → resend reminder
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
    """Sends detailed Telegram notification after approve/reject decision."""
    if decision == "approved":
        text = (
            f"<b>✅ ML Scorer V2 — режим обновлён</b>\n\n"
            f"<code>ML_SCORER_MODE</code>: <b>shadow → enforce</b>\n"
            f"ML Scorer V2 теперь влияет на confidence scoring.\n\n"
            f"Calibrator следующий раз проверит стабильность через ≥72ч.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    else:
        text = (
            f"<b>❌ ML Scorer V2 — предложение отклонено</b>\n\n"
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


# ────────────────────────────────────────────────────────────── main ──────── #

def main() -> None:
    ap = argparse.ArgumentParser(description="ML Scorer V2 Calibrator")
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
    min_spearman = float(os.getenv("MLS_CAL_MIN_SPEARMAN", "0.05"))
    min_veto_precision = float(os.getenv("MLS_CAL_MIN_VETO_PRECISION", "0.55"))
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_ttl = int(os.getenv("MLS_CAL_BUNDLE_TTL_SEC", "86400"))
    reminder_sec = int(os.getenv("MLS_CAL_REMINDER_SEC", "1800"))

    logger.info(
        f"ML Scorer V2 Calibrator | hours={args.hours} "
        f"min_scored_trades={args.min_scored_trades} "
        f"max_scan={args.max_scan} "
        f"dry_run={args.dry_run} "
        f"force_propose={args.force_propose} "
        f"min_spearman={min_spearman} "
        f"min_veto_precision={min_veto_precision}"
    )

    r = _get_redis()

    # ── 1) Idempotency guard ──────────────────────────────────────────────── #
    if not args.dry_run and not args.force_propose:
        if r.exists(pending_key):
            logger.info(f"Pending proposal already exists ({pending_key}). Skipping.")
            sys.exit(0)

    # ── 2) Check current mode ─────────────────────────────────────────────── #
    cur_mode = _load_current_mode(r, cfg_key)
    logger.info(f"Current ML_SCORER_MODE: {cur_mode}")

    if cur_mode == "enforce":
        logger.info("ML_SCORER_MODE already 'enforce'. Nothing to promote.")
        sys.exit(0)

    # ── 3) Holddown check ─────────────────────────────────────────────────── #
    hd_ok, elapsed_h = _holddown_ok(r, step_ts_key, holddown_h)
    logger.info(f"Holddown: elapsed={elapsed_h:.1f}h required={holddown_h}h ok={hd_ok}")

    # ── 4) Collect analytics ──────────────────────────────────────────────── #
    stats = _collect_analytics(r, args.hours, args.max_scan)

    logger.info(
        f"Stats over {args.hours}h:\n"
        f"  total_decisions      = {int(stats['total_decisions'])}\n"
        f"  shadow_scored_count  = {int(stats['shadow_scored_count'])}\n"
        f"  scored_with_outcome  = {int(stats['scored_with_outcome'])}\n"
        f"  spearman_corr        = {stats['spearman_corr']:+.4f}\n"
        f"  rmse_rule            = {stats['rmse_rule']:.4f}\n"
        f"  rmse_ml              = {stats['rmse_ml']:.4f}\n"
        f"  rmse_improvement_pct = {stats['rmse_improvement_pct']:+.1f}%\n"
        f"  shadow_veto_count    = {int(stats['shadow_veto_count'])}\n"
        f"  shadow_veto_outcome  = {int(stats['shadow_veto_with_outcome'])}\n"
        f"  shadow_veto_negative = {int(stats['shadow_veto_negative'])}\n"
        f"  shadow_veto_precision= {stats['shadow_veto_precision']:.1%}\n"
        f"  shadow_veto_r_sum    = {stats['shadow_veto_r_sum']:+.3f}R\n"
        f"  pnl_impact_r         = {stats['pnl_impact_r']:+.3f}R\n"
        f"  pass_count           = {int(stats['pass_count'])}\n"
        f"  pass_r_sum           = {stats['pass_r_sum']:+.3f}R"
    )

    # ── 5) Decision ───────────────────────────────────────────────────────── #
    if args.force_propose:
        should_go = True
        reason = "force_propose"
        logger.info("Forcing proposal (--force-propose)")
    else:
        should_go, reason = _should_propose(
            stats=stats,
            holddown_ok=hd_ok,
            min_scored_trades=args.min_scored_trades,
            min_spearman=min_spearman,
            min_veto_precision=min_veto_precision,
        )

    logger.info(f"Decision: should_propose={should_go} reason={reason}")

    if not should_go:
        logger.info(f"Thresholds NOT met ({reason}). No proposal sent.")
        sys.exit(0)

    # ── 6) Build and store bundle ─────────────────────────────────────────── #
    bid, sig, bundle = _build_proposal_bundle(
        cfg_key=cfg_key,
        secret=secret,
        ttl=bundle_ttl,
    )

    if args.dry_run:
        logger.info(
            f"DRY-RUN: Would create bundle {bid} and send Telegram proposal.\n"
            f"  ML_SCORER_MODE: shadow → enforce\n"
            f"  spearman: {stats['spearman_corr']:+.4f}\n"
            f"  veto_precision: {stats['shadow_veto_precision']:.1%}\n"
            f"  pnl_impact: {stats['pnl_impact_r']:+.3f}R"
        )
        sys.exit(0)

    # Write bundle to Redis
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=bundle_ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=bundle_ttl)
    logger.info(f"Bundle {bid} stored in Redis (TTL={bundle_ttl}s)")

    # Idempotency lock (23h)
    r.set(pending_key, json.dumps({
        "bundle_id": bid,
        "cur_mode": "shadow",
        "next_mode": "enforce",
        "ts_ms": _now_ms(),
    }, separators=(",", ":")), ex=82800)  # 23h

    # ── 7) Send initial Telegram notification ─────────────────────────────── #
    _send_telegram_proposal(
        r,
        bundle_id=bid,
        sig=sig,
        hours=args.hours,
        stats=stats,
        notify_stream=notify_stream,
    )

    # ── 8) Reminder loop: resend until approve/reject ─────────────────────── #
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
