#!/usr/bin/env python3
"""
ML Confirm Gate Calibrator.

Анализирует shadow-veto performance за --hours (default 168h):
  - Читает metrics:ml_confirm stream → shadow-blocked события (allow=0)
  - Джойнит с trades:closed stream по sid → получает outcome (r_mult)
  - Вычисляет: precision_veto, veto_r_sum, pnl_impact

При выполнении условий — продвигает enforce_share по лестнице:
  0.0 → 0.05 → 0.20 → 0.50 → 1.00

Каждый шаг требует интерактивного подтверждения через Telegram (✅/❌).
Idempotency guard: Redis key meta:ml_cal:pending (TTL=23h).
Hold-down: meta:ml_cal:last_step_ms — минимальный интервал между шагами.

ENV vars:
  REDIS_URL                   redis://redis-worker-1:6379/0
  ML_CONFIRM_METRICS_STREAM   metrics:ml_confirm
  ML_CFG_CHAMPION_KEY         cfg:ml_confirm:champion
  ML_OUTCOME_STREAM           trades:closed
  RECS_HMAC_SECRET            CHANGE_ME
  NOTIFY_STREAM               notify:telegram
  ML_CAL_HOURS                168
  ML_CAL_MIN_VETO_HITS        30
  ML_CAL_MAX_SCAN             500000
  ML_CAL_PRECISION_L1         0.55    (0.0  → 0.05)
  ML_CAL_PRECISION_L2         0.57    (0.05 → 0.20)
  ML_CAL_PRECISION_L3         0.60    (0.20 → 0.50)
  ML_CAL_PRECISION_L4         0.62    (0.50 → 1.00)
  ML_CAL_ENFORCE_HOLDDOWN_H   72      (min hours between ladder steps)
  ML_CAL_REMINDER_SEC         1800    (reminder interval until approve/reject)
  ML_CAL_PENDING_KEY          meta:ml_cal:pending
  ML_CAL_STEP_TS_KEY          meta:ml_cal:last_step_ms
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
logger = logging.getLogger("ml_confirm_calibrator")

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


# ──────────────────────────────────────────── ladder configuration ─────────── #

LADDER_LEVELS = [0.05, 0.20, 0.50, 1.00]

# Precision thresholds for each next level
PRECISION_FOR_LEVEL = {
    0.05: "ML_CAL_PRECISION_L1",
    0.20: "ML_CAL_PRECISION_L2",
    0.50: "ML_CAL_PRECISION_L3",
    1.00: "ML_CAL_PRECISION_L4",
}

PRECISION_DEFAULTS = {
    0.05: 0.55,
    0.20: 0.57,
    0.50: 0.60,
    1.00: 0.62,
}


def _precision_threshold(next_level: float) -> float:
    env_key = PRECISION_FOR_LEVEL.get(next_level, "ML_CAL_PRECISION_L1")
    default = PRECISION_DEFAULTS.get(next_level, 0.55)
    try:
        return float(os.getenv(env_key, str(default)) or default)
    except Exception:
        return default


def _ladder_next(cur: float) -> Optional[float]:
    """Return next ladder level, or None if already at top."""
    for lv in LADDER_LEVELS:
        if cur + 1e-9 < lv:
            return lv
    return None  # already at 1.0


# ────────────────────────────────────────── stream reader ─────────────────── #

def _read_stream_since(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, str]]:
    """
    Read stream entries from `since_ms` timestamp onwards.
    Returns list of field-dicts (Redis stream message bodies).
    """
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
        # increment to avoid re-reading last entry
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

def _collect_analytics(
    r: redis.Redis,
    hours: float,
    max_scan: int,
) -> Dict[str, float]:
    """
    Read metrics:ml_confirm and trades:closed, join by sid.

    Returns dict with:
      total_events       - total ml_confirm events in window
      shadow_veto_count  - events where allow=0 AND ok_rule=1 (pure ML-veto on rule-passed signals)
      shadow_veto_all    - events where allow=0 (all, including rule-blocked)
      ok_rule_skip_count - events where allow=0 BUT ok_rule=0 (rule already blocked, ML irrelevant)
      enforce_veto_count - events where allow=0 AND mode=ENFORCE
      veto_with_outcome  - shadow-veto (ok_rule=1 only) events that have a closed trade
      veto_negative      - veto events where r_mult < 0 (correct vetoes)
      veto_r_sum         - sum of r_mult for vetoed-then-closed trades (ok_rule=1 only)
      precision_veto     - veto_negative / veto_with_outcome
      mean_r_vetoed      - mean r_mult of vetoed trades
      pnl_impact_r       - -veto_r_sum (PnL saved by vetoing bad trades)
      pass_r_sum         - sum of r_mult for passed trades (allow=1)
      pass_count         - events where allow=1 with outcome
    """
    ml_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
    trades_stream = os.getenv("ML_OUTCOME_STREAM", "trades:closed")

    since_ms = _now_ms() - int(hours * 3600 * 1000)
    logger.info(f"Reading {ml_stream} since {hours}h ago | max_scan={max_scan}")

    ml_events = _read_stream_since(r, ml_stream, since_ms, max_scan)
    logger.info(f"  → {len(ml_events)} ml_confirm events")

    trades_events = _read_stream_recent(r, trades_stream, since_ms, max_scan)
    logger.info(f"  → {len(trades_events)} trades:closed events")

    # Build trades lookup by sid
    trades_by_sid: Dict[str, Dict[str, str]] = {}
    for ev in trades_events:
        sid = str(ev.get("sid") or ev.get("signal_id") or "").strip()
        if sid and sid not in trades_by_sid:
            trades_by_sid[sid] = ev

    total_events = len(ml_events)
    shadow_veto_count = 0   # ML-veto where ok_rule=1 (pure ML signal)
    shadow_veto_all = 0     # all allow=0 events (including rule-blocked)
    ok_rule_skip_count = 0  # allow=0 but ok_rule=0 → rule already blocked, ML irrelevant
    enforce_veto_count = 0
    veto_with_outcome = 0
    veto_negative = 0
    veto_r_sum = 0.0
    pass_with_outcome = 0
    pass_r_sum = 0.0

    for ev in ml_events:
        allow = _i(ev.get("allow"), 1)
        mode = str(ev.get("mode") or "SHADOW").upper()
        sid = str(ev.get("sid") or "").strip()
        ok_rule = _i(ev.get("ok_rule"), 0)

        if allow == 0:
            shadow_veto_all += 1
            if mode == "ENFORCE":
                enforce_veto_count += 1

            # Only count as ML-veto if rules passed (ok_rule=1).
            # If ok_rule=0, the signal was already rule-blocked — ML veto is irrelevant
            # and its PnL impact would be misleading (the trade was never going to fire).
            if ok_rule != 1:
                ok_rule_skip_count += 1
                continue

            shadow_veto_count += 1
            trade = trades_by_sid.get(sid)
            if trade:
                r_mult = _f(trade.get("r_mult") or trade.get("pnl_r") or trade.get("pnl"))
                veto_with_outcome += 1
                veto_r_sum += r_mult
                if r_mult < 0.0:
                    veto_negative += 1
        elif allow == 1:
            trade = trades_by_sid.get(sid)
            if trade:
                r_mult = _f(trade.get("r_mult") or trade.get("pnl_r") or trade.get("pnl"))
                pass_with_outcome += 1
                pass_r_sum += r_mult

    precision = (veto_negative / veto_with_outcome) if veto_with_outcome > 0 else 0.0
    mean_r_vetoed = (veto_r_sum / veto_with_outcome) if veto_with_outcome > 0 else 0.0
    pnl_impact_r = -veto_r_sum  # positive = we saved money

    return {
        "total_events": total_events,
        "shadow_veto_count": shadow_veto_count,
        "shadow_veto_all": shadow_veto_all,
        "ok_rule_skip_count": ok_rule_skip_count,
        "enforce_veto_count": enforce_veto_count,
        "veto_with_outcome": veto_with_outcome,
        "veto_negative": veto_negative,
        "veto_r_sum": veto_r_sum,
        "precision_veto": precision,
        "mean_r_vetoed": mean_r_vetoed,
        "pnl_impact_r": pnl_impact_r,
        "pass_with_outcome": pass_with_outcome,
        "pass_r_sum": pass_r_sum,
    }


# ─────────────────────────────────────────── champion cfg read/write ──────── #

def _load_champion_cfg(r: redis.Redis, key: str) -> Dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_champion_cfg(r: redis.Redis, key: str, cfg: Dict) -> None:
    r.set(key, json.dumps(cfg, ensure_ascii=False, separators=(",", ":")))


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


# ─────────────────────────────────────── bundle + telegram ────────────────── #

def _build_proposal_bundle(
    champion_key: str,
    cfg: Dict,
    next_share: float,
    secret: str,
    ttl: int = 86400,
) -> Tuple[str, str, Dict]:
    """Create recs bundle that patches enforce_share in champion cfg JSON."""
    bid = secrets.token_hex(6)
    sig = _sign(bid, secret)
    ts = _now_ms()

    new_cfg = dict(cfg)
    new_cfg["enforce_share"] = float(next_share)
    new_cfg["updated_ms"] = ts
    new_cfg["updated_by"] = "ml_confirm_calibrator"

    ops = [{
        "op": "SET",
        "key": champion_key,
        "value": json.dumps(new_cfg, ensure_ascii=False, separators=(",", ":")),
    }]
    bundle = {
        "id": bid,
        "created_ms": ts,
        "ttl_sec": ttl,
        "who": "ml_confirm_calibrator",
        "ops": ops,
        "meta": {"kind": "ml_confirm_enforce_share_ladder"},
    }
    return bid, sig, bundle


def _send_telegram_proposal(
    r: redis.Redis,
    *,
    bundle_id: str,
    sig: str,
    cur_share: float,
    next_share: float,
    hours: float,
    stats: Dict[str, float],
    notify_stream: str,
    is_reminder: bool = False,
) -> None:
    veto_count = int(stats["shadow_veto_count"])
    veto_outcome = int(stats["veto_with_outcome"])
    precision = stats["precision_veto"]
    veto_r = stats["veto_r_sum"]
    pnl_impact = stats["pnl_impact_r"]
    mean_r = stats["mean_r_vetoed"]

    pnl_sign = "+" if pnl_impact >= 0 else ""
    reminder_tag = "\n⏰ <i>Напоминание — ожидается ваше решение</i>\n" if is_reminder else ""

    text = (
        f"<b>🤖 ML Confirm Gate Calibrator</b>{reminder_tag}\n\n"
        f"За последние <b>{int(hours)}ч</b> shadow-veto собрал данные:\n"
        f"  • Shadow-veto событий: <b>{veto_count}</b>\n"
        f"  • С известным исходом: <b>{veto_outcome}</b>\n"
        f"  • Precision (верных вето): <b>{precision:.1%}</b>\n"
        f"  • Суммарный R vetoed: <b>{veto_r:+.2f}R</b>\n"
        f"  • Ср. R vetoed: <b>{mean_r:+.3f}R</b>\n"
        f"  • PnL impact (сэкономлено): <b>{pnl_sign}{pnl_impact:.2f}R</b>\n\n"
        f"Предлагаю: <code>enforce_share</code> <b>{cur_share:.2f} → {next_share:.2f}</b>\n"
        f"(это {int(next_share * 100)}% реальных блокировок)"
    )

    buttons = [[
        {"text": "✅ Применить", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
        {"text": "❌ Отклонить", "callback_data": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    r.xadd(notify_stream, {
        "type": "report",
        "subtype": "ml_confirm_calibrator",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
    }, maxlen=50000)
    tag = "REMINDER" if is_reminder else "NEW"
    logger.info(f"Telegram proposal [{tag}]: bundle_id={bundle_id} enforce_share {cur_share:.2f} → {next_share:.2f}")


# ──────────────────────────────────────────────────────── decision logic ───── #

def _should_propose(
    *,
    next_share: float,
    stats: Dict[str, float],
    holddown_ok: bool,
    min_veto_hits: int,
) -> Tuple[bool, str]:
    """
    Returns (should_propose, reason).
    All conditions must be satisfied.
    """
    veto_count = int(stats["shadow_veto_count"])
    veto_with_outcome = int(stats["veto_with_outcome"])
    precision = stats["precision_veto"]
    veto_r_sum = stats["veto_r_sum"]
    pnl_impact = stats["pnl_impact_r"]

    precision_thr = _precision_threshold(next_share)

    if not holddown_ok:
        return False, "holddown_not_expired"

    if veto_count < min_veto_hits:
        return False, f"veto_count={veto_count} < min={min_veto_hits}"

    if veto_with_outcome < max(10, min_veto_hits // 3):
        return False, f"veto_with_outcome={veto_with_outcome} < 10"

    if precision < precision_thr:
        return False, f"precision={precision:.3f} < threshold={precision_thr:.3f}"

    # For first step (0→0.05): require veto_r_sum < 0 (blocked trades were losers)
    if next_share <= 0.05 and veto_r_sum >= 0:
        return False, f"veto_r_sum={veto_r_sum:.3f} >= 0 (not blocking losers)"

    # For higher steps: require positive pnl impact (enforce helped more than hurt)
    if next_share > 0.05 and pnl_impact <= 0:
        return False, f"pnl_impact={pnl_impact:.3f} <= 0"

    return True, "ok"


# ────────────────────────────────────────────────────────────── main ──────── #

def main() -> None:
    ap = argparse.ArgumentParser(description="ML Confirm Gate Calibrator")
    ap.add_argument(
        "--hours", type=float,
        default=float(os.getenv("ML_CAL_HOURS", "168")),
        help="Lookback window in hours (default 168 = 7 days)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Compute metrics but do not write bundle or send Telegram",
    )
    ap.add_argument(
        "--force-propose", action="store_true",
        help="Skip threshold checks and always propose next ladder step",
    )
    ap.add_argument(
        "--min-veto-hits", type=int,
        default=int(os.getenv("ML_CAL_MIN_VETO_HITS", "30")),
        help="Minimum shadow-veto count to consider proposing",
    )
    ap.add_argument(
        "--max-scan", type=int,
        default=int(os.getenv("ML_CAL_MAX_SCAN", "500000")),
        help="Maximum stream entries to scan",
    )
    args = ap.parse_args()

    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    pending_key = os.getenv("ML_CAL_PENDING_KEY", "meta:ml_cal:pending")
    step_ts_key = os.getenv("ML_CAL_STEP_TS_KEY", "meta:ml_cal:last_step_ms")
    holddown_h = float(os.getenv("ML_CAL_ENFORCE_HOLDDOWN_H", "72"))
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_ttl = int(os.getenv("ML_CAL_BUNDLE_TTL_SEC", "86400"))
    reminder_sec = int(os.getenv("ML_CAL_REMINDER_SEC", "1800"))

    logger.info(
        f"ML Confirm Gate Calibrator | hours={args.hours} "
        f"min_veto_hits={args.min_veto_hits} "
        f"max_scan={args.max_scan} "
        f"dry_run={args.dry_run} "
        f"force_propose={args.force_propose} "
        f"reminder_sec={reminder_sec}"
    )

    r = _get_redis()

    # ── 1) Idempotency guard ──────────────────────────────────────────────── #
    if not args.dry_run and not args.force_propose:
        if r.exists(pending_key):
            logger.info(f"Pending proposal already exists ({pending_key}). Skipping.")
            sys.exit(0)

    # ── 2) Load current enforce_share from champion cfg ───────────────────── #
    cfg = _load_champion_cfg(r, champion_key)
    if not cfg:
        logger.warning(f"Champion cfg not found at {champion_key}. Nothing to calibrate.")
        sys.exit(0)

    cur_share = _f(cfg.get("enforce_share"), 0.0)
    logger.info(f"Current champion model: kind={cfg.get('kind')} enforce_share={cur_share:.3f} mode={cfg.get('mode')}")

    # ── 3) Determine next ladder step ─────────────────────────────────────── #
    next_share = _ladder_next(cur_share)
    if next_share is None:
        logger.info(f"enforce_share={cur_share:.2f} already at maximum (1.0). Nothing to promote.")
        sys.exit(0)

    # ── 4) Holddown check ─────────────────────────────────────────────────── #
    hd_ok, elapsed_h = _holddown_ok(r, step_ts_key, holddown_h)
    logger.info(f"Holddown: elapsed={elapsed_h:.1f}h required={holddown_h}h ok={hd_ok}")

    # ── 5) Collect analytics ──────────────────────────────────────────────── #
    stats = _collect_analytics(r, args.hours, args.max_scan)

    logger.info(
        f"Stats over {args.hours}h:\n"
        f"  total_events      = {int(stats['total_events'])}\n"
        f"  shadow_veto_all   = {int(stats['shadow_veto_all'])}  (all allow=0)\n"
        f"  ok_rule_skip      = {int(stats['ok_rule_skip_count'])}  (allow=0, ok_rule=0 → rule-blocked, ML irrelevant)\n"
        f"  shadow_veto_count = {int(stats['shadow_veto_count'])}  (allow=0, ok_rule=1 → pure ML-veto)\n"
        f"  veto_with_outcome = {int(stats['veto_with_outcome'])}\n"
        f"  veto_negative     = {int(stats['veto_negative'])}\n"
        f"  precision_veto    = {stats['precision_veto']:.1%}\n"
        f"  veto_r_sum        = {stats['veto_r_sum']:+.3f}R\n"
        f"  pnl_impact_r      = {stats['pnl_impact_r']:+.3f}R\n"
        f"  mean_r_vetoed     = {stats['mean_r_vetoed']:+.3f}R\n"
        f"  pass_with_outcome = {int(stats['pass_with_outcome'])}\n"
        f"  pass_r_sum        = {stats['pass_r_sum']:+.3f}R"
    )

    # ── 6) Decision ───────────────────────────────────────────────────────── #
    if args.force_propose:
        should_go = True
        reason = "force_propose"
        logger.info("Forcing proposal (--force-propose)")
    else:
        should_go, reason = _should_propose(
            next_share=next_share,
            stats=stats,
            holddown_ok=hd_ok,
            min_veto_hits=args.min_veto_hits,
        )

    logger.info(f"Decision: should_propose={should_go} reason={reason}")

    if not should_go:
        logger.info(f"Thresholds NOT met ({reason}). No proposal sent.")
        sys.exit(0)

    # ── 7) Build and store bundle ─────────────────────────────────────────── #
    bid, sig, bundle = _build_proposal_bundle(
        champion_key=champion_key,
        cfg=cfg,
        next_share=next_share,
        secret=secret,
        ttl=bundle_ttl,
    )

    if args.dry_run:
        logger.info(
            f"DRY-RUN: Would create bundle {bid} and send Telegram proposal.\n"
            f"  enforce_share: {cur_share:.3f} → {next_share:.3f}\n"
            f"  precision: {stats['precision_veto']:.1%}\n"
            f"  pnl_impact: {stats['pnl_impact_r']:+.3f}R"
        )
        sys.exit(0)

    # Write bundle to Redis
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=bundle_ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=bundle_ttl)
    logger.info(f"Bundle {bid} stored in Redis (TTL={bundle_ttl}s)")

    # Idempotency lock (23h) — prevent duplicate proposals
    r.set(pending_key, json.dumps({
        "bundle_id": bid,
        "cur_share": cur_share,
        "next_share": next_share,
        "ts_ms": _now_ms(),
    }, separators=(",", ":")), ex=82800)  # 23h

    # ── 8) Send initial Telegram notification ─────────────────────────────── #
    _send_telegram_proposal(
        r,
        bundle_id=bid,
        sig=sig,
        cur_share=cur_share,
        next_share=next_share,
        hours=args.hours,
        stats=stats,
        notify_stream=notify_stream,
    )

    # ── 9) Reminder loop: resend until approve/reject ─────────────────────── #
    _wait_for_decision(
        r,
        bundle_id=bid,
        sig=sig,
        cur_share=cur_share,
        next_share=next_share,
        hours=args.hours,
        stats=stats,
        notify_stream=notify_stream,
        reminder_sec=reminder_sec,
        step_ts_key=step_ts_key,
        pending_key=pending_key,
        bundle_ttl=bundle_ttl,
    )


def _wait_for_decision(
    r: redis.Redis,
    *,
    bundle_id: str,
    sig: str,
    cur_share: float,
    next_share: float,
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
            # Bundle TTL expired — no decision made, exit
            logger.warning(f"Bundle {bundle_id} expired (TTL). Exiting reminder loop.")
            r.delete(pending_key)
            return

        if status == "APPLIED":
            # Decision: approved → update holddown, send confirmation
            r.set(step_ts_key, str(_now_ms()))
            logger.info(f"Bundle {bundle_id} APPLIED. Holddown timestamp updated.")
            _send_decision_notification(
                r,
                notify_stream=notify_stream,
                decision="approved",
                cur_share=cur_share,
                next_share=next_share,
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
                cur_share=cur_share,
                next_share=next_share,
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
            cur_share=cur_share,
            next_share=next_share,
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
    cur_share: float,
    next_share: float,
    bundle_id: str,
) -> None:
    """
    Sends detailed Telegram notification after approve/reject decision.
    """
    if decision == "approved":
        text = (
            f"<b>✅ ML Confirm Gate — enforce_share обновлён</b>\n\n"
            f"<code>enforce_share</code>: <b>{cur_share:.2f} → {next_share:.2f}</b>\n"
            f"({int(next_share * 100)}% реальных блокировок)\n\n"
            f"Изменения применены.\n"
            f"Следующий шаг: calibrator предложит дальнейшее повышение\n"
            f"через ≥72ч при выполнении precision-порога.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    else:
        text = (
            f"<b>❌ ML Confirm Gate — предложение отклонено</b>\n\n"
            f"<code>enforce_share</code> остаётся <b>{cur_share:.2f}</b>\n"
            f"(предлагалось {next_share:.2f})\n\n"
            f"Calibrator предложит снова в следующем цикле\n"
            f"(если precision-порог по-прежнему выполнен).\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )

    r.xadd(notify_stream, {
        "type": "report",
        "subtype": "ml_confirm_calibrator_decision",
        "ts": str(_now_ms()),
        "text": text,
        "parse_mode": "HTML",
    }, maxlen=50000)
    logger.info(f"Decision notification sent: {decision} bundle={bundle_id}")


if __name__ == "__main__":
    main()
