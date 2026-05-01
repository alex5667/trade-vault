from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Taker Flow Gate Calibrator.

Uses exported JSON data from Redis (trades and inputs) to evaluate the 7-day shadow_veto performance.
If enforcing it would have improved PnL, proposes switching `taker_flow_gate_mode`
to `enforce` via an interactive Telegram message in `notify:telegram`.

Reminder mode (--remind-pending): scans all PENDING bundles and re-sends a reminder
Telegram message every REMIND_INTERVAL_SEC seconds (default 1800 = 30 min) until the
proposal is Approved or Rejected.
"""

import argparse
import hmac
import hashlib
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from typing import List, Tuple

import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_redis_url() -> str:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis_url

def get_redis() -> redis.Redis:
    return redis.Redis.from_url(get_redis_url(), decode_responses=True)


def sign_bundle(bundle_id: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id like ml_guard_approve."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def query_shadow_veto_stats(hours: float) -> Tuple[int, int, float, float]:
    """
    Returns:
      total_trades: int
      veto_hits: int,
      veto_r_sum: float,
      global_r_sum: float
    """
    with tempfile.TemporaryDirectory() as tmp:
        trades_file = os.path.join(tmp, "trades.ndjson")
        inputs_file = os.path.join(tmp, "inputs.ndjson")
        
        redis_url = get_redis_url()
        
        # 1. Export trades
        logger.info(f"Exporting trades for {hours} hours to {trades_file}")
        try:
            subprocess.check_call([
                sys.executable, "tools/export_trade_closed_ndjson.py",
                "--since-hours", str(hours),
                "--out", trades_file,
                "--redis-url", redis_url,
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to export trades: {e}")
            return 0, 0, 0.0, 0.0
            
        # 2. Export OF inputs (use hours + 24 to be safe about timeframe limits)
        logger.info(f"Exporting OF inputs for {hours + 24} hours to {inputs_file}")
        try:
            subprocess.check_call([
                sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
                "--since-hours", str(hours + 24),
                "--out", inputs_file,
                "--redis-url", redis_url,
                "--quiet"
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to export inputs: {e}")
            return 0, 0, 0.0, 0.0

        # 3. Parse trades
        trades_by_sid = {}
        total_trades = 0
        global_r_sum = 0.0
        
        with open(trades_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                try:
                    t = json.loads(line)
                    sid = t.get("sid")
                    if sid:
                        trades_by_sid[sid] = t
                        total_trades += 1
                        global_r_sum += float(t.get("r_mult", 0.0))
                except Exception:
                    pass
                    
        # 4. Parse inputs and join
        veto_hits = 0
        veto_r_sum = 0.0
        
        with open(inputs_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                try:
                    inp = json.loads(line)
                    sid = inp.get("sid")
                    trade = trades_by_sid.get(sid)
                    
                    if trade:
                        ev = inp.get("evidence") or inp.get("evidences") or {}
                        sv = ev.get("taker_flow_gate_shadow_veto")
                        if str(sv) == "1":
                            veto_hits += 1
                            veto_r_sum += float(trade.get("r_mult", 0.0))
                            # Prevent double counting if same sid exists multiple times in inputs
                            del trades_by_sid[sid] 
                except Exception:
                    pass

    return total_trades, veto_hits, veto_r_sum, global_r_sum


def create_and_send_proposal(
    r: redis.Redis,
    hours: float,
    veto_hits: int,
    veto_r_sum: float,
    global_r_sum: float
):
    bundle_id = f"taker_enforce_{int(time.time())}"
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    sig = sign_bundle(bundle_id, secret)

    # 1. Store bundle in Redis
    ops = [
        {"op": "HSET", "key": "config:orderflow:GLOBAL", "field": "taker_flow_gate_mode", "value": "enforce"}
    ]
    
    meta = {
        "title": "Enable TakerFlowGate enforce mode",
        "details": {
            "period": f"Last {hours} hours",
            "veto_hits": veto_hits,
            "veto_R_sum": round(veto_r_sum, 2)
        }
    }
    
    bundle = {
        "id": bundle_id,
        "created_ms": get_ny_time_millis(),
        "ops": ops,
        "meta": meta
    }
    
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle))
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=86400) # 24h expire

    # 2. Add Telegram message
    text = (
        f"<b>Taker Flow Gate Calibrator</b>\n\n"
        f"За последние {hours} часов `shadow_veto` сработал на {veto_hits} трейдах.\n"
        f"Совокупный R-multiple этих сделок: <b>{veto_r_sum:.2f}R</b>.\n"
        f"Текущий общий PnL (закрытие позиций) был бы на <b>{-veto_r_sum:.2f}R</b> выше при режиме <code>enforce</code>.\n\n"
        f"Предлагаю включить `taker_flow_gate_mode=enforce` (GLOBAL)."
    )

    buttons = [
        [
            {"text": "✅ Approve", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback_data": f"recs:reject:{bundle_id}:{sig}"}
        ]
    ]

    r.xadd("notify:telegram", {
        "type": "report",
        "subtype": "taker_calibrator",
        "ts": str(get_ny_time_millis()),
        "text": text,
        "parse_mode": "HTML",
        "buttons": json.dumps(buttons),
    }, maxlen=50000)
    logger.info(f"Telegram proposal sent for bundle_id {bundle_id}")
    # Record first reminded_at so reminder loop treats it as freshly sent
    r.set(f"recs:reminded_at:{bundle_id}", str(int(time.time())), ex=86400)


# ---------------------------------------------------------------------------
# Reminder logic
# ---------------------------------------------------------------------------

DEFAULT_REMIND_INTERVAL_SEC: int = 1800  # 30 minutes


def _get_pending_bundle_ids(r: redis.Redis) -> List[str]:
    """Return bundle_ids that still have status=PENDING."""
    pending: List[str] = []
    # recs:status:* keys hold PENDING / APPROVED / REJECTED
    for key in r.scan_iter("recs:status:taker_enforce_*"):
        status = r.get(key)
        if status == "PENDING":
            bundle_id = key.removeprefix("recs:status:")
            pending.append(bundle_id)
    return pending


def _send_reminder(r: redis.Redis, bundle_id: str, remind_number: int) -> None:
    """Re-send the Telegram message for an unresolved bundle."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    sig = sign_bundle(bundle_id, secret)

    raw = r.get(f"recs:bundle:{bundle_id}")
    if not raw:
        logger.warning(f"Bundle {bundle_id} not found in Redis, skipping reminder.")
        return

    bundle = json.loads(raw)
    meta = bundle.get("meta", {})
    details = meta.get("details", {})
    period = details.get("period", "N/A")
    veto_hits = details.get("veto_hits", "?")
    veto_r = details.get("veto_R_sum", 0.0)

    text = (
        f"⏰ <b>Reminder #{remind_number} — TakerFlowGate ожидает решения</b>\n\n"
        f"Предложение создано <code>{bundle_id}</code>.\n"
        f"Период: {period} | shadow_veto hits: <b>{veto_hits}</b> | R-sum: <b>{veto_r:.2f}R</b>\n\n"
        f"PnL был бы на <b>{-veto_r:.2f}R</b> выше при режиме <code>enforce</code>.\n\n"
        f"Пожалуйста, подтвердите или отклоните:"
    )

    buttons = [
        [
            {"text": "✅ Approve", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
            {"text": "❌ Reject",  "callback_data": f"recs:reject:{bundle_id}:{sig}"}
        ]
    ]

    r.xadd("notify:telegram", {
        "type": "report",
        "subtype": "taker_calibrator_reminder",
        "ts": str(get_ny_time_millis()),
        "text": text,
        "parse_mode": "HTML",
        "buttons": json.dumps(buttons),
    }, maxlen=50000)
    r.set(f"recs:reminded_at:{bundle_id}", str(int(time.time())), ex=86400)
    logger.info(f"Reminder #{remind_number} sent for bundle_id {bundle_id}")


def remind_pending_proposals(
    r: redis.Redis,
    interval_sec: int = DEFAULT_REMIND_INTERVAL_SEC,
    dry_run: bool = False,
) -> None:
    """
    Check all PENDING taker_enforce bundles.
    Re-send a Telegram reminder if the bundle has not been reminded
    within `interval_sec` seconds.
    """
    pending_ids = _get_pending_bundle_ids(r)
    if not pending_ids:
        logger.info("No PENDING taker_enforce bundles found — nothing to remind.")
        return

    now = int(time.time())
    for bundle_id in pending_ids:
        reminded_at_raw = r.get(f"recs:reminded_at:{bundle_id}")
        last_reminded = int(reminded_at_raw) if reminded_at_raw else 0
        elapsed = now - last_reminded

        if elapsed < interval_sec:
            remaining = interval_sec - elapsed
            logger.info(
                f"Bundle {bundle_id} reminded {elapsed}s ago — next remind in {remaining}s, skipping."
            )
            continue

        # Count how many reminders have been sent (stored counter)
        remind_count_key = f"recs:remind_count:{bundle_id}"
        remind_number = int(r.incr(remind_count_key))
        r.expire(remind_count_key, 86400)

        if dry_run:
            logger.info(
                f"DRY-RUN: Would send reminder #{remind_number} for bundle {bundle_id} "
                f"(elapsed {elapsed}s >= interval {interval_sec}s)."
            )
        else:
            _send_reminder(r, bundle_id, remind_number)


def main():
    parser = argparse.ArgumentParser(description="Taker Flow Gate Calibrator")
    parser.add_argument("--hours", type=float, default=float(os.getenv("TAKER_CALIBRATOR_HOURS", "168")), help="Lookback hours")
    parser.add_argument("--dry-run", action="store_true", help="Do not send telegram message or store bundle")
    parser.add_argument("--force-propose", action="store_true", help="Send proposal even if threshold not met")
    parser.add_argument("--min-veto-hits", type=int, default=5, help="Minimum number of shadow_veto hits to propose")
    parser.add_argument("--max-r-mult", type=float, default=-1.0, help="R-multiple must be worse than this to propose (e.g. -1.0 means losers)")
    # Reminder mode
    parser.add_argument(
        "--remind-pending",
        action="store_true",
        help="Scan PENDING bundles and re-send Telegram reminder if not acted on within --remind-interval-sec.",
    )
    parser.add_argument(
        "--remind-interval-sec",
        type=int,
        default=int(os.getenv("TAKER_REMIND_INTERVAL_SEC", str(DEFAULT_REMIND_INTERVAL_SEC))),
        help="Minimum seconds between reminders (default 1800 = 30 min).",
    )
    args = parser.parse_args()

    redis_client = get_redis()

    # ------------------------------------------------------------------
    # Mode: remind-pending — check & resend unresolved proposals
    # ------------------------------------------------------------------
    if args.remind_pending:
        logger.info(f"Running in remind-pending mode (interval={args.remind_interval_sec}s).")
        remind_pending_proposals(
            redis_client,
            interval_sec=args.remind_interval_sec,
            dry_run=args.dry_run,
        )
        return

    # ------------------------------------------------------------------
    # Mode: normal calibration run
    # ------------------------------------------------------------------
    logger.info(f"Running Taker Flow Calibrator. Hours: {args.hours}, min hits: {args.min_veto_hits}, max R: {args.max_r_mult}")

    total_trades, veto_hits, veto_r_sum, global_r_sum = query_shadow_veto_stats(args.hours)

    logger.info(f"Total trades checked: {total_trades}")
    logger.info(f"Veto hits: {veto_hits}, Veto R-sum: {veto_r_sum:.2f}, Global R-sum: {global_r_sum:.2f}")

    should_propose = False
    if args.force_propose:
        should_propose = True
        logger.info("Forcing proposal due to --force-propose flag.")
    elif veto_hits >= args.min_veto_hits and veto_r_sum <= args.max_r_mult:
        should_propose = True
        logger.info(f"Thresholds met! Veto hits ({veto_hits} >= {args.min_veto_hits}) AND R-sum ({veto_r_sum:.2f} <= {args.max_r_mult}).")
    else:
        logger.info("Thresholds NOT met. No proposal needed.")

    if should_propose:
        if args.dry_run:
            logger.info("DRY-RUN: Would have created bundle and sent Telegram message.")
        else:
            create_and_send_proposal(redis_client, args.hours, veto_hits, veto_r_sum, global_r_sum)


if __name__ == "__main__":
    main()
