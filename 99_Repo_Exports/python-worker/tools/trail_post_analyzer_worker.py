#!/usr/bin/env python3
from __future__ import annotations
"""
Trail Post-Analyzer & Calibrator Worker — unified timer entry point.

Runs Steps 1+2+3 + Approve/Reject flow:
  1. TrailPostAnalyzer — MFE/MAE analysis per symbol × regime
  2. Telegram report (if TRAIL_ANALYZER_NOTIFY=1)
  3. TrailCalibrator — compute optimal trailing params (always writes as shadow)
  4. Send Telegram report with Approve ✅ / Reject ❌ buttons
  5. Reminder loop: re-send every 30min if no response

On Approve (handled by BotCallbackPoller in notify_worker.py):
  → All trail:calib:{sym}:{regime} keys get mode=enforce
  → Executor starts using calibrated params

On Reject:
  → Params remain in shadow mode

Usage:
  python3 -m tools.trail_post_analyzer_worker --once
  python3 -m tools.trail_post_analyzer_worker --loop --interval 21600
"""
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time

import redis

from common.log import setup_logger
from services.trail_post_analyzer import TrailPostAnalyzer, TrailAnalyzerConfig
from services.trail_calibrator import TrailCalibrator, TrailCalibratorConfig, CalibratedTrailParams
from services.trail_shadow_simulator import TrailShadowSimulator, ShadowSimConfig
from services.trail_stability_tracker import TrailStabilityTracker, StabilityConfig

logger = setup_logger("TrailWorker")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram") or "notify:telegram"
PENDING_PREFIX = "trail:calib:pending"
PENDING_TTL = int(os.getenv("TRAIL_CALIB_PENDING_TTL_SEC", "86400") or 86400)
REMINDER_SEC = int(os.getenv("TRAIL_CALIB_REMINDER_SEC", "1800") or 1800)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


APPROVAL_REQUIRED = _env_bool("TRAIL_CALIB_APPROVAL_REQUIRED", True)
# Auto-approve: if True, symbols with BETTER/NEUTRAL shadow (delta>=0) are
# automatically switched to mode=enforce without waiting for Telegram button.
AUTO_APPROVE = _env_bool("TRAIL_CALIB_AUTO_APPROVE", False)


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------

def _notify_telegram(r: redis.Redis, message: str, buttons: list | None = None) -> None:
    """Publish message to notify:telegram stream with optional inline buttons."""
    fields: dict[str, str] = {
        "type": "report",
        "text": message,
        "parse_mode": "HTML",
        "source": "trail_calibrator",
    }
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    try:
        r.xadd(NOTIFY_STREAM, fields, maxlen=50_000)
        logger.info("Telegram report sent to %s (buttons=%s)", NOTIFY_STREAM, buttons is not None)
    except Exception as e:
        logger.error("Failed to publish to %s: %s", NOTIFY_STREAM, e)


def _build_approval_buttons(run_id: str) -> list:
    """Build inline keyboard with Approve/Reject buttons."""
    return [[
        {"text": "✅ Approve (enforce)", "callback": f"trail_approve:{run_id}"},
        {"text": "❌ Reject (shadow)",   "callback": f"trail_reject:{run_id}"},
    ]]


# ---------------------------------------------------------------------------
# Pending approval management
# ---------------------------------------------------------------------------

def _create_pending(
    r: redis.Redis,
    run_id: str,
    params: list[CalibratedTrailParams],
    report: str,
    *,
    shadow_summary: dict | None = None,
    stability_summary: dict | None = None,
    shadow_results: list | None = None,
) -> None:
    """Store pending approval in Redis with shadow/stability context.

    shadow_results: list of ShadowSimResult objects from TrailShadowSimulator.
    Stored as shadow_per_symbol dict for per-symbol selective enforcement.
    """
    key = f"{PENDING_PREFIX}:{run_id}"

    # Build per-symbol param details for confirmation message
    param_details = []
    for p in params:
        param_details.append({
            "symbol": p.symbol,
            "regime": p.regime,
            "callback_atr_mult": p.callback_atr_mult,
            "activate_offset_bps": p.activate_offset_bps,
            "min_profit_lock_r": p.min_profit_lock_r,
            "confidence": p.confidence,
            "n_total": p.n_total,
        })

    # Build per-symbol shadow results for selective enforce on approve
    shadow_per_symbol: dict[str, dict] = {}
    if shadow_results:
        for sr in shadow_results:
            sym_key = f"{sr.symbol}:{sr.regime}"
            shadow_per_symbol[sym_key] = {
                "symbol": sr.symbol,
                "regime": sr.regime,
                "delta_pnl_r": sr.delta_pnl_r,
                "recommendation": sr.recommendation,
                "n_trades": sr.n_trades,
                "shadow_sharpe": sr.shadow_sharpe,
            }

    summary = {
        "run_id": run_id,
        "status": "PENDING",
        "created_at_ms": get_ny_time_millis(),
        "last_reminder_ms": get_ny_time_millis(),
        "n_params": len(params),
        "symbols": list({p.symbol for p in params}),
        "report": report,
        "param_details": param_details,
        "shadow_summary": shadow_summary or {},
        "stability_summary": stability_summary or {},
        "shadow_per_symbol": shadow_per_symbol,
    }
    try:
        r.set(key, json.dumps(summary, ensure_ascii=False), ex=PENDING_TTL)
        logger.info("Created pending approval: %s (%d params, %d shadow symbols)", run_id, len(params), len(shadow_per_symbol))
    except Exception as e:
        logger.error("Failed to create pending: %s", e)


def _check_and_send_reminders(r: redis.Redis) -> None:
    """Scan for pending approvals and re-send reminders every REMINDER_SEC."""
    try:
        cursor = 0
        now_ms = get_ny_time_millis()
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{PENDING_PREFIX}:*", count=10000)
            for key in keys:
                raw = r.get(key)
                if not raw:
                    continue
                try:
                    pending = json.loads(raw)
                except Exception:
                    continue

                if pending.get("status") != "PENDING":
                    continue

                last_reminder = int(pending.get("last_reminder_ms", 0))
                if (now_ms - last_reminder) < REMINDER_SEC * 1000:
                    continue

                # Time to remind
                run_id = pending.get("run_id", "unknown")
                report = pending.get("report", "")
                elapsed_min = (now_ms - int(pending.get("created_at_ms", now_ms))) // 60_000

                reminder_text = (
                    f"⏰ <b>REMINDER</b> — Trail calibration pending approval ({elapsed_min}min ago)\n\n"
                    f"{report}"
                )
                buttons = _build_approval_buttons(run_id)
                _notify_telegram(r, reminder_text, buttons=buttons)

                # Update last_reminder_ms
                pending["last_reminder_ms"] = now_ms
                r.set(key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                logger.info("Sent reminder for pending %s (%d min ago)", run_id, elapsed_min)

            if cursor == 0:
                break
    except Exception as e:
        logger.error("Reminder check failed: %s", e)


# ---------------------------------------------------------------------------
# Auto-approve / selective enforce
# ---------------------------------------------------------------------------

def _apply_selective_enforce(
    r: redis.Redis,
    run_id: str,
    shadow_results: list,
    params: list,
    shadow_summary_data: dict,
    stability_summary_data: dict,
    calib_prefix: str,
) -> None:
    """
    Automatically enforce symbols with positive statistics:
      - shadow recommendation is BETTER or NEUTRAL
      - delta_pnl_r >= 0

    Sends Telegram notification with enforced/skipped breakdown.
    Called when TRAIL_CALIB_AUTO_APPROVE=1.
    """
    # Build per-symbol shadow map
    shadow_per_symbol: dict[str, dict] = {}
    for sr in shadow_results:
        sym_key = f"{sr.symbol}:{sr.regime}"
        shadow_per_symbol[sym_key] = {
            "symbol": sr.symbol,
            "regime": sr.regime,
            "delta_pnl_r": sr.delta_pnl_r,
            "recommendation": sr.recommendation,
            "n_trades": sr.n_trades,
            "shadow_sharpe": sr.shadow_sharpe,
        }

    enforced_keys: list[dict] = []
    skipped_keys: list[dict] = []

    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{calib_prefix}:*", count=10000)
            for k in keys:
                # Skip auxiliary keys
                if ":pending:" in k or ":stability:" in k or ":shadow:" in k:
                    continue
                suffix = k.replace(f"{calib_prefix}:", "", 1)
                shadow_info = shadow_per_symbol.get(suffix, {})
                recommendation = shadow_info.get("recommendation", "")
                delta_r = float(shadow_info.get("delta_pnl_r", 0.0))

                if shadow_info and delta_r < 0:
                    # Negative delta → keep shadow
                    skipped_keys.append({
                        "key": suffix,
                        "delta_r": delta_r,
                        "reason": recommendation or "NEGATIVE_DELTA",
                    })
                else:
                    # delta >= 0 or no shadow data (fail-open) → enforce
                    try:
                        r.hset(k, "mode", "enforce")
                        enforced_keys.append({
                            "key": suffix,
                            "delta_r": delta_r,
                            "recommendation": recommendation or "NO_DATA",
                        })
                    except Exception as e:
                        logger.error("Failed to enforce %s: %s", k, e)
            if cursor == 0:
                break
    except Exception as e:
        logger.error("Auto-approve scan failed: %s", e)
        return

    n_enforced = len(enforced_keys)
    n_skipped = len(skipped_keys)

    # Build enforced block
    enforced_lines = [
        f"  <code>{ek['key']}</code>: Δ={ek['delta_r']:+.3f}R ({ek['recommendation']})"
        for ek in sorted(enforced_keys, key=lambda x: x["key"])
    ]
    enforced_block = "\n".join(enforced_lines) if enforced_lines else "  (nothing)"

    # Build skipped block
    skipped_lines = [
        f"  <code>{sk['key']}</code>: Δ={sk['delta_r']:+.3f}R ({sk['reason']})"
        for sk in sorted(skipped_keys, key=lambda x: x["key"])
    ]
    skipped_block = "\n".join(skipped_lines) if skipped_lines else "  (none)"

    # Shadow summary line
    shadow_line = ""
    if shadow_summary_data:
        shadow_line = (
            f"\n📊 <b>Shadow P&L:</b> "
            f"{shadow_summary_data.get('n_better', 0)}✅ better, "
            f"{shadow_summary_data.get('n_neutral', 0)}🔄 neutral, "
            f"{shadow_summary_data.get('n_worse', 0)}⚠️ worse | "
            f"avg Δ={shadow_summary_data.get('avg_delta_r', 0):+.3f}R\n"
        )

    # Stability summary line
    stability_line = ""
    if stability_summary_data:
        stability_line = (
            f"📏 <b>Stability:</b> "
            f"{stability_summary_data.get('n_stable', 0)}/"
            f"{stability_summary_data.get('n_total', 0)} stable\n"
        )

    confirm_text = (
        f"🤖 <b>Trail Calibration AUTO-APPROVED (selective)</b>\n"
        f"\n"
        f"<b>✅ Enforced ({n_enforced}):</b>\n"
        f"{enforced_block}\n\n"
        f"<b>⏭️ Skipped ({n_skipped}, kept shadow):</b>\n"
        f"{skipped_block}\n\n"
        f"{shadow_line}"
        f"{stability_line}"
        f"<i>Auto-promote: symbols with delta&gt;=0 switched to mode=enforce.</i>\n"
        f"<i>To disable: set <code>TRAIL_CALIB_AUTO_APPROVE=0</code></i>\n\n"
        f"Run ID: <code>{run_id}</code>"
    )
    _notify_telegram(r, confirm_text)
    logger.info(
        "Auto-approve done: run_id=%s enforced=%d skipped=%d",
        run_id, n_enforced, n_skipped,
    )


# ---------------------------------------------------------------------------
# Main cycle
# ---------------------------------------------------------------------------

def run_once(r: redis.Redis, symbols: list[str]) -> None:
    """Run one cycle: analysis → calibration → shadow sim → stability → report."""
    ts_start = time.time()

    # Step 1: Post-Analysis
    analyzer_cfg = TrailAnalyzerConfig.from_env()
    analyzer = TrailPostAnalyzer(r, cfg=analyzer_cfg)
    buckets = analyzer.run(symbols=symbols if symbols else None)

    # Step 2: Analysis Telegram report
    if buckets and analyzer_cfg.notify:
        report = TrailPostAnalyzer.format_telegram_report(buckets)
        _notify_telegram(r, report)

    # Step 3: Calibration (always writes as shadow initially)
    calib_cfg = TrailCalibratorConfig.from_env()
    calibrator = TrailCalibrator(r, cfg=calib_cfg)
    params = calibrator.run(symbols=symbols if symbols else None)

    # Step 3.5: Shadow Simulation (virtual P&L A/B test)
    shadow_results = []
    if params:
        try:
            shadow_cfg = ShadowSimConfig.from_env()
            simulator = TrailShadowSimulator(r, cfg=shadow_cfg)
            trades = analyzer._load_trades()
            if symbols:
                syms = {s.upper() for s in symbols}
                trades = [t for t in trades if t.symbol in syms]
            trades_by_bucket: dict[str, list] = {}
            for t in trades:
                key = f"{t.symbol}:{t.regime}"
                trades_by_bucket.setdefault(key, []).append(t)
            shadow_results = simulator.run(trades_by_bucket)
        except Exception as e:
            logger.error("Shadow simulation failed (non-fatal): %s", e)

    # Step 3.6: Stability Tracking
    stability_reports = []
    if params:
        try:
            stab_cfg = StabilityConfig.from_env()
            tracker = TrailStabilityTracker(r, cfg=stab_cfg)
            stability_reports = tracker.record_and_assess(params)
        except Exception as e:
            logger.error("Stability tracking failed (non-fatal): %s", e)

    # Step 4: Enhanced report with calibration + shadow + stability + approve/reject
    if params and analyzer_cfg.notify:
        calib_report = TrailCalibrator.format_telegram_report(params)

        # Append shadow comparison
        shadow_section = ""
        shadow_summary_data: dict = {}
        if shadow_results:
            shadow_section = "\n\n" + TrailShadowSimulator.format_telegram_report(shadow_results)
            shadow_summary_data = {
                "n_better": sum(1 for r in shadow_results if r.recommendation == "BETTER"),
                "n_worse": sum(1 for r in shadow_results if r.recommendation == "WORSE"),
                "n_neutral": sum(1 for r in shadow_results if r.recommendation == "NEUTRAL"),
                "avg_delta_r": round(sum(r.delta_pnl_r for r in shadow_results) / len(shadow_results), 4),
            }

        # Append stability assessment
        stability_section = ""
        stability_summary_data: dict = {}
        if stability_reports:
            stability_section = "\n\n" + TrailStabilityTracker.format_telegram_report(stability_reports)
            n_stable = sum(1 for s in stability_reports if s.is_stable)
            stability_summary_data = {
                "n_stable": n_stable,
                "n_total": len(stability_reports),
                "all_stable": n_stable == len(stability_reports),
            }

        # Build readiness verdict
        if shadow_summary_data and shadow_results:
            n_total_shadow = len(shadow_results)
            worse_ratio = shadow_summary_data.get("n_worse", 0) / n_total_shadow if n_total_shadow > 0 else 0.0
            avg_delta = shadow_summary_data.get("avg_delta_r", 0.0)
            has_critical_drop = any(r.delta_pnl_r < -0.3 for r in shadow_results)
            shadow_ok = (avg_delta >= -0.05) and (worse_ratio <= 0.5) and not has_critical_drop
        else:
            shadow_ok = True

        stability_ok = stability_summary_data.get("all_stable", False) if stability_summary_data else False
        ready = shadow_ok and stability_ok
        verdict = "🟢 READY FOR ENFORCE" if ready else "🟡 NOT READY — continue shadow"

        calib_prefix_env = os.getenv("TRAIL_CALIB_KEY_PREFIX", "trail:calib") or "trail:calib"
        run_id = f"{int(time.time())}_{len(params)}p"

        if AUTO_APPROVE:
            # ── AUTO-APPROVE PATH ──────────────────────────────────────────
            # Enforce immediately for symbols with positive statistics,
            # then send Telegram notification (no button press needed).
            full_report = (
                f"{calib_report}"
                f"{shadow_section}"
                f"{stability_section}\n\n"
                f"{'─' * 30}\n"
                f"<b>Verdict:</b> {verdict}\n"
                f"🤖 <b>Auto-approve enabled</b> — enforcing positive symbols..."
            )
            _notify_telegram(r, full_report)

            _apply_selective_enforce(
                r=r,
                run_id=run_id,
                shadow_results=shadow_results,
                params=params,
                shadow_summary_data=shadow_summary_data,
                stability_summary_data=stability_summary_data,
                calib_prefix=calib_prefix_env,
            )

            # Store pending (for audit/history), mark auto-approved
            _create_pending(
                r, run_id, params, calib_report,
                shadow_summary=shadow_summary_data,
                stability_summary=stability_summary_data,
                shadow_results=shadow_results,
            )
            # Mark as auto-approved in pending key
            try:
                pending_key = f"{PENDING_PREFIX}:{run_id}"
                raw = r.get(pending_key)
                if raw:
                    pending = json.loads(raw)
                    pending["status"] = "AUTO_APPROVED"
                    pending["approved_by"] = "auto"
                    pending["approved_at_ms"] = int(time.time() * 1000)
                    r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
            except Exception as e:
                logger.error("Failed to mark auto-approved pending: %s", e)

        elif APPROVAL_REQUIRED:
            # ── MANUAL APPROVAL PATH ───────────────────────────────────────
            buttons = _build_approval_buttons(run_id)

            full_report = (
                f"{calib_report}"
                f"{shadow_section}"
                f"{stability_section}\n\n"
                f"{'─' * 30}\n"
                f"<b>Verdict:</b> {verdict}\n\n"
                f"📋 <b>Action required:</b> Approve to switch <code>mode=enforce</code> "
                f"(only symbols with BETTER/NEUTRAL shadow will be enforced)\n"
                f"Run ID: <code>{run_id}</code>"
            )
            _notify_telegram(r, full_report, buttons=buttons)

            # Store pending with shadow/stability context + per-symbol shadow results
            _create_pending(
                r, run_id, params, calib_report,
                shadow_summary=shadow_summary_data,
                stability_summary=stability_summary_data,
                shadow_results=shadow_results,
            )
        else:
            full_report = f"{calib_report}{shadow_section}{stability_section}"
            _notify_telegram(r, full_report)

    elapsed = time.time() - ts_start
    logger.info(
        "Trail worker cycle done in %.1fs: %d analysis, %d calibrations, %d shadow, %d stability",
        elapsed, len(buckets), len(params), len(shadow_results), len(stability_reports),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Trail Post-Analyzer & Calibrator Worker")
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    parser.add_argument(
        "--symbols",
        default=os.getenv("TRAIL_ANALYZER_SYMBOLS", ""),
        help="Comma-separated symbols (empty = all)",
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument(
        "--loop", action="store_true", help="Run in loop mode",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("TRAIL_ANALYZER_INTERVAL_SEC", "21600")),
        help="Loop interval in seconds (default: 6h)",
    )

    args = parser.parse_args(argv)

    try:
        r = redis.from_url(args.redis_url, decode_responses=True)
        r.ping()
    except Exception as e:
        logger.error("Cannot connect to Redis: %s", e)
        return 1

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else []

    if args.once:
        run_once(r, symbols)
        return 0

    if args.loop:
        logger.info("Starting loop mode: interval=%ds, reminder=%ds, symbols=%s",
                     args.interval, REMINDER_SEC, symbols or "all")
        last_run = 0.0
        while True:
            now = time.time()
            try:
                # Run calibration cycle at full interval
                if now - last_run >= args.interval:
                    run_once(r, symbols)
                    last_run = now

                # Check reminders every loop iteration (sleep is REMINDER_SEC/2 for responsiveness)
                _check_and_send_reminders(r)

            except Exception as e:
                logger.error("Cycle failed: %s", e)

            sleep_sec = min(REMINDER_SEC // 2, args.interval)
            logger.debug("Sleeping %ds...", sleep_sec)
            time.sleep(sleep_sec)
    else:
        # Default: run once
        run_once(r, symbols)
        return 0


if __name__ == "__main__":
    sys.exit(main())
