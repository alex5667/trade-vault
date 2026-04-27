# -*- coding: utf-8 -*-
"""
Research Guard Calibrator Service — IO layer (Redis + Telegram).

Runs hourly (from of_timers_worker.py or standalone) to evaluate whether
the G14 Strategy Research Guard should be promoted from REPORT-ONLY to ENFORCE.

Reads nightly report metrics from Redis:
  - cfg:research_guard:blocker:v1  → {blocker_active, reason, report_only}
  - metrics:strategy_research_guard:last → {psr, dsr, pbo, ...}

Promotion flow:
  1. Check nightly report freshness + PSR/DSR/PBO thresholds
  2. Build proof streak (consecutive healthy windows)
  3. If streak ≥ required → send Telegram with Enforce/Keep buttons
  4. On Approve → write STRATEGY_RESEARCH_GUARD_REPORT_ONLY=0 to Redis blocker key
  5. On Reject → reset streak, keep REPORT-ONLY

Redis keys written:
  - cfg:rg_calib:state                → JSON {mode, proof_streak, ...}
  - cfg:rg_calib:last_result          → JSON (full result for observability)
  - rg_calib:pending:{run_id}         → JSON (pending approval for Telegram)

Usage:
  - python -m services.research_guard_calibrator_service
  - Called from of_timers_worker as hourly timer task
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None  # type: ignore[assignment]

try:
    from common.log import setup_logger
    logger = setup_logger("ResearchGuardCalibrator")
except ImportError:
    import logging
    logger = logging.getLogger("ResearchGuardCalibrator")

from core.research_guard_calibrator import (
    NightlyReport,
    ResearchGuardCalibResult,
    evaluate_research_guard,
    rg_mode_to_int,
    rg_is_promotion,
    rg_is_rollback,
)
from core.redis_keys import RedisStreams as RS


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_KEY = "cfg:rg_calib:state"
RESULT_KEY = "cfg:rg_calib:last_result"
PENDING_TTL_SEC = 48 * 3600  # 48h
STATE_TTL_SEC = 14 * 24 * 3600  # 14 days

BLOCKER_KEY_DEFAULT = "cfg:research_guard:blocker:v1"
SUMMARY_KEY_DEFAULT = "metrics:strategy_research_guard:last"

NOTIFY_STREAM = RS.NOTIFY_TELEGRAM


# ---------------------------------------------------------------------------
# Prometheus metrics (lazy init, fail-safe)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Gauge, Counter

    rg_calib_psr_gauge = Gauge(
        "rg_calib_latest_psr", "Latest PSR from nightly report"
    )
    rg_calib_dsr_gauge = Gauge(
        "rg_calib_latest_dsr", "Latest DSR from nightly report"
    )
    rg_calib_pbo_gauge = Gauge(
        "rg_calib_latest_pbo", "Latest PBO from nightly report"
    )
    rg_calib_ece_gauge = Gauge(
        "rg_calib_latest_ece", "Latest ECE from nightly report"
    )
    rg_calib_brier_gauge = Gauge(
        "rg_calib_latest_brier", "Latest Brier Score from nightly report"
    )
    rg_calib_proof_streak_gauge = Gauge(
        "rg_calib_proof_streak", "Current proof streak"
    )
    rg_calib_rollback_streak_gauge = Gauge(
        "rg_calib_rollback_streak", "Current rollback streak"
    )
    rg_calib_mode_gauge = Gauge(
        "rg_calib_mode", "0=report, 1=enforce"
    )
    rg_calib_report_age_gauge = Gauge(
        "rg_calib_report_age_sec", "Age of latest nightly report in seconds"
    )
    rg_calib_promote_total = Counter(
        "rg_calib_promote_total", "Promote events"
    )
    rg_calib_rollback_total = Counter(
        "rg_calib_rollback_total", "Rollback events"
    )
    rg_calib_run_total = Counter(
        "rg_calib_run_total", "Calibration runs", ["result"]
    )
    _HAS_PROM = True
except ImportError:
    _HAS_PROM = False


def _update_prometheus(result: ResearchGuardCalibResult) -> None:
    if not _HAS_PROM:
        return
    try:
        rg_calib_psr_gauge.set(result.latest_psr)
        rg_calib_dsr_gauge.set(result.latest_dsr)
        rg_calib_pbo_gauge.set(result.latest_pbo)
        rg_calib_ece_gauge.set(result.latest_ece)
        rg_calib_brier_gauge.set(result.latest_brier)
        rg_calib_proof_streak_gauge.set(result.proof_streak)
        rg_calib_rollback_streak_gauge.set(result.rollback_streak)
        rg_calib_mode_gauge.set(rg_mode_to_int(result.effective_mode))
        rg_calib_report_age_gauge.set(result.latest_report_age_sec)
        rg_calib_run_total.labels(result=result.recommend).inc()
        if result.is_ready_for_promote:
            rg_calib_promote_total.inc()
        if result.is_rollback:
            rg_calib_rollback_total.inc()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Redis loaders
# ---------------------------------------------------------------------------

def _load_nightly_report(redis_client: Any) -> NightlyReport:
    """Load latest nightly report from Redis blocker + summary keys."""
    blocker_key = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", BLOCKER_KEY_DEFAULT)
    summary_key = os.getenv("STRATEGY_RESEARCH_GUARD_SUMMARY_KEY", SUMMARY_KEY_DEFAULT)

    report = NightlyReport()

    try:
        # Read summary metrics
        summary_raw = redis_client.get(summary_key)
        if summary_raw:
            summary = json.loads(summary_raw)
            report.psr = float(summary.get("psr", 0.0))
            report.dsr = float(summary.get("dsr", 0.0))
            report.pbo = float(summary.get("pbo", 0.0))
            report.ece = float(summary.get("ece", 0.0))
            report.brier = float(summary.get("brier", 0.0))
            report.report_ts = int(summary.get("timestamp", 0) or
                                   summary.get("ts", 0) or
                                   summary.get("updated_ts_ms", 0) / 1000)
            report.has_data = True

        # Read blocker state
        blocker_raw = redis_client.get(blocker_key)
        if blocker_raw:
            blocker = json.loads(blocker_raw)
            report.blocker_active = bool(blocker.get("blocker_active", False))
            if not report.has_data:
                report.has_data = True

        # Compute age
        if report.report_ts > 0:
            report.report_age_sec = max(0.0, time.time() - report.report_ts)
        elif report.has_data:
            report.report_age_sec = 0.0  # Unknown but data exists

    except Exception as e:
        logger.warning("Failed to load nightly report: %s", e)

    return report


def _load_state(redis_client: Any) -> Dict[str, Any]:
    try:
        raw = redis_client.get(STATE_KEY)
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning("Failed to load RG calibrator state: %s", e)
    return {}


def _save_state(
    redis_client: Any,
    result: ResearchGuardCalibResult,
    run_id: str,
) -> None:
    state = {
        "mode": result.effective_mode,
        "proof_streak": result.proof_streak,
        "rollback_streak": result.rollback_streak,
        "latest_psr": round(result.latest_psr, 4),
        "latest_dsr": round(result.latest_dsr, 4),
        "latest_pbo": round(result.latest_pbo, 4),
        "latest_ece": round(result.latest_ece, 4),
        "latest_brier": round(result.latest_brier, 4),
        "last_recommend": result.recommend,
        "last_reason": result.reason,
        "run_id": run_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_ms": get_ny_time_millis(),
    }
    try:
        pipe = redis_client.pipeline()
        pipe.set(STATE_KEY, json.dumps(state, default=str), ex=STATE_TTL_SEC)
        pipe.set(RESULT_KEY, json.dumps(result.as_dict(), default=str), ex=STATE_TTL_SEC)
        pipe.execute()
    except Exception as e:
        logger.error("Failed to save RG calibrator state: %s", e)


def _apply_blocker_mode(
    redis_client: Any,
    mode: str,
) -> None:
    """Write REPORT_ONLY flag to the blocker Redis key."""
    blocker_key = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", BLOCKER_KEY_DEFAULT)
    report_only = 1 if mode == "report" else 0

    try:
        raw = redis_client.get(blocker_key)
        if raw:
            blocker = json.loads(raw)
        else:
            blocker = {}

        blocker["report_only"] = report_only
        blocker["rg_calib_mode"] = mode
        blocker["rg_calib_updated_ms"] = get_ny_time_millis()

        redis_client.set(blocker_key, json.dumps(blocker, default=str))
        logger.info(
            "Applied blocker mode: report_only=%d, mode=%s → %s",
            report_only, mode, blocker_key,
        )
    except Exception as e:
        logger.error("Failed to apply blocker mode: %s", e)


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def _generate_run_id() -> str:
    raw = f"rg-{time.time()}-{uuid.uuid4().hex[:8]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def format_telegram_report(result: ResearchGuardCalibResult, run_id: str) -> str:
    mode_emoji = {"report": "🟢", "enforce": "🔴"}
    recommend_emoji = {"hold": "⏸️", "promote": "⬆️", "rollback": "⬇️"}

    m_e = mode_emoji.get(result.effective_mode, "❓")
    r_e = recommend_emoji.get(result.recommend, "❓")

    age_h = result.latest_report_age_sec / 3600 if result.latest_report_age_sec > 0 else 0

    lines = [
        "📊 <b>G14 Strategy Research Guard Calibrator</b>",
        "",
        f"📊 <b>Mode:</b> {m_e} <code>{result.effective_mode}</code>",
        f"🎯 <b>Recommend:</b> {r_e} <code>{result.recommend}</code>",
        f"📝 <b>Reason:</b> <code>{result.reason}</code>",
        "",
        "── Nightly Report Metrics ──",
        f"📈 PSR: <code>{result.latest_psr:.3f}</code> (min: {result.thresholds.get('psr_min', 0):.2f})",
        f"📈 DSR: <code>{result.latest_dsr:.3f}</code> (min: {result.thresholds.get('dsr_min', 0):.2f})",
        f"📉 PBO: <code>{result.latest_pbo:.3f}</code> (max: {result.thresholds.get('pbo_max', 0):.2f})",
        f"📉 ECE: <code>{result.latest_ece:.3f}</code> (max: {result.thresholds.get('ece_max', 0):.2f})",
        f"📉 Brier: <code>{result.latest_brier:.3f}</code> (max: {result.thresholds.get('brier_max', 0):.2f})",
        f"⏱️ Report age: <code>{age_h:.1f}h</code> (max: {result.thresholds.get('max_report_age_sec', 0) / 3600:.0f}h)",
        "",
        "── Proof Streak ──",
        f"📊 Streak: <code>{result.proof_streak}/{result.proof_streak_required}</code>",
    ]

    if result.rollback_streak > 0:
        lines.append(
            f"⚠️ Rollback streak: <code>{result.rollback_streak}/{result.rollback_streak_required}</code>"
        )

    if result.failing_metrics:
        lines.append("")
        lines.append("❌ Failing: <code>" + ", ".join(result.failing_metrics) + "</code>")

    lines.append("")
    lines.append(f"Run ID: <code>{run_id}</code>")

    return "\n".join(lines)


def _build_buttons(run_id: str, result: ResearchGuardCalibResult) -> Optional[str]:
    if result.recommend == "promote":
        buttons = [[
            {"text": "🔴 Enforce (Block Deploys)", "callback_data": f"rg_calib_approve:{run_id}"},
            {"text": "🟢 Keep Report-Only", "callback_data": f"rg_calib_reject:{run_id}"},
        ]]
        return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    if result.recommend == "rollback":
        buttons = [[
            {"text": "🟢 Confirm Rollback → Report", "callback_data": f"rg_calib_reject:{run_id}"},
        ]]
        return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    return None


def _store_pending(
    redis_client: Any,
    run_id: str,
    result: ResearchGuardCalibResult,
) -> None:
    now_ms = get_ny_time_millis()
    pending = {
        "run_id": run_id,
        "status": "PENDING",
        "action": "promote_to_enforce",
        "effective_mode": result.effective_mode,
        "latest_psr": round(result.latest_psr, 4),
        "latest_dsr": round(result.latest_dsr, 4),
        "latest_pbo": round(result.latest_pbo, 4),
        "latest_ece": round(result.latest_ece, 4),
        "latest_brier": round(result.latest_brier, 4),
        "proof_streak": result.proof_streak,
        "created_at_ms": now_ms,
        # Reminder tracking
        "last_reminder_ms": now_ms,
        "reminder_count": 0,
    }
    try:
        redis_client.set(
            f"rg_calib:pending:{run_id}",
            json.dumps(pending, default=str),
            ex=PENDING_TTL_SEC,
        )
    except Exception as e:
        logger.warning("Failed to store RG pending approval: %s", e)


def _send_telegram(
    redis_client: Any,
    text: str,
    buttons_json: Optional[str] = None,
) -> None:
    notify_stream = os.getenv("NOTIFY_STREAM", NOTIFY_STREAM)
    fields: Dict[str, str] = {
        "type": "report",
        "text": text,
        "ts": str(get_ny_time_millis()),
    }
    if buttons_json:
        fields["buttons"] = buttons_json
    try:
        redis_client.xadd(
            notify_stream, fields,
            maxlen=200000, approximate=True,
        )
        logger.info("📤 RG Calibrator Telegram report sent (buttons=%s)", bool(buttons_json))
    except Exception as e:
        logger.error("Failed to send RG Calibrator Telegram report: %s", e)


# ---------------------------------------------------------------------------
# Throttle
# ---------------------------------------------------------------------------

THROTTLE_KEY = "rg_calib:last_run_ts"


def _should_run(redis_client: Any, interval_sec: int) -> bool:
    try:
        last_raw = redis_client.get(THROTTLE_KEY)
        if last_raw:
            elapsed = time.time() - float(last_raw)
            if elapsed < interval_sec:
                logger.info(
                    "RG Calibrator throttled: last run %ds ago (interval=%ds)",
                    int(elapsed), interval_sec,
                )
                return False
    except Exception:
        pass
    return True


def _record_run(redis_client: Any, interval_sec: int) -> None:
    try:
        redis_client.set(THROTTLE_KEY, str(time.time()), ex=interval_sec * 3)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_research_guard_calibration(
    redis_url: str,
    *,
    send_telegram: bool = True,
    telegram_interval_sec: int = 3600,
) -> Optional[ResearchGuardCalibResult]:
    """
    Run one calibration cycle for the Research Guard.

    1. Load latest nightly report from Redis
    2. Load previous state from Redis
    3. Evaluate calibrator
    4. On promote/rollback → apply mode change to blocker key
    5. Send Telegram report
    6. Update Prometheus metrics

    Returns ResearchGuardCalibResult or None if throttled/error.
    """
    if redis_lib is None:
        logger.error("redis library not available")
        return None

    r = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    # Throttle check
    if not _should_run(r, telegram_interval_sec):
        return None

    # Load previous state
    prev_state = _load_state(r)
    prev_mode = str(prev_state.get("mode", "report") or "report")
    prev_proof_streak = int(prev_state.get("proof_streak", 0) or 0)
    prev_rollback_streak = int(prev_state.get("rollback_streak", 0) or 0)

    # Load ENV thresholds
    psr_min = float(os.getenv("RG_CALIB_PSR_MIN", "0.95"))
    dsr_min = float(os.getenv("RG_CALIB_DSR_MIN", "0.90"))
    pbo_max = float(os.getenv("RG_CALIB_PBO_MAX", "0.10"))
    ece_max = float(os.getenv("RG_CALIB_ECE_MAX", "0.15"))
    brier_max = float(os.getenv("RG_CALIB_BRIER_MAX", "0.25"))
    max_report_age_sec = float(os.getenv("RG_CALIB_MAX_REPORT_AGE_SEC", "129600"))
    proof_streak_required = int(os.getenv("RG_CALIB_PROOF_STREAK_REQUIRED", "7"))
    rollback_streak_required = int(os.getenv("RG_CALIB_ROLLBACK_STREAK", "2"))

    # Load latest nightly report
    report = _load_nightly_report(r)

    # Evaluate
    result = evaluate_research_guard(
        report,
        psr_min=psr_min,
        dsr_min=dsr_min,
        pbo_max=pbo_max,
        ece_max=ece_max,
        brier_max=brier_max,
        max_report_age_sec=max_report_age_sec,
        proof_streak=prev_proof_streak,
        proof_streak_required=proof_streak_required,
        rollback_streak=prev_rollback_streak,
        rollback_streak_required=rollback_streak_required,
        current_mode=prev_mode,
    )

    run_id = _generate_run_id()

    logger.info(
        "RG Calibrator: mode=%s→%s, recommend=%s, PSR=%.3f, DSR=%.3f, PBO=%.3f, ECE=%.3f, Brier=%.3f, "
        "streak=%d/%d, age=%.0fs",
        prev_mode, result.effective_mode, result.recommend,
        result.latest_psr, result.latest_dsr, result.latest_pbo,
        result.latest_ece, result.latest_brier,
        result.proof_streak, proof_streak_required,
        result.latest_report_age_sec,
    )

    # Apply mode changes
    mode_changed = result.effective_mode != prev_mode

    if mode_changed:
        if rg_is_promotion(prev_mode, result.effective_mode):
            logger.info(
                "🚀 RG Calibrator PROMOTE: %s → %s", prev_mode, result.effective_mode,
            )
            # Auto-apply is NOT done here; user must approve via Telegram
        elif rg_is_rollback(prev_mode, result.effective_mode):
            logger.warning(
                "⬇️ RG Calibrator AUTO-ROLLBACK: %s → %s (reason: %s)",
                prev_mode, result.effective_mode, result.reason,
            )
            # Auto-rollback: immediately switch back to report-only
            _apply_blocker_mode(r, "report")

    # Save state
    _save_state(r, result, run_id)

    # Telegram
    if send_telegram and (mode_changed or result.recommend != "hold"):
        text = format_telegram_report(result, run_id)
        buttons_json = _build_buttons(run_id, result)
        if buttons_json and result.recommend == "promote":
            _store_pending(r, run_id, result)
        _send_telegram(r, text, buttons_json)

    _record_run(r, telegram_interval_sec)

    # Prometheus
    _update_prometheus(result)

    return result


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def run_once() -> None:
    """CLI entrypoint — run one calibration pass."""
    if os.getenv("RG_CALIB_ENABLE", "0").strip().lower() not in ("1", "true", "yes"):
        logger.info("RG Calibrator disabled (RG_CALIB_ENABLE != 1)")
        return

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    send_tg = os.getenv("RG_CALIB_TELEGRAM", "1").strip().lower() in ("1", "true", "yes")
    interval = int(os.getenv("RG_CALIB_TELEGRAM_INTERVAL_SEC", "3600"))

    logger.info("Research Guard Calibrator: redis=%s...", redis_url[:40])

    result = run_research_guard_calibration(
        redis_url,
        send_telegram=send_tg,
        telegram_interval_sec=interval,
    )

    if result:
        logger.info(
            "RG Calibrator result: mode=%s, recommend=%s, PSR=%.3f, DSR=%.3f, PBO=%.3f, ECE=%.3f, Brier=%.3f",
            result.effective_mode, result.recommend,
            result.latest_psr, result.latest_dsr, result.latest_pbo,
            result.latest_ece, result.latest_brier,
        )


if __name__ == "__main__":
    run_once()
