# -*- coding: utf-8 -*-
"""
Strong Gate Calibrator Service — IO layer (PG + Redis + Telegram).

Runs hourly (from of_timers_worker.py or standalone) to evaluate whether
the G5 Strong Gate should be promoted from SHADOW to ENFORCE.

Two-stage promotion:
  Stage 1 (auto): shadow → shadow_enforce (gate enforces but shadow=True fallback)
  Stage 2 (manual): shadow_enforce → full_enforce (via Telegram Approve button)

Rollback (auto): if precision degrades → revert to shadow + Telegram alert.

Redis keys written:
  - cfg:sg_calib:state              → JSON {mode, proof_streak, rollback_streak, ...}
  - cfg:sg_calib:last_result        → JSON (full result for observability)
  - sg_calib:pending:{run_id}       → JSON (pending approval data for Telegram)

Dynamic cfg keys set (on promote/rollback):
  - sg_calib_mode                   → "shadow" | "shadow_enforce" | "full_enforce"
  - sg_calib_proof_streak           → int
  - sg_calib_last_precision         → float
  - sg_calib_updated_ms             → int (epoch ms)

Usage:
  - python -m services.strong_gate_calibrator_service
  - Called from of_timers_worker.py as an hourly timer task
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
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

try:
    from common.log import setup_logger
    logger = setup_logger("StrongGateCalibrator")
except ImportError:
    import logging
    logger = logging.getLogger("StrongGateCalibrator")

from core.strong_gate_calibrator import (
    TradeOutcome,
    StrongGateCalibResult,
    evaluate_strong_gate,
    mode_to_int,
    is_promotion,
    is_rollback,
)
from core.dyn_cfg_keys import DynCfgKeys as DK
from core.redis_keys import RedisStreams as RS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_KEY = "cfg:sg_calib:state"
RESULT_KEY = "cfg:sg_calib:last_result"
PENDING_TTL_SEC = 48 * 3600  # 48h
STATE_TTL_SEC = 7 * 24 * 3600  # 7 days

NOTIFY_STREAM = RS.NOTIFY_TELEGRAM


# ---------------------------------------------------------------------------
# Prometheus metrics (lazy init, fail-safe)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Gauge, Counter

    sg_calib_veto_precision_gauge = Gauge(
        "sg_calib_veto_precision", "Last veto precision (P(loss|vetoed))"
    )
    sg_calib_pass_loss_rate_gauge = Gauge(
        "sg_calib_pass_loss_rate", "Loss rate of passed signals"
    )
    sg_calib_veto_lift_gauge = Gauge(
        "sg_calib_veto_lift", "Veto precision lift over baseline"
    )
    sg_calib_proof_streak_gauge = Gauge(
        "sg_calib_proof_streak", "Current proof streak"
    )
    sg_calib_rollback_streak_gauge = Gauge(
        "sg_calib_rollback_streak", "Current rollback streak"
    )
    sg_calib_mode_gauge = Gauge(
        "sg_calib_mode", "0=shadow, 1=shadow_enforce, 2=full_enforce"
    )
    sg_calib_n_total_gauge = Gauge(
        "sg_calib_n_total", "Total outcomes in window"
    )
    sg_calib_n_vetoed_gauge = Gauge(
        "sg_calib_n_vetoed", "Vetoed outcomes in window"
    )
    sg_calib_promote_total = Counter(
        "sg_calib_promote_total", "Auto-promotions to enforce"
    )
    sg_calib_rollback_total = Counter(
        "sg_calib_rollback_total", "Auto-rollbacks to shadow"
    )
    sg_calib_run_total = Counter(
        "sg_calib_run_total", "Calibration runs", ["result"]
    )
    _HAS_PROM = True
except ImportError:
    _HAS_PROM = False


def _update_prometheus(result: StrongGateCalibResult) -> None:
    """Best-effort Prometheus gauge updates."""
    if not _HAS_PROM:
        return
    try:
        sg_calib_veto_precision_gauge.set(result.veto_precision)
        sg_calib_pass_loss_rate_gauge.set(result.pass_loss_rate)
        sg_calib_veto_lift_gauge.set(result.veto_lift)
        sg_calib_proof_streak_gauge.set(result.proof_streak)
        sg_calib_rollback_streak_gauge.set(result.rollback_streak)
        sg_calib_mode_gauge.set(mode_to_int(result.effective_mode))
        sg_calib_n_total_gauge.set(result.n_total)
        sg_calib_n_vetoed_gauge.set(result.n_vetoed)
        sg_calib_run_total.labels(result=result.recommend).inc()
        if result.is_ready_for_promote:
            sg_calib_promote_total.inc()
        if result.is_rollback:
            sg_calib_rollback_total.inc()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Database loader
# ---------------------------------------------------------------------------

def load_shadow_veto_outcomes(
    dsn: str,
    window_hours: int = 24,
) -> List[TradeOutcome]:
    """
    Load trade outcomes from trades_closed with shadow veto annotations.

    Joins trades_closed with indicators JSON to determine:
    - shadow_vetoed: indicators->>'strong_gate_shadow_veto' = '1'
    - ok: indicators->>'of_confirm_ok' = '1' (or ok=1 in root)

    Returns list of TradeOutcome.
    """
    if psycopg2 is None:
        logger.error("psycopg2 not available, cannot load outcomes")
        return []

    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10, application_name="sg_calibrator")

        sql = """
        SELECT
            symbol,
            pnl_pct,
            direction,
            COALESCE(
                (indicators::jsonb ->> 'strong_gate_shadow_veto')::int,
                CASE WHEN (indicators::jsonb ->> 'is_virtual')::int = 1
                     AND (indicators::jsonb ->> 'of_gate_mode') = 'SHADOW'
                THEN 1 ELSE 0 END
            ) AS shadow_vetoed,
            COALESCE(
                (indicators::jsonb ->> 'of_confirm_ok')::int,
                (indicators::jsonb ->> 'strong_gate_ok')::int,
                0
            ) AS of_ok,
            COALESCE(indicators::jsonb ->> 'strong_gate_scn', '') AS scenario,
            EXTRACT(EPOCH FROM entry_ts) * 1000 AS ts_ms
        FROM trades_closed
        WHERE exit_ts > NOW() - INTERVAL '%s hours'
          AND source IN ('CryptoOrderFlow', 'AggregatedHub-V2', 'orderflow')
          AND indicators IS NOT NULL
          AND indicators::text != ''
          AND indicators::text != '{}'
        ORDER BY exit_ts DESC
        LIMIT 2000
        """ % int(window_hours)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        outcomes: List[TradeOutcome] = []
        for row in rows:
            try:
                pnl = float(row.get("pnl_pct") or 0.0)
                sv = int(row.get("shadow_vetoed") or 0) == 1
                ok = int(row.get("of_ok") or 0) == 1
                outcomes.append(TradeOutcome(
                    symbol=str(row.get("symbol") or ""),
                    pnl_pct=pnl,
                    is_loss=pnl < 0,
                    shadow_vetoed=sv,
                    ok=ok,
                    scenario=str(row.get("scenario") or ""),
                    direction=str(row.get("direction") or ""),
                    ts_ms=int(row.get("ts_ms") or 0),
                ))
            except Exception:
                continue

        logger.info(
            "Loaded %d outcomes (%d vetoed, %d passed) from trades_closed (window=%dh)",
            len(outcomes),
            sum(1 for o in outcomes if o.shadow_vetoed),
            sum(1 for o in outcomes if o.ok),
            window_hours,
        )
        return outcomes

    except Exception as e:
        logger.error("Failed to load outcomes from trades_closed: %s", e)
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# State persistence (Redis)
# ---------------------------------------------------------------------------

def _load_state(redis_client: Any) -> Dict[str, Any]:
    """Load calibrator state from Redis."""
    try:
        raw = redis_client.get(STATE_KEY)
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning("Failed to load calibrator state: %s", e)
    return {}


def _save_state(
    redis_client: Any,
    result: StrongGateCalibResult,
    run_id: str,
) -> None:
    """Persist calibrator state to Redis."""
    state = {
        "mode": result.effective_mode,
        "proof_streak": result.proof_streak,
        "rollback_streak": result.rollback_streak,
        "last_precision": round(result.veto_precision, 4),
        "last_lift": round(result.veto_lift, 4),
        "last_recommend": result.recommend,
        "last_reason": result.reason,
        "n_total": result.n_total,
        "n_vetoed": result.n_vetoed,
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
        logger.error("Failed to save calibrator state: %s", e)


def _apply_dynamic_cfg(
    redis_client: Any,
    result: StrongGateCalibResult,
    symbols: List[str],
) -> None:
    """Write calibrator decision to dynamic_cfg for each symbol runtime.

    Strategy.py reads these keys at enforce decision point (line ~2211).
    """
    now_ms = get_ny_time_millis()

    for symbol in symbols:
        dyn_key = f"config:orderflow:{symbol}"
        try:
            pipe = redis_client.pipeline()
            pipe.hset(dyn_key, DK.SG_CALIB_MODE, result.effective_mode)
            pipe.hset(dyn_key, DK.SG_CALIB_PROOF_STREAK, str(result.proof_streak))
            pipe.hset(dyn_key, DK.SG_CALIB_LAST_PRECISION, str(round(result.veto_precision, 4)))
            pipe.hset(dyn_key, DK.SG_CALIB_UPDATED_MS, str(now_ms))
            pipe.execute()
        except Exception as e:
            logger.warning("Failed to apply dynamic_cfg for %s: %s", symbol, e)

    logger.info(
        "Applied dynamic_cfg: mode=%s, precision=%.3f, streak=%d → %d symbols",
        result.effective_mode, result.veto_precision, result.proof_streak, len(symbols),
    )


# ---------------------------------------------------------------------------
# Telegram reports
# ---------------------------------------------------------------------------

def _generate_run_id() -> str:
    raw = f"sg-{time.time()}-{uuid.uuid4().hex[:8]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def format_telegram_report(result: StrongGateCalibResult, run_id: str) -> str:
    """Build Telegram HTML report for Strong Gate calibration."""
    mode_emoji = {
        "shadow": "🟡",
        "shadow_enforce": "🟠",
        "full_enforce": "🔴",
    }
    recommend_emoji = {
        "hold": "⏸️",
        "promote": "⬆️",
        "rollback": "⬇️",
    }

    m_e = mode_emoji.get(result.effective_mode, "❓")
    r_e = recommend_emoji.get(result.recommend, "❓")

    lines = [
        f"🛡️ <b>G5 Strong Gate Calibrator</b>",
        f"",
        f"📊 <b>Mode:</b> {m_e} <code>{result.effective_mode}</code>",
        f"🎯 <b>Recommend:</b> {r_e} <code>{result.recommend}</code>",
        f"📝 <b>Reason:</b> <code>{result.reason}</code>",
        f"",
        f"── Метрики (window={result.window_h}h) ──",
        f"📈 Veto Precision: <code>{result.veto_precision:.1%}</code> (порог: {result.thresholds.get('min_precision', 0):.0%})",
        f"📉 Pass Loss Rate: <code>{result.pass_loss_rate:.1%}</code>",
        f"🔺 Veto Lift:      <code>{result.veto_lift:+.1%}</code> (порог: {result.thresholds.get('min_lift', 0):.0%})",
        f"",
        f"── Выборка ──",
        f"📦 Total: <code>{result.n_total}</code>",
        f"🚫 Vetoed: <code>{result.n_vetoed}</code> (loss: <code>{result.n_vetoed_loss}</code>, win: <code>{result.n_vetoed_win}</code>)",
        f"✅ Passed: <code>{result.n_passed}</code> (loss: <code>{result.n_passed_loss}</code>, win: <code>{result.n_passed_win}</code>)",
        f"",
        f"── Proof Streak ──",
        f"📊 Streak: <code>{result.proof_streak}/{result.proof_streak_required}</code>",
    ]

    if result.rollback_streak > 0:
        lines.append(f"⚠️ Rollback streak: <code>{result.rollback_streak}/{result.rollback_streak_required}</code>")

    lines.append(f"")
    lines.append(f"Run ID: <code>{run_id}</code>")

    return "\n".join(lines)


def _build_buttons(run_id: str, result: StrongGateCalibResult) -> Optional[str]:
    """Build Telegram inline keyboard for approval.

    Buttons shown when:
    - Auto-promoted to shadow_enforce → user can confirm full_enforce
    - Or calibrator recommends promote → user can approve
    """
    if result.recommend == "promote" or result.effective_mode == "shadow_enforce":
        buttons = [[
            {"text": "✅ Full Enforce", "callback_data": f"sg_calib_approve:{run_id}"},
            {"text": "⬇️ Revert Shadow", "callback_data": f"sg_calib_reject:{run_id}"},
        ]]
        return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    return None


def _store_pending(
    redis_client: Any,
    run_id: str,
    result: StrongGateCalibResult,
) -> None:
    """Store pending approval data for Telegram callback handler.

    Includes reminder tracking fields used by BotCallbackPoller._sg_calib_reminder_loop
    to resend the Telegram message every 30 min until user responds.
    """
    now_ms = get_ny_time_millis()
    pending = {
        "run_id": run_id,
        "status": "PENDING",
        "action": "promote_to_full_enforce",
        "effective_mode": result.effective_mode,
        "veto_precision": round(result.veto_precision, 4),
        "veto_lift": round(result.veto_lift, 4),
        "proof_streak": result.proof_streak,
        "created_at_ms": now_ms,
        # Reminder tracking — used by notify_worker reminder loop
        "last_reminder_ms": now_ms,
        "reminder_count": 0,
    }
    try:
        redis_client.set(
            f"sg_calib:pending:{run_id}",
            json.dumps(pending, default=str),
            ex=PENDING_TTL_SEC,
        )
    except Exception as e:
        logger.warning("Failed to store pending approval: %s", e)


def _send_telegram(
    redis_client: Any,
    text: str,
    buttons_json: Optional[str] = None,
) -> None:
    """Push message to notify:telegram stream."""
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
        logger.info("📤 SG Calibrator Telegram report sent (buttons=%s)", bool(buttons_json))
    except Exception as e:
        logger.error("Failed to send SG Calibrator Telegram report: %s", e)


# ---------------------------------------------------------------------------
# Throttle
# ---------------------------------------------------------------------------

THROTTLE_KEY = "sg_calib:last_run_ts"


def _should_run(redis_client: Any, interval_sec: int) -> bool:
    try:
        last_raw = redis_client.get(THROTTLE_KEY)
        if last_raw:
            elapsed = time.time() - float(last_raw)
            if elapsed < interval_sec:
                logger.info(
                    "SG Calibrator throttled: last run %ds ago (interval=%ds)",
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

def run_strong_gate_calibration(
    dsn: str,
    redis_url: str,
    *,
    window_hours: int = 24,
    send_telegram: bool = True,
    telegram_interval_sec: int = 3600,
    symbols: Optional[List[str]] = None,
) -> Optional[StrongGateCalibResult]:
    """
    Run one calibration cycle for the Strong Gate.

    1. Load trade outcomes from PG
    2. Load previous state from Redis
    3. Evaluate calibrator
    4. Apply mode changes to dynamic_cfg
    5. Send Telegram report
    6. Update Prometheus metrics

    Returns StrongGateCalibResult or None if throttled/error.
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
    prev_mode = str(prev_state.get("mode", "shadow") or "shadow")
    prev_proof_streak = int(prev_state.get("proof_streak", 0) or 0)
    prev_rollback_streak = int(prev_state.get("rollback_streak", 0) or 0)

    # Load ENV-driven thresholds
    min_precision = float(os.getenv("SG_CALIB_MIN_PRECISION", "0.55"))
    min_lift = float(os.getenv("SG_CALIB_MIN_LIFT", "0.10"))
    min_n_vetoed = int(os.getenv("SG_CALIB_MIN_N_VETOED", "10"))
    min_n_total = int(os.getenv("SG_CALIB_MIN_N_TOTAL", "30"))
    proof_streak_required = int(os.getenv("SG_CALIB_PROOF_STREAK_REQUIRED", "3"))
    rollback_precision = float(os.getenv("SG_CALIB_ROLLBACK_PRECISION", "0.40"))
    rollback_streak_required = int(os.getenv("SG_CALIB_ROLLBACK_STREAK", "2"))

    # Load outcomes from PG
    outcomes = load_shadow_veto_outcomes(dsn, window_hours)

    # Evaluate
    result = evaluate_strong_gate(
        outcomes,
        window_h=window_hours,
        min_precision=min_precision,
        min_lift=min_lift,
        min_n_vetoed=min_n_vetoed,
        min_n_total=min_n_total,
        proof_streak=prev_proof_streak,
        proof_streak_required=proof_streak_required,
        rollback_precision=rollback_precision,
        rollback_streak=prev_rollback_streak,
        rollback_streak_required=rollback_streak_required,
        current_mode=prev_mode,
    )

    run_id = _generate_run_id()

    logger.info(
        "SG Calibrator: mode=%s→%s, recommend=%s, precision=%.3f, lift=%.3f, "
        "streak=%d/%d, n_total=%d, n_vetoed=%d",
        prev_mode, result.effective_mode, result.recommend,
        result.veto_precision, result.veto_lift,
        result.proof_streak, proof_streak_required,
        result.n_total, result.n_vetoed,
    )

    # Apply mode changes
    mode_changed = result.effective_mode != prev_mode

    if mode_changed:
        if is_promotion(prev_mode, result.effective_mode):
            logger.info(
                "🚀 SG Calibrator AUTO-PROMOTING: %s → %s",
                prev_mode, result.effective_mode,
            )
        elif is_rollback(prev_mode, result.effective_mode):
            logger.warning(
                "⬇️ SG Calibrator AUTO-ROLLBACK: %s → %s (reason: %s)",
                prev_mode, result.effective_mode, result.reason,
            )

    # Resolve target symbols for dynamic_cfg
    if symbols is None:
        symbols_raw = os.getenv("SG_CALIB_SYMBOLS", "")
        if symbols_raw.strip():
            symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
        else:
            # Apply globally: use wildcard key or enumerate from running symbols
            symbols = _discover_active_symbols(r)

    if symbols:
        _apply_dynamic_cfg(r, result, symbols)

    # Save state
    _save_state(r, result, run_id)

    # Telegram
    if send_telegram and (mode_changed or result.recommend != "hold"):
        text = format_telegram_report(result, run_id)
        buttons_json = _build_buttons(run_id, result)
        if buttons_json:
            _store_pending(r, run_id, result)
        _send_telegram(r, text, buttons_json)

    _record_run(r, telegram_interval_sec)

    # Prometheus
    _update_prometheus(result)

    return result


def _discover_active_symbols(redis_client: Any) -> List[str]:
    """Discover active symbols from running orderflow configs in Redis."""
    try:
        keys = redis_client.keys("config:orderflow:*")
        symbols = []
        for k in (keys or []):
            # key format: config:orderflow:BTCUSDT
            parts = str(k).split(":")
            if len(parts) >= 3:
                sym = parts[2].upper()
                if sym and len(sym) >= 4:
                    symbols.append(sym)
        return sorted(set(symbols))
    except Exception as e:
        logger.warning("Failed to discover active symbols: %s", e)
        return []


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def run_once() -> None:
    """CLI entrypoint — run one calibration pass."""
    if os.getenv("SG_CALIB_ENABLE", "0").strip().lower() not in ("1", "true", "yes"):
        logger.info("SG Calibrator disabled (SG_CALIB_ENABLE != 1)")
        return

    dsn = (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or os.getenv("PG_DSN_CALIBRATION")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    window_hours = int(os.getenv("SG_CALIB_WINDOW_HOURS", "24"))
    send_tg = os.getenv("SG_CALIB_TELEGRAM", "1").strip().lower() in ("1", "true", "yes")
    interval = int(os.getenv("SG_CALIB_TELEGRAM_INTERVAL_SEC", "3600"))

    logger.info("Strong Gate Calibrator: window=%dh, dsn=%s...", window_hours, dsn[:40])

    result = run_strong_gate_calibration(
        dsn, redis_url,
        window_hours=window_hours,
        send_telegram=send_tg,
        telegram_interval_sec=interval,
    )

    if result:
        logger.info(
            "SG Calibrator result: mode=%s, recommend=%s, precision=%.3f",
            result.effective_mode, result.recommend, result.veto_precision,
        )


if __name__ == "__main__":
    run_once()
