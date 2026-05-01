# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Adverse Gate Calibrator Service — IO layer (PG + Redis + Telegram).

Runs hourly per-symbol to evaluate whether G10 (Adverse Selection Gate)
should be auto-enabled. Starts with major pairs, accumulates proof
streaks, and auto-enables per-symbol with Telegram approval for enforce.

Two-stage per-symbol promotion:
  Stage 1 (auto): disabled → shadow (gate evaluates but doesn't veto)
  Stage 2 (manual): shadow → enforce (via Telegram Approve button)

Rollback (auto): if reversal veto precision degrades → disable for symbol.

Redis keys written (per-symbol):
  - cfg:adv_calib:state:{SYMBOL}     → JSON {mode, proof_streak, ...}
  - cfg:adv_calib:last_result        → JSON (aggregate for observability)
  - adv_calib:pending:{run_id}       → JSON (pending Telegram approval)

Dynamic cfg keys set (per-symbol on promote/rollback):
  - adv_calib_mode         → "disabled" | "shadow" | "enforce"
  - adv_calib_streak       → int
  - adv_calib_precision    → float
  - adv_calib_updated_ms   → int (epoch ms)

Usage:
  - python -m services.adverse_gate_calibrator_service
  - Called from of_timers_worker.py as an hourly timer task
"""
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    logger = setup_logger("AdverseGateCalibrator")
except ImportError:
    import logging
    logger = logging.getLogger("AdverseGateCalibrator")

from core.adverse_gate_calibrator import (
    AdverseOutcome,
    AdverseGateCalibResult,
    evaluate_adverse_gate,
    adv_mode_to_int,
    is_adv_enable,
    is_adv_disable,
)
from core.dyn_cfg_keys import DynCfgKeys as DK
from core.redis_keys import RedisStreams as RS


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_KEY_PREFIX = "cfg:adv_calib:state:"  # + SYMBOL
RESULT_KEY = "cfg:adv_calib:last_result"
PENDING_TTL_SEC = 48 * 3600  # 48h
STATE_TTL_SEC = 7 * 24 * 3600  # 7 days

NOTIFY_STREAM = RS.NOTIFY_TELEGRAM

# Default major pairs to start with
DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "DOTUSDT", 
]


# ---------------------------------------------------------------------------
# Prometheus metrics (lazy init, fail-safe)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Gauge, Counter

    adv_calib_veto_precision_gauge = Gauge(
        "adv_calib_reversal_veto_precision",
        "Last reversal veto precision per symbol",
        ["symbol"],
    )
    adv_calib_veto_lift_gauge = Gauge(
        "adv_calib_reversal_veto_lift",
        "Reversal veto lift over baseline per symbol",
        ["symbol"],
    )
    adv_calib_proof_streak_gauge = Gauge(
        "adv_calib_proof_streak",
        "Current proof streak per symbol",
        ["symbol"],
    )
    adv_calib_mode_gauge = Gauge(
        "adv_calib_mode",
        "0=disabled, 1=shadow, 2=enforce per symbol",
        ["symbol"],
    )
    adv_calib_n_total_gauge = Gauge(
        "adv_calib_n_total",
        "Total outcomes in window per symbol",
        ["symbol"],
    )
    adv_calib_enable_total = Counter(
        "adv_calib_enable_total", "Auto-enables to shadow", ["symbol"]
    )
    adv_calib_disable_total = Counter(
        "adv_calib_disable_total", "Auto-disables back to disabled", ["symbol"]
    )
    adv_calib_run_total = Counter(
        "adv_calib_run_total", "Calibration runs", ["symbol", "result"]
    )
    _HAS_PROM = True
except ImportError:
    _HAS_PROM = False


def _update_prometheus(result: AdverseGateCalibResult) -> None:
    """Best-effort Prometheus gauge updates."""
    if not _HAS_PROM:
        return
    try:
        s = result.symbol
        adv_calib_veto_precision_gauge.labels(symbol=s).set(result.reversal_veto_precision)
        adv_calib_veto_lift_gauge.labels(symbol=s).set(result.reversal_veto_lift)
        adv_calib_proof_streak_gauge.labels(symbol=s).set(result.proof_streak)
        adv_calib_mode_gauge.labels(symbol=s).set(adv_mode_to_int(result.effective_mode))
        adv_calib_n_total_gauge.labels(symbol=s).set(result.n_total)
        adv_calib_run_total.labels(symbol=s, result=result.recommend).inc()
        if result.is_ready_for_enable:
            adv_calib_enable_total.labels(symbol=s).inc()
        if result.is_disable:
            adv_calib_disable_total.labels(symbol=s).inc()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Database loader
# ---------------------------------------------------------------------------

def load_adverse_outcomes(
    dsn: str,
    window_hours: int = 24,
    symbols: Optional[List[str]] = None,
) -> Dict[str, List[AdverseOutcome]]:
    """
    Load per-symbol trade outcomes with G10 adverse gate annotations.

    Returns: dict of symbol → list of AdverseOutcome.
    """
    if psycopg2 is None:
        logger.error("psycopg2 not available, cannot load outcomes")
        return {}

    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10, application_name="adv_calibrator")

        symbol_filter = ""
        if symbols:
            placeholders = ",".join(f"'{s}'" for s in symbols)
            symbol_filter = f"AND symbol IN ({placeholders})"

        sql = f"""
        SELECT
            symbol,
            pnl_pct,
            direction,
            COALESCE(indicators::jsonb ->> 'strong_gate_scn', '') AS scenario,
            -- Reversal sub-gate flags
            COALESCE((indicators::jsonb ->> 'g10_reversal_vetoed')::int, 0) AS rev_vetoed,
            COALESCE((indicators::jsonb ->> 'g10_reversal_passed')::int,
                CASE WHEN COALESCE(indicators::jsonb ->> 'strong_gate_scn', '') ILIKE '%reversal%'
                     AND COALESCE((indicators::jsonb ->> 'g10_reversal_vetoed')::int, 0) = 0
                THEN 1 ELSE 0 END
            ) AS rev_passed,
            COALESCE((indicators::jsonb ->> 'cvd_reclaim_ok')::int, 0)
            + COALESCE((indicators::jsonb ->> 'obi_stable')::int, 0)
            + COALESCE((indicators::jsonb ->> 'ofi_stable')::int, 0)
            AS evidence_count,
            -- Continuation sub-gate flags
            COALESCE((indicators::jsonb ->> 'adverse_confirmed')::int, 0) AS cont_confirmed,
            COALESCE((indicators::jsonb ->> 'adverse_rejected')::int, 0) AS cont_rejected,
            COALESCE((indicators::jsonb ->> 'adverse_timeout')::int, 0) AS cont_timeout,
            COALESCE((indicators::jsonb ->> 'adverse_wait_ms')::int, 0) AS adverse_wait_ms,
            EXTRACT(EPOCH FROM entry_ts) * 1000 AS ts_ms
        FROM trades_closed
        WHERE exit_ts > NOW() - INTERVAL '{int(window_hours)} hours'
          AND source IN ('CryptoOrderFlow', 'AggregatedHub-V2', 'orderflow')
          AND indicators IS NOT NULL
          AND indicators::text != ''
          AND indicators::text != '{{}}'
          {symbol_filter}
        ORDER BY exit_ts DESC
        LIMIT 5000
        """

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        by_symbol: Dict[str, List[AdverseOutcome]] = {}
        for row in rows:
            try:
                sym = str(row.get("symbol") or "")
                pnl = float(row.get("pnl_pct") or 0.0)
                scenario = str(row.get("scenario") or "")
                if not scenario:
                    scenario = "reversal"  # Default scenario for signals without annotation

                outcome = AdverseOutcome(
                    symbol=sym,
                    pnl_pct=pnl,
                    is_loss=pnl < 0,
                    scenario=scenario,
                    direction=str(row.get("direction") or ""),
                    reversal_vetoed=int(row.get("rev_vetoed") or 0) == 1,
                    reversal_passed=int(row.get("rev_passed") or 0) == 1,
                    has_evidence=int(row.get("evidence_count") or 0) > 0,
                    continuation_confirmed=int(row.get("cont_confirmed") or 0) == 1,
                    continuation_rejected=int(row.get("cont_rejected") or 0) == 1,
                    continuation_timed_out=int(row.get("cont_timeout") or 0) == 1,
                    adverse_wait_ms=int(row.get("adverse_wait_ms") or 0),
                    ts_ms=int(row.get("ts_ms") or 0),
                )
                by_symbol.setdefault(sym, []).append(outcome)
            except Exception:
                continue

        total = sum(len(v) for v in by_symbol.values())
        logger.info(
            "Loaded %d adverse outcomes across %d symbols (window=%dh)",
            total, len(by_symbol), window_hours,
        )
        return by_symbol

    except Exception as e:
        logger.error("Failed to load adverse outcomes: %s", e)
        return {}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# State persistence (Redis) — per-symbol
# ---------------------------------------------------------------------------

def _load_symbol_state(redis_client: Any, symbol: str) -> Dict[str, Any]:
    """Load per-symbol calibrator state from Redis."""
    try:
        raw = redis_client.get(f"{STATE_KEY_PREFIX}{symbol}")
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning("Failed to load adv state for %s: %s", symbol, e)
    return {}


def _save_symbol_state(
    redis_client: Any,
    symbol: str,
    result: AdverseGateCalibResult,
    run_id: str,
) -> None:
    """Persist per-symbol calibrator state to Redis."""
    state = {
        "symbol": symbol,
        "mode": result.effective_mode,
        "proof_streak": result.proof_streak,
        "rollback_streak": result.rollback_streak,
        "last_precision": round(result.reversal_veto_precision, 4),
        "last_lift": round(result.reversal_veto_lift, 4),
        "last_recommend": result.recommend,
        "last_reason": result.reason,
        "n_total": result.n_total,
        "n_reversals": result.n_reversals,
        "run_id": run_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_ms": get_ny_time_millis(),
    }
    try:
        redis_client.set(
            f"{STATE_KEY_PREFIX}{symbol}",
            json.dumps(state, default=str),
            ex=STATE_TTL_SEC,
        )
    except Exception as e:
        logger.error("Failed to save adv state for %s: %s", symbol, e)


def _apply_dynamic_cfg(
    redis_client: Any,
    result: AdverseGateCalibResult,
) -> None:
    """Write calibrator decision to dynamic_cfg for a single symbol."""
    now_ms = get_ny_time_millis()
    dyn_key = f"config:orderflow:{result.symbol}"
    try:
        pipe = redis_client.pipeline()
        pipe.hset(dyn_key, DK.ADV_CALIB_MODE, result.effective_mode)
        pipe.hset(dyn_key, DK.ADV_CALIB_STREAK, str(result.proof_streak))
        pipe.hset(dyn_key, DK.ADV_CALIB_PRECISION, str(round(result.reversal_veto_precision, 4)))
        pipe.hset(dyn_key, DK.ADV_CALIB_UPDATED_MS, str(now_ms))
        # Write the actual enable flag
        if result.effective_mode in ("shadow", "enforce"):
            pipe.hset(dyn_key, "adverse_check_enable", "1")
        elif result.effective_mode == "disabled":
            pipe.hset(dyn_key, "adverse_check_enable", "0")
        pipe.execute()
    except Exception as e:
        logger.warning("Failed to apply adv dynamic_cfg for %s: %s", result.symbol, e)


# ---------------------------------------------------------------------------
# Telegram reports
# ---------------------------------------------------------------------------

def _generate_run_id() -> str:
    raw = f"adv-{time.time()}-{uuid.uuid4().hex[:8]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def format_telegram_report(
    results: List[Tuple[str, AdverseGateCalibResult]],
    run_id: str,
    changed_only: bool = True,
) -> str:
    """Build Telegram HTML report for adverse gate calibration (multi-symbol)."""

    mode_emoji = {"disabled": "⚪", "shadow": "🟡", "enforce": "🟢"}

    lines = [
        "🛡️ <b>G10 Adverse Gate Calibrator</b>",
        "",
        f"📊 <b>Symbols evaluated:</b> {len(results)}",
        "",
    ]

    # Summary table
    enabled = [(s, r) for s, r in results if r.effective_mode != "disabled"]
    changed = [(s, r) for s, r in results if r.recommend != "hold"]

    if changed:
        lines.append("── 🔄 Changes ──")
        for sym, res in changed:
            e = mode_emoji.get(res.effective_mode, "❓")
            lines.append(
                f"  {e} <code>{sym:12s}</code> → <code>{res.effective_mode:8s}</code> "
                f"({res.recommend}) prec=<code>{res.reversal_veto_precision:.1%}</code> "
                f"lift=<code>{res.reversal_veto_lift:+.1%}</code>"
            )
        lines.append("")

    if enabled:
        lines.append("── ✅ Enabled symbols ──")
        for sym, res in enabled:
            e = mode_emoji.get(res.effective_mode, "❓")
            lines.append(
                f"  {e} <code>{sym:12s}</code> <code>{res.effective_mode:8s}</code> "
                f"streak=<code>{res.proof_streak}/{res.proof_streak_required}</code>"
            )
        lines.append("")

    # Top 5 candidates building proof
    building = [(s, r) for s, r in results
                if r.recommend == "hold" and r.proof_streak > 0 and r.effective_mode == "disabled"]
    if building:
        lines.append("── 🏗️ Building proof ──")
        for sym, res in sorted(building, key=lambda x: x[1].proof_streak, reverse=True)[:5]:
            lines.append(
                f"  ⏳ <code>{sym:12s}</code> streak=<code>{res.proof_streak}/{res.proof_streak_required}</code> "
                f"prec=<code>{res.reversal_veto_precision:.1%}</code>"
            )
        lines.append("")

    lines.append(f"Run ID: <code>{run_id}</code>")
    return "\n".join(lines)


def _build_buttons(run_id: str, symbols_to_approve: List[str]) -> Optional[str]:
    """Build Telegram inline keyboard for per-symbol enforcement.

    Shows 'Enforce All' if there are symbols in shadow ready for enforcement.
    """
    if not symbols_to_approve:
        return None

    sym_list = ",".join(symbols_to_approve[:10])  # Limit to 10 in callback
    buttons = [[
        {"text": f"🟢 Enforce ({len(symbols_to_approve)} sym)", "callback_data": f"adv_calib_approve:{run_id}"},
        {"text": "⬇️ Disable All", "callback_data": f"adv_calib_reject:{run_id}"},
    ]]
    return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))


def _store_pending(
    redis_client: Any,
    run_id: str,
    symbols_to_approve: List[str],
    results: Dict[str, AdverseGateCalibResult],
) -> None:
    """Store pending approval data for Telegram callback handler."""
    symbol_data = {}
    for sym in symbols_to_approve:
        r = results.get(sym)
        if r:
            symbol_data[sym] = {
                "reversal_veto_precision": round(r.reversal_veto_precision, 4),
                "reversal_veto_lift": round(r.reversal_veto_lift, 4),
                "proof_streak": r.proof_streak,
            }

    now_ms = get_ny_time_millis()
    pending = {
        "run_id": run_id,
        "status": "PENDING",
        "action": "promote_to_enforce",
        "symbols": symbols_to_approve,
        "symbol_data": symbol_data,
        "created_at_ms": now_ms,
        # Reminder tracking — used by notify_worker reminder loop
        "last_reminder_ms": now_ms,
        "reminder_count": 0,
    }
    try:
        redis_client.set(
            f"adv_calib:pending:{run_id}",
            json.dumps(pending, default=str),
            ex=PENDING_TTL_SEC,
        )
    except Exception as e:
        logger.warning("Failed to store adv pending approval: %s", e)


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
        logger.info("📤 Adverse Calibrator Telegram report sent (buttons=%s)", bool(buttons_json))
    except Exception as e:
        logger.error("Failed to send Adverse Calibrator Telegram report: %s", e)


# ---------------------------------------------------------------------------
# Throttle
# ---------------------------------------------------------------------------

THROTTLE_KEY = "adv_calib:last_run_ts"


def _should_run(redis_client: Any, interval_sec: int) -> bool:
    try:
        last_raw = redis_client.get(THROTTLE_KEY)
        if last_raw:
            elapsed = time.time() - float(last_raw)
            if elapsed < interval_sec:
                logger.info(
                    "Adverse Calibrator throttled: last run %ds ago (interval=%ds)",
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

def run_adverse_gate_calibration(
    dsn: str,
    redis_url: str,
    *,
    window_hours: int = 24,
    send_telegram: bool = True,
    telegram_interval_sec: int = 3600,
    symbols: Optional[List[str]] = None,
) -> Optional[Dict[str, AdverseGateCalibResult]]:
    """
    Run one calibration cycle for G10 Adverse Gate (per-symbol).

    1. Load trade outcomes per-symbol from PG
    2. Load previous per-symbol state from Redis
    3. Evaluate calibrator per symbol
    4. Apply mode changes to dynamic_cfg per symbol
    5. Send aggregate Telegram report
    6. Update Prometheus metrics

    Returns dict of symbol → AdverseGateCalibResult or None if throttled.
    """
    if redis_lib is None:
        logger.error("redis library not available")
        return None

    r = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    # Throttle
    if not _should_run(r, telegram_interval_sec):
        return None

    # Resolve target symbols
    if symbols is None:
        symbols_raw = os.getenv("ADV_CALIB_SYMBOLS", "")
        if symbols_raw.strip():
            symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
        else:
            symbols = DEFAULT_SYMBOLS.copy()

    # Load ENV thresholds
    min_precision = float(os.getenv("ADV_CALIB_MIN_PRECISION", "0.55"))
    min_lift = float(os.getenv("ADV_CALIB_MIN_LIFT", "0.08"))
    min_n_reversals = int(os.getenv("ADV_CALIB_MIN_N_REVERSALS", "5"))
    min_n_total = int(os.getenv("ADV_CALIB_MIN_N_TOTAL", "15"))
    proof_streak_required = int(os.getenv("ADV_CALIB_PROOF_STREAK_REQUIRED", "3"))
    rollback_precision = float(os.getenv("ADV_CALIB_ROLLBACK_PRECISION", "0.35"))
    rollback_streak_required = int(os.getenv("ADV_CALIB_ROLLBACK_STREAK", "2"))

    # Load outcomes from PG (grouped by symbol)
    outcomes_by_symbol = load_adverse_outcomes(dsn, window_hours, symbols)

    results: Dict[str, AdverseGateCalibResult] = {}
    changed_symbols: List[str] = []
    symbols_in_shadow: List[str] = []

    for sym in symbols:
        outcomes = outcomes_by_symbol.get(sym, [])

        # Load previous state
        prev_state = _load_symbol_state(r, sym)
        prev_mode = str(prev_state.get("mode", "disabled") or "disabled")
        prev_proof_streak = int(prev_state.get("proof_streak", 0) or 0)
        prev_rollback_streak = int(prev_state.get("rollback_streak", 0) or 0)

        result = evaluate_adverse_gate(
            outcomes,
            symbol=sym,
            window_h=window_hours,
            min_rev_veto_precision=min_precision,
            min_rev_veto_lift=min_lift,
            min_n_reversals=min_n_reversals,
            min_n_total=min_n_total,
            proof_streak=prev_proof_streak,
            proof_streak_required=proof_streak_required,
            rollback_precision=rollback_precision,
            rollback_streak=prev_rollback_streak,
            rollback_streak_required=rollback_streak_required,
            current_mode=prev_mode,
        )

        results[sym] = result

        mode_changed = result.effective_mode != prev_mode

        if mode_changed:
            changed_symbols.append(sym)
            if is_adv_enable(prev_mode, result.effective_mode):
                logger.info("🚀 ADV Calibrator AUTO-ENABLING %s: %s → %s", sym, prev_mode, result.effective_mode)
            elif is_adv_disable(prev_mode, result.effective_mode):
                logger.warning("⬇️ ADV Calibrator AUTO-DISABLING %s: %s → %s", sym, prev_mode, result.effective_mode)

        # Apply dynamic_cfg for this symbol
        if mode_changed:
            _apply_dynamic_cfg(r, result)

        # Track symbols in shadow (candidates for enforce)
        if result.effective_mode == "shadow":
            symbols_in_shadow.append(sym)

        # Save per-symbol state
        run_id = _generate_run_id()
        _save_symbol_state(r, sym, result, run_id)

        # Prometheus
        _update_prometheus(result)

    # --- Aggregate Telegram report ---
    run_id = _generate_run_id()

    if send_telegram and (changed_symbols or symbols_in_shadow):
        result_pairs = [(sym, results[sym]) for sym in symbols if sym in results]
        text = format_telegram_report(result_pairs, run_id)

        buttons_json = _build_buttons(run_id, symbols_in_shadow)
        if buttons_json:
            _store_pending(r, run_id, symbols_in_shadow, results)
        _send_telegram(r, text, buttons_json)

    # Save aggregate result
    try:
        agg = {
            "total_symbols": len(results),
            "enabled_symbols": [s for s, res in results.items() if res.effective_mode != "disabled"],
            "changed_symbols": changed_symbols,
            "run_id": run_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        r.set(RESULT_KEY, json.dumps(agg, default=str), ex=STATE_TTL_SEC)
    except Exception:
        pass

    _record_run(r, telegram_interval_sec)

    logger.info(
        "ADV Calibrator complete: %d symbols evaluated, %d changed, %d in shadow",
        len(results), len(changed_symbols), len(symbols_in_shadow),
    )

    return results


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def run_once() -> None:
    """CLI entrypoint — run one calibration pass."""
    if os.getenv("ADV_CALIB_ENABLE", "0").strip().lower() not in ("1", "true", "yes"):
        logger.info("Adverse Calibrator disabled (ADV_CALIB_ENABLE != 1)")
        return

    dsn = (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or os.getenv("PG_DSN_CALIBRATION")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    window_hours = int(os.getenv("ADV_CALIB_WINDOW_HOURS", "24"))
    send_tg = os.getenv("ADV_CALIB_TELEGRAM", "1").strip().lower() in ("1", "true", "yes")
    interval = int(os.getenv("ADV_CALIB_TELEGRAM_INTERVAL_SEC", "3600"))

    logger.info("Adverse Gate Calibrator: window=%dh, dsn=%s...", window_hours, dsn[:40])

    results = run_adverse_gate_calibration(
        dsn, redis_url,
        window_hours=window_hours,
        send_telegram=send_tg,
        telegram_interval_sec=interval,
    )

    if results:
        enabled = [s for s, r in results.items() if r.effective_mode != "disabled"]
        logger.info(
            "ADV Calibrator result: %d/%d symbols enabled",
            len(enabled), len(results),
        )


if __name__ == "__main__":
    run_once()
