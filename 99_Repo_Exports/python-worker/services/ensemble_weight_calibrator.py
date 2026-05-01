# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Ensemble Weight Calibrator — hourly recalculation of per-source signal weights.

Algorithm:
  For each (symbol, source) pair:
    1. Load last 30 days of trade outcomes from trades_closed
    2. Compute OOS Sharpe ratio (using MAD for robustness)
    3. Normalize weights: negative Sharpe → weight 0
    4. Write normalized weights to Redis HASH weights:ensemble:{symbol}

Telegram flow (like Trail Calibrator):
  1. Calibrator computes new weights
  2. If ENSEMBLE_CALIB_MODE=shadow → writes shadow weights + sends Telegram report
     with ✅ Approve / ❌ Reject buttons
  3. On Approve (via notify_worker BotCallbackPoller) → switch to enforce
  4. On Reject → discard, keep current weights

Redis keys written:
  - weights:ensemble:{symbol}         → HASH {source: weight} (enforce mode)
  - weights:ensemble:shadow:{symbol}  → HASH (shadow preview)
  - ensemble:weight_meta:{symbol}     → STRING JSON observability
  - ensemble:calib:pending:{run_id}   → STRING JSON (pending approval data)

Usage:
  - python -m services.ensemble_weight_calibrator
  - Called from of_timers_worker.py as an hourly timer task
"""
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    from common.log import setup_logger
    logger = setup_logger("EnsembleWeightCalibrator")
except ImportError:
    import logging
    logger = logging.getLogger("EnsembleWeightCalibrator")

from core.redis_keys import RedisStreams as RS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENSEMBLE_SOURCES: List[str] = ["orderflow", "ta_indicators", "microstructure", "regime_filter"]

# Source tag mapping: how to match trades_closed.source / indicators JSON
# to ensemble source names
SOURCE_TAG_MAP: Dict[str, List[str]] = {
    "orderflow": ["CryptoOrderFlow", "AggregatedHub-V2", "orderflow"],
    "ta_indicators": ["ta_indicators", "ta"],
    "microstructure": ["microstructure", "micro"],
    "regime_filter": ["regime_filter", "regime"],
}

DEFAULT_LOOKBACK_DAYS: int = 30
MIN_OUTCOMES_FOR_WEIGHT: int = 20
ANNUALIZATION_FACTOR: float = (365.25 * 24) ** 0.5  # for hourly data

# Telegram / notification
NOTIFY_STREAM = RS.NOTIFY_TELEGRAM
PENDING_TTL_SEC = 48 * 3600  # 48 hours

# Source name for human-readable display
SOURCE_DISPLAY: Dict[str, str] = {
    "orderflow": "📊 OrderFlow",
    "ta_indicators": "📈 TA Indicators",
    "microstructure": "🔬 Microstructure",
    "regime_filter": "🌊 Regime Filter",
}


# ---------------------------------------------------------------------------
# Sharpe computation (robust: MAD-based)
# ---------------------------------------------------------------------------

def compute_sharpe_robust(pnl_series: List[float]) -> float:
    """
    Compute annualized Sharpe ratio using MAD (Median Absolute Deviation)
    instead of standard deviation for robustness against outliers.

    MAD-based Sharpe = median(returns) / (1.4826 * MAD(returns)) * annualization

    The factor 1.4826 converts MAD to an estimate of std for normal data.

    Returns:
        Sharpe ratio (can be negative). Returns 0.0 if insufficient data.
    """
    if len(pnl_series) < MIN_OUTCOMES_FOR_WEIGHT:
        return 0.0

    if HAS_NUMPY:
        arr = np.array(pnl_series, dtype=np.float64)
        med = float(np.median(arr))
        mad = float(np.median(np.abs(arr - med)))
    else:
        sorted_data = sorted(pnl_series)
        n = len(sorted_data)
        if n % 2 == 0:
            med = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2.0
        else:
            med = sorted_data[n // 2]
        deviations = sorted(abs(x - med) for x in pnl_series)
        nd = len(deviations)
        if nd % 2 == 0:
            mad = (deviations[nd // 2 - 1] + deviations[nd // 2]) / 2.0
        else:
            mad = deviations[nd // 2]

    if mad < 1e-12:
        # All values are the same — zero volatility edge case
        return 10.0 if med > 0 else (-10.0 if med < 0 else 0.0)

    # 1.4826 * MAD ≈ std for normal distributions
    robust_std = 1.4826 * mad
    sharpe = (med / robust_std) * ANNUALIZATION_FACTOR

    # Cap at ±10 to avoid extreme values
    return max(-10.0, min(10.0, sharpe))


# ---------------------------------------------------------------------------
# Outcome loader
# ---------------------------------------------------------------------------

def load_outcomes(
    dsn: str,
    symbol: str,
    source_tags: List[str],
    days: int = DEFAULT_LOOKBACK_DAYS,
) -> List[float]:
    """
    Load PnL (R-multiple or raw pnl_pct) from trades_closed for given source tags.
    """
    if not source_tags:
        return []

    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5, application_name="ensemble_calibrator")

        safe_sql = f"""
        SELECT pnl_pct
        FROM trades_closed
        WHERE symbol = %(symbol)s
          AND exit_ts > NOW() - INTERVAL '{int(days)} days'
          AND (
            source = ANY(%(tags)s)
          )
        ORDER BY exit_ts DESC
        LIMIT 1000
        """

        with conn.cursor() as cur:
            cur.execute(safe_sql, {
                "symbol": symbol,
                "tags": source_tags,
            })
            rows = cur.fetchall()

        return [float(row[0]) for row in rows if row[0] is not None]

    except Exception as e:
        logger.warning("Failed to load outcomes for %s/%s: %s", symbol, source_tags, e)
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class WeightCalibrationResult:
    """Result of a single calibration run."""
    symbol: str
    weights: Dict[str, float]
    sharpes: Dict[str, float]
    outcome_counts: Dict[str, int]
    previous_weights: Dict[str, float]
    ts: str = ""

    def __post_init__(self) -> None:
        if not self.ts:
            self.ts = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Main calibrator
# ---------------------------------------------------------------------------

def _read_current_weights(redis_client: Any, symbol: str) -> Dict[str, float]:
    """Read current enforce weights from Redis for delta tracking."""
    try:
        h = redis_client.hgetall(f"weights:ensemble:{symbol}")
        if h:
            return {k: float(v) for k, v in h.items()}
    except Exception:
        pass
    return {}


def calibrate_ensemble_weights(
    symbol: str,
    dsn: str,
    redis_client: Any = None,
    days: int = DEFAULT_LOOKBACK_DAYS,
) -> WeightCalibrationResult:
    """
    Compute ensemble weights for a single symbol.

    For each source:
      1. Load outcomes from trades_closed matching source tags
      2. Compute robust Sharpe
      3. Normalize: negative Sharpe → 0, positive → proportional weight

    Returns WeightCalibrationResult with weights and diagnostics.
    """
    sharpes: Dict[str, float] = {}
    outcome_counts: Dict[str, int] = {}
    previous_weights = {}
    if redis_client:
        previous_weights = _read_current_weights(redis_client, symbol)

    for source in ENSEMBLE_SOURCES:
        tags = SOURCE_TAG_MAP.get(source, [source])
        outcomes = load_outcomes(dsn, symbol, tags, days)
        outcome_counts[source] = len(outcomes)

        if len(outcomes) < MIN_OUTCOMES_FOR_WEIGHT:
            logger.info(
                "[%s] %s: insufficient outcomes (%d < %d), using default weight",
                symbol, source, len(outcomes), MIN_OUTCOMES_FOR_WEIGHT,
            )
            sharpes[source] = 0.0
            continue

        sharpe = compute_sharpe_robust(outcomes)
        sharpes[source] = sharpe
        logger.info(
            "[%s] %s: outcomes=%d sharpe=%.3f",
            symbol, source, len(outcomes), sharpe,
        )

    # Normalize weights
    positive_sharpes = {s: max(0.0, v) for s, v in sharpes.items()}
    total_positive = sum(positive_sharpes.values())

    if total_positive <= 0:
        # All sources have negative or zero Sharpe → equal weights
        weights = {s: 1.0 / len(ENSEMBLE_SOURCES) for s in ENSEMBLE_SOURCES}
        logger.warning("[%s] All Sharpes <= 0 — using equal weights", symbol)
    else:
        weights = {s: v / total_positive for s, v in positive_sharpes.items()}

    return WeightCalibrationResult(
        symbol=symbol,
        weights=weights,
        sharpes=sharpes,
        outcome_counts=outcome_counts,
        previous_weights=previous_weights,
    )


# ---------------------------------------------------------------------------
# Weight publishing
# ---------------------------------------------------------------------------

def publish_weights(
    redis_client: Any,
    result: WeightCalibrationResult,
    *,
    shadow: bool = False,
) -> None:
    """Write calibrated weights to Redis.

    If shadow=True → writes to weights:ensemble:shadow:{symbol} (preview only).
    If shadow=False → writes to weights:ensemble:{symbol} (production).
    """
    if shadow:
        key = f"weights:ensemble:shadow:{result.symbol}"
    else:
        key = f"weights:ensemble:{result.symbol}"

    # Write weights hash
    pipe = redis_client.pipeline()
    pipe.delete(key)
    for source, weight in result.weights.items():
        pipe.hset(key, source, str(round(weight, 6)))
    if shadow:
        pipe.expire(key, PENDING_TTL_SEC)
    pipe.execute()

    # Write meta for observability
    meta = {
        "symbol": result.symbol,
        "weights": result.weights,
        "sharpes": result.sharpes,
        "outcome_counts": result.outcome_counts,
        "previous_weights": result.previous_weights,
        "shadow": shadow,
        "updated_at": result.ts,
    }
    redis_client.set(
        f"ensemble:weight_meta:{result.symbol}",
        json.dumps(meta, default=str),
        ex=86400,  # 24h TTL
    )

    mode_label = "shadow" if shadow else "enforce"
    logger.info(
        "✅ [%s] Ensemble weights published (%s): %s",
        result.symbol, mode_label,
        {s: f"{w:.3f}" for s, w in result.weights.items()},
    )


# ---------------------------------------------------------------------------
# Telegram report builder
# ---------------------------------------------------------------------------

def _generate_run_id() -> str:
    """Short unique run ID for this calibration cycle."""
    raw = f"{time.time()}-{uuid.uuid4().hex[:8]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def format_telegram_report(
    results: List[WeightCalibrationResult],
    mode: str,
    run_id: str,
    days: int,
) -> str:
    """Build Telegram HTML report for ensemble weight calibration."""
    if not results:
        return "⚖️ <b>Ensemble Weight Calibrator</b>\n\nНет данных для калибровки."

    emoji_mode = "🔧" if mode == "shadow" else "✅"
    lines = [
        f"{emoji_mode} <b>Ensemble Weight Calibrator</b> (mode=<code>{mode}</code>)",
        f"📅 Окно: <code>{days}d</code> | Символов: <code>{len(results)}</code>",
        "",
    ],

    for r in sorted(results, key=lambda x: x.symbol):
        lines.append(f"<b>{r.symbol}</b>:")
        for src in ENSEMBLE_SOURCES:
            w_new = r.weights.get(src, 0.0)
            w_old = r.previous_weights.get(src, 0.0)
            sharpe = r.sharpes.get(src, 0.0)
            n_outcomes = r.outcome_counts.get(src, 0)
            display = SOURCE_DISPLAY.get(src, src)

            delta = ""
            if w_old > 1e-6:
                pct_change = ((w_new - w_old) / w_old) * 100
                if abs(pct_change) > 0.5:
                    delta = f" (Δ{pct_change:+.1f}%)"

            status = "⚠️" if n_outcomes < MIN_OUTCOMES_FOR_WEIGHT else ("🟢" if sharpe > 0 else "🔴")
            lines.append(
                f"  {status} {display}: <code>{w_new:.3f}</code>{delta} | "
                f"Sharpe=<code>{sharpe:+.2f}</code> n=<code>{n_outcomes}</code>"
            )
        lines.append("")

    # Footer
    lines.append(f"Run ID: <code>{run_id}</code>")

    return "\n".join(lines)


def _build_buttons(run_id: str, mode: str) -> Optional[str]:
    """Build Telegram inline keyboard buttons for approve/reject.

    Only in shadow mode: user must approve to switch to enforce.
    """
    if mode != "shadow":
        return None

    buttons = [[
        {"text": "✅ Approve (enforce)", "callback_data": f"ensemble_approve:{run_id}"},
        {"text": "❌ Reject", "callback_data": f"ensemble_reject:{run_id}"},
    ]]
    return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))


def _store_pending(
    redis_client: Any,
    run_id: str,
    results: List[WeightCalibrationResult],
    mode: str,
) -> None:
    """Store calibration data for pending approval (used by callback handler)."""
    pending = {
        "run_id": run_id,
        "status": "PENDING",
        "mode": mode,
        "created_at_ms": get_ny_time_millis(),
        "symbols": [r.symbol for r in results],
        "n_symbols": len(results),
        "weight_details": [
            {
                "symbol": r.symbol,
                "weights": {s: round(w, 4) for s, w in r.weights.items()},
                "sharpes": {s: round(v, 3) for s, v in r.sharpes.items()},
                "outcome_counts": r.outcome_counts,
                "previous_weights": {s: round(w, 4) for s, w in r.previous_weights.items()},
            }
            for r in results
        ],
    }
    redis_client.set(
        f"ensemble:calib:pending:{run_id}",
        json.dumps(pending, ensure_ascii=False, default=str),
        ex=PENDING_TTL_SEC,
    )


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
        logger.info("📤 Telegram report sent (buttons=%s)", bool(buttons_json))
    except Exception as e:
        logger.error("Failed to send Telegram report: %s", e)


# ---------------------------------------------------------------------------
# Throttle: skip if last report was sent recently
# ---------------------------------------------------------------------------

def _should_run(redis_client: Any, interval_sec: int) -> bool:
    """Redis-based throttle for Telegram notifications."""
    key = "ensemble:calib:last_sent_ts"
    try:
        last_raw = redis_client.get(key)
        if last_raw:
            elapsed = time.time() - float(last_raw)
            if elapsed < interval_sec:
                logger.info(
                    "Throttled: last sent %ds ago (interval=%ds)",
                    int(elapsed), interval_sec,
                )
                return False
    except Exception:
        pass
    return True


def _record_sent(redis_client: Any, interval_sec: int) -> None:
    """Record last send timestamp for throttle."""
    try:
        redis_client.set(
            "ensemble:calib:last_sent_ts",
            str(time.time()),
            ex=interval_sec * 2,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Entrypoints
# ---------------------------------------------------------------------------

def run_calibration_for_symbols(
    symbols: List[str],
    dsn: str,
    redis_url: str,
    days: int = DEFAULT_LOOKBACK_DAYS,
    mode: str = "shadow",
    send_telegram: bool = True,
    telegram_interval_sec: int = 3600,
) -> List[WeightCalibrationResult]:
    """
    Run weight calibration for all specified symbols.

    Args:
        symbols: List of trading symbols
        dsn: PostgreSQL DSN for trades_closed
        redis_url: Redis URL
        days: Lookback window in days
        mode: "shadow" (preview + buttons) or "enforce" (direct apply)
        send_telegram: Whether to send Telegram notification
        telegram_interval_sec: Throttle interval
    """
    r = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    # Throttle check
    if send_telegram and not _should_run(r, telegram_interval_sec):
        return []

    results: List[WeightCalibrationResult] = []
    for symbol in symbols:
        try:
            result = calibrate_ensemble_weights(symbol, dsn, r, days)
            # In shadow mode → write to shadow key; in enforce → write to production key
            is_shadow = (mode == "shadow")
            publish_weights(r, result, shadow=is_shadow)
            results.append(result)
        except Exception as e:
            logger.error("[%s] Calibration failed: %s", symbol, e, exc_info=True)

    if not results:
        logger.info("No calibration results produced")
        return results

    # Generate run ID and store pending data
    run_id = _generate_run_id()

    if mode == "shadow":
        _store_pending(r, run_id, results, mode)

    # Send Telegram report
    if send_telegram:
        text = format_telegram_report(results, mode, run_id, days)
        buttons_json = _build_buttons(run_id, mode)
        _send_telegram(r, text, buttons_json)
        _record_sent(r, telegram_interval_sec)

    logger.info(
        "Ensemble calibration complete: %d symbols, mode=%s, run_id=%s",
        len(results), mode, run_id,
    )
    return results


def run_once() -> None:
    """CLI entrypoint — run one calibration pass for configured symbols."""
    dsn = (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or os.getenv("PG_DSN_CALIBRATION")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    symbols_raw = os.getenv("ENSEMBLE_CALIBRATION_SYMBOLS", "BTCUSDT,ETHUSDT")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    days = int(os.getenv("ENSEMBLE_LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
    mode = os.getenv("ENSEMBLE_CALIB_MODE", "shadow")
    send_tg = os.getenv("ENSEMBLE_CALIB_TELEGRAM", "1").strip().lower() in ("1", "true", "yes")
    interval = int(os.getenv("ENSEMBLE_CALIB_TELEGRAM_INTERVAL_SEC", "3600"))

    logger.info(
        "Ensemble weight calibration: symbols=%s days=%d mode=%s dsn=%s",
        symbols, days, mode, dsn[:40] + "...",
    )

    results = run_calibration_for_symbols(
        symbols, dsn, redis_url, days,
        mode=mode,
        send_telegram=send_tg,
        telegram_interval_sec=interval,
    )
    for result in results:
        logger.info(
            "Result: %s weights=%s sharpes=%s",
            result.symbol,
            {s: f"{w:.3f}" for s, w in result.weights.items()},
            {s: f"{v:.3f}" for s, v in result.sharpes.items()},
        )


if __name__ == "__main__":
    run_once()
