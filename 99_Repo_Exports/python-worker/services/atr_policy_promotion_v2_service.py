"""ATR Policy Promotion V2 Service — Phase 5.1

Evaluates provenanced closed trades and decides (PROMOTE, HOLD, ROLLBACK).
Writes decisions to cfg:suggestions:atr_policy_v2:* and notify:telegram.
Provides Prometheus metrics.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras
import redis
from prometheus_client import Counter, Gauge, start_http_server

from core.redis_client import get_atr_redis

logger = logging.getLogger(__name__)

# Metrics
g_score = Gauge(
    "atr_policy_promotion_v2_score",
    "Action score of the cohort",
    ["symbol", "scenario", "regime", "bucket", "layer", "cert_status", "policy_ver"]
)
g_n_trades = Gauge(
    "atr_policy_promotion_v2_n_trades",
    "Number of trades in the cohort",
    ["symbol", "scenario", "regime", "bucket", "layer", "cert_status", "policy_ver"]
)
g_action = Gauge(
    "atr_policy_promotion_v2_action_state",
    "Action calculated for cohort (0=HOLD, -1=ROLLBACK, 1=PROMOTE)",
    ["symbol", "scenario", "regime", "bucket", "layer", "cert_status", "policy_ver"]
)
c_action_total = Counter(
    "atr_policy_promotion_v2_action_total",
    "Total actions decided by V2 loop",
    ["action", "layer"]
)
c_low_sample_total = Counter(
    "atr_policy_promotion_v2_low_sample_total",
    "Total cohorts evaluated but skipped due to low sample",
    ["layer"]
)
c_rollback_total = Counter(
    "atr_policy_promotion_v2_rollback_total",
    "Total cohorts scoring deeply negative suggested for rollback",
    ["layer"]
)
c_promote_total = Counter(
    "atr_policy_promotion_v2_promote_total",
    "Total cohorts scoring positive with enough samples suggested for promote",
    ["layer"]
)

c_loop_runs = Counter(
    "atr_policy_promotion_v2_loop_runs_total",
    "Total executions of the promotion V2 loop"
)
c_loop_errors = Counter(
    "atr_policy_promotion_v2_loop_errors_total",
    "Total errors inside the promotion V2 loop"
)

def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis() -> redis.Redis:
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _hard_min_n() -> int:
    return int(os.getenv("ATR_POLICY_PROMOTION_MIN_N", "50"))

def _rollback_min_n() -> int:
    return int(os.getenv("ATR_POLICY_ROLLBACK_MIN_N", "30"))

def _score(row: Dict[str, Any]) -> float:
    """Calculate the score for a specific layer cohort based on delta vs baseline."""
    cert = str(row.get("restore_cert_status") or "")
    cert_bonus = 0.40 if cert == "passed" else (-0.50 if cert in {"failed", "stale"} else 0.0)

    n = float(row.get("n_trades") or 0)
    min_n = float(_hard_min_n())
    sample_penalty = max(0.0, min_n - n) / min_n

    return (
        1.00 * float(row.get("delta_pnl_bps") or 0.0)
        + 0.35 * float(row.get("delta_win_rate") or 0.0) * 100.0
        - 0.70 * float(row.get("delta_slippage_bps") or 0.0)
        - 0.50 * float(row.get("delta_stop_rate") or 0.0) * 100.0
        + 0.30 * float(row.get("delta_tp1_rate") or 0.0) * 100.0
        - 0.25 * float(row.get("delta_mae_pct") or 0.0) * 100.0
        + cert_bonus
        - 1.20 * sample_penalty
    )

def _determine_action(score: float, n_trades: int) -> tuple[str, str]:
    if score >= 0.75 and n_trades >= _hard_min_n():
        return "PROMOTE", "ATR_POLICY_PROMOTE_V2"
    elif score <= -0.75 and n_trades >= _rollback_min_n():
        return "ROLLBACK", "ATR_POLICY_ROLLBACK_V2"
    else:
        return "HOLD", "ATR_POLICY_HOLD_V2"

def _run_loop() -> None:
    r = _redis()
    
    try:
        conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_promotion_v2_service")
    except Exception as e:
        logger.error(f"Failed to connect to analytics DB: {e}")
        return

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH base AS (
                  SELECT
                    symbol,
                    scenario,
                    regime,
                    bucket,
                    atr_stop_ttl_mode,
                    atr_trailing_mode,
                    atr_policy_ver,
                    atr_restore_cert_status AS restore_cert_status,
                    n_trades,
                    avg_pnl_bps,
                    avg_slippage_bps,
                    avg_mae_pct,
                    win_rate,
                    stop_rate,
                    tp1_rate
                  FROM v_atr_policy_promotion_inputs
                ),
                stop_pairs AS (
                  SELECT
                    c.symbol,
                    c.scenario,
                    c.regime,
                    c.bucket,
                    'stop_ttl'::text AS layer,
                    c.atr_policy_ver,
                    c.restore_cert_status,
                    c.n_trades,
                    c.avg_pnl_bps - b.avg_pnl_bps AS delta_pnl_bps,
                    c.avg_slippage_bps - b.avg_slippage_bps AS delta_slippage_bps,
                    c.avg_mae_pct - b.avg_mae_pct AS delta_mae_pct,
                    c.win_rate - b.win_rate AS delta_win_rate,
                    c.stop_rate - b.stop_rate AS delta_stop_rate,
                    c.tp1_rate - b.tp1_rate AS delta_tp1_rate
                  FROM base c
                  JOIN base b
                    ON b.symbol = c.symbol
                   AND b.scenario = c.scenario
                   AND b.regime = c.regime
                   AND b.bucket = c.bucket
                   AND b.atr_trailing_mode = c.atr_trailing_mode
                  WHERE c.atr_stop_ttl_mode = 'live'
                    AND b.atr_stop_ttl_mode IN ('canary','shadow')
                ),
                trail_pairs AS (
                  SELECT
                    c.symbol,
                    c.scenario,
                    c.regime,
                    c.bucket,
                    'trailing'::text AS layer,
                    c.atr_policy_ver,
                    c.restore_cert_status,
                    c.n_trades,
                    c.avg_pnl_bps - b.avg_pnl_bps AS delta_pnl_bps,
                    c.avg_slippage_bps - b.avg_slippage_bps AS delta_slippage_bps,
                    c.avg_mae_pct - b.avg_mae_pct AS delta_mae_pct,
                    c.win_rate - b.win_rate AS delta_win_rate,
                    c.stop_rate - b.stop_rate AS delta_stop_rate,
                    c.tp1_rate - b.tp1_rate AS delta_tp1_rate
                  FROM base c
                  JOIN base b
                    ON b.symbol = c.symbol
                   AND b.scenario = c.scenario
                   AND b.regime = c.regime
                   AND b.bucket = c.bucket
                   AND b.atr_stop_ttl_mode = c.atr_stop_ttl_mode
                  WHERE c.atr_trailing_mode = 'live'
                    AND b.atr_trailing_mode IN ('canary','shadow')
                )
                SELECT * FROM stop_pairs
                UNION ALL
                SELECT * FROM trail_pairs
            """)
            rows = cur.fetchall()
            
        if not rows:
            logger.info("No paired data found for promotion v2 inputs.")
            return

        for row in rows:
            symbol = row.get("symbol", "unknown")
            scenario = row.get("scenario", "")
            regime = row.get("regime", "")
            bucket = row.get("bucket", "")
            ver = row.get("atr_policy_ver", 0)
            cert = row.get("restore_cert_status", "")
            n = int(row.get("n_trades", 0) or 0)
            layer = row.get("layer", "")
            
            score = _score(row)
            action, reason_code = _determine_action(score, n)
            
            # Expose metrics
            action_val = 1 if action == "PROMOTE" else (-1 if action == "ROLLBACK" else 0)
            g_score.labels(
                symbol=symbol,
                scenario=scenario,
                regime=regime,
                bucket=bucket,
                layer=layer,
                cert_status=cert,
                policy_ver=str(ver),
            ).set(score)
            g_n_trades.labels(
                symbol=symbol,
                scenario=scenario,
                regime=regime,
                bucket=bucket,
                layer=layer,
                cert_status=cert,
                policy_ver=str(ver),
            ).set(n)
            g_action.labels(
                symbol=symbol,
                scenario=scenario,
                regime=regime,
                bucket=bucket,
                layer=layer,
                cert_status=cert,
                policy_ver=str(ver),
            ).set(action_val)
            
            c_action_total.labels(action=action, layer=layer).inc()
            
            if n < _hard_min_n():
                c_low_sample_total.labels(layer=layer).inc()
                
            if action == "ROLLBACK":
                c_rollback_total.labels(layer=layer).inc()
            elif action == "PROMOTE":
                c_promote_total.labels(layer=layer).inc()
            
            # Write to Redis (Suggestion Payload v2)
            suggestion_key = f"cfg:suggestions:atr_policy_v2:{symbol}:{scenario}:{regime}:{bucket}:{layer}"
            payload = {
                "symbol": symbol,
                "scenario": scenario,
                "regime": regime,
                "risk_horizon_bucket": bucket,
                "layer": layer,
                "action": action,
                "policy_ver": int(ver or 0),
                "score": score,
                "reason_code": reason_code,
                "restore_cert_status": cert,
                "evidence": dict(row),
                "created_at_ms": int(time.time() * 1000),
                "updated_at_ms": int(time.time() * 1000)
            }
            r.set(suggestion_key, json.dumps(payload, default=str))

    except Exception as exc:
        logger.exception("Error in atr_policy_promotion_v2_service loop: %s", exc)
        c_loop_errors.inc()
    finally:
        conn.close()

def run_forever() -> None:
    port = int(os.getenv("ATR_POLICY_PROMOTION_V2_METRICS_PORT", "9145"))
    start_http_server(port)
    logger.info("atr_policy_promotion_v2_service: metrics server started on :%d", port)

    interval = int(os.getenv("ATR_POLICY_PROMOTION_V2_INTERVAL_SEC", "3600"))

    while True:
        c_loop_runs.inc()
        _run_loop()
        time.sleep(interval)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
