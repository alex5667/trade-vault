from __future__ import annotations

import json
import os
import time

import psycopg2
import psycopg2.extras
import redis

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _min_n() -> int:
    try:
        return int(os.getenv("ATR_PROMOTION_MIN_N", "80") or 80)
    except Exception:
        return 80


def _window_days() -> int:
    try:
        return int(os.getenv("ATR_PROMOTION_WINDOW_DAYS", "21") or 21)
    except Exception:
        return 21


def _promote_stop(row) -> bool:
    return bool(
        (row["stop_n_canary"] or 0) >= _min_n()
        and (row["stop_n_control"] or 0) >= _min_n()
        and (row["stop_pnl_canary"] or 0.0) >= (row["stop_pnl_control"] or 0.0)
        and (row["stop_tp1_canary"] or 0.0) >= (row["stop_tp1_control"] or 0.0) - 0.01
        and (row["stop_slip_canary"] or 0.0) <= (row["stop_slip_control"] or 0.0) + 0.5
    )


def _promote_trailing(row) -> bool:
    return bool(
        (row["trail_n_canary"] or 0) >= _min_n()
        and (row["trail_n_control"] or 0) >= _min_n()
        and (row["trail_pnl_canary"] or 0.0) >= (row["trail_pnl_control"] or 0.0)
        and (row["trail_mfe_canary"] or 0.0) >= (row["trail_mfe_control"] or 0.0)
    )


def run_once() -> int:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_promotion_policy_service")
    r = _redis()
    written = 0
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                WITH stop_ab AS (
                  SELECT
                    source, symbol, scenario, regime, risk_horizon_bucket,
                    max(CASE WHEN live_surface_applied THEN n END) AS stop_n_canary,
                    max(CASE WHEN NOT live_surface_applied THEN n END) AS stop_n_control,
                    max(CASE WHEN live_surface_applied THEN avg_pnl_bps END) AS stop_pnl_canary,
                    max(CASE WHEN NOT live_surface_applied THEN avg_pnl_bps END) AS stop_pnl_control,
                    max(CASE WHEN live_surface_applied THEN tp1_rate END) AS stop_tp1_canary,
                    max(CASE WHEN NOT live_surface_applied THEN tp1_rate END) AS stop_tp1_control,
                    max(CASE WHEN live_surface_applied THEN avg_slippage_bps END) AS stop_slip_canary,
                    max(CASE WHEN NOT live_surface_applied THEN avg_slippage_bps END) AS stop_slip_control
                  FROM horizon_live_surface_ab_daily
                  WHERE day >= current_date - %s::int
                  GROUP BY 1,2,3,4,5
                ),
                trail_ab AS (
                  SELECT
                    source, symbol, kind AS scenario, 'na'::text AS regime, risk_horizon_bucket,
                    max(CASE WHEN trailing_surface_applied THEN n END) AS trail_n_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN n END) AS trail_n_control,
                    max(CASE WHEN trailing_surface_applied THEN avg_pnl_bps END) AS trail_pnl_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN avg_pnl_bps END) AS trail_pnl_control,
                    max(CASE WHEN trailing_surface_applied THEN avg_mfe_pnl END) AS trail_mfe_canary,
                    max(CASE WHEN NOT trailing_surface_applied THEN avg_mfe_pnl END) AS trail_mfe_control
                  FROM horizon_trailing_ab_daily
                  WHERE day >= current_date - %s::int
                  GROUP BY 1,2,3,4,5
                )
                SELECT
                  s.source, s.symbol, s.scenario, s.regime, s.risk_horizon_bucket,
                  s.stop_n_canary, s.stop_n_control, s.stop_pnl_canary, s.stop_pnl_control,
                  s.stop_tp1_canary, s.stop_tp1_control, s.stop_slip_canary, s.stop_slip_control,
                  t.trail_n_canary, t.trail_n_control, t.trail_pnl_canary, t.trail_pnl_control,
                  t.trail_mfe_canary, t.trail_mfe_control
                FROM stop_ab s
                LEFT JOIN trail_ab t
                  ON t.source = s.source
                 AND t.symbol = s.symbol
                 AND t.scenario = s.scenario
                 AND t.risk_horizon_bucket = s.risk_horizon_bucket
                """
                ,
                (_window_days(), _window_days()),
            )

            for row in cur.fetchall():
                promote_stop = _promote_stop(row)
                promote_trailing = _promote_trailing(row)

                stop_mode = "live" if promote_stop else "canary"
                trailing_mode = "live" if promote_trailing else "canary"

                reason = (
                    "ATR_POLICY_PROMOTED_BOTH" if promote_stop and promote_trailing else
                    "ATR_POLICY_PROMOTED_STOP_ONLY" if promote_stop else
                    "ATR_POLICY_PROMOTED_TRAILING_ONLY" if promote_trailing else
                    "ATR_POLICY_STAY_CANARY"
                )

                payload = {
                    "policy_ver": 1,
                    "source": row["source"],
                    "symbol": row["symbol"],
                    "scenario": row["scenario"],
                    "regime": row["regime"],
                    "risk_horizon_bucket": row["risk_horizon_bucket"],
                    "stop_ttl_mode": stop_mode,
                    "trailing_mode": trailing_mode,
                    "reason_code": reason,
                    "approved": False,
                    "updated_at_ms": int(time.time() * 1000),
                    "evidence": {
                        "stop_ttl": {
                            "n_canary": row["stop_n_canary"],
                            "n_control": row["stop_n_control"],
                            "pnl_canary": row["stop_pnl_canary"],
                            "pnl_control": row["stop_pnl_control"],
                            "tp1_canary": row["stop_tp1_canary"],
                            "tp1_control": row["stop_tp1_control"],
                            "slip_canary": row["stop_slip_canary"],
                            "slip_control": row["stop_slip_control"],
                        },
                        "trailing": {
                            "n_canary": row["trail_n_canary"],
                            "n_control": row["trail_n_control"],
                            "pnl_canary": row["trail_pnl_canary"],
                            "pnl_control": row["trail_pnl_control"],
                            "mfe_canary": row["trail_mfe_canary"],
                            "mfe_control": row["trail_mfe_control"],
                        },
                    },
                }

                key = f"cfg:suggestions:atr_policy:{row['source']}:{row['symbol']}:{row['scenario']}:{row['regime']}:{row['risk_horizon_bucket']}"
                r.set(key, json.dumps(payload, ensure_ascii=False, sort_keys=True))

                try:
                    from services.atr_policy_workflow import submit_proposal
                    proposal_id = submit_proposal(payload)
                    payload["proposal_id"] = proposal_id
                    r.set(key, json.dumps(payload, ensure_ascii=False, sort_keys=True))
                    from services.atr_policy_telegram_ops import publish_policy_proposal_to_telegram
                    publish_policy_proposal_to_telegram(payload)
                except Exception:
                    pass

                try:
                    from services.atr_promotion_policy_metrics import atr_promotion_policy_suggest_total
                    atr_promotion_policy_suggest_total.labels(reason_code=reason).inc()
                except Exception:
                    pass

                written += 1
    finally:
        conn.close()
    return written


if __name__ == "__main__":
    print(run_once())
