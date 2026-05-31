"""Phase C.4: Конкретный entrypoint для PromotionRunner.

Подключает три I/O-коллбэка к PromotionRunner (fetch_rows / write_decision /
publish_snapshot) и запускает цикл с интервалом PROMOTION_INTERVAL_SEC.

ENV knobs:
  TRADES_DB_DSN / ANALYTICS_DB_DSN  — Postgres DSN
  REDIS_URL                          — redis-worker-1 (autocal:regime_exec:state)
  PROMOTION_ENFORCE                  — 0=shadow (default), 1=enforce
  REGIME_EXEC_AUTOCAL_HMAC_SECRET    — HMAC secret (обязателен для enforce)
  PROMOTION_INTERVAL_SEC             — интервал между запусками (default 21600 = 6h)
  PROMOTION_MIN_N / _MIN_EV_R / _MIN_AVG_R / _MAX_TIMEOUT_RATE / _Z_ALPHA
  PROMOTION_METRICS_PORT             — Prometheus port (default 9841)
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Generator

import redis

from services.regime_exec_promotion_v1 import (
    BucketDecision,
    BucketRow,
    PromotionGates,
    PromotionRunner,
)

log = logging.getLogger("regime_exec_promotion_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# ──────────────────────── Prometheus (fail-open) ─────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server
    _run_total = Counter("promotion_run_total", "Total PromotionRunner.run_once() calls", ["status"])
    _decisions = Counter("promotion_decisions_total", "Decisions emitted", ["decision"])
    _last_run_ts = Gauge("promotion_last_run_ts_ms", "Epoch ms of last completed run")
    _PROM_OK = True
except Exception:
    _run_total = _decisions = _last_run_ts = None  # type: ignore[assignment]
    _PROM_OK = False

# ─────────────────────── Postgres DSN ────────────────────────────────────────
_DEFAULT_DSN = "postgresql://postgres:postgres@postgres:5432/trades"
TRADES_DB_DSN = (
    os.getenv("TRADES_DB_DSN")
    or os.getenv("ANALYTICS_DB_DSN")
    or _DEFAULT_DSN
)


def _get_pg_conn():
    import psycopg2
    return psycopg2.connect(TRADES_DB_DSN, connect_timeout=10)


# ─────────────────────── I/O callbacks ──────────────────────────────────────

def fetch_rows() -> Generator[BucketRow, None, None]:
    """Читает strategy_bucket_outcomes_14d → BucketRow."""
    sql = """
        SELECT
            symbol,
            regime_label,
            scenario,
            direction,
            n,
            win_rate,
            avg_r,
            ev_r_after_costs,
            mfe_r_p50,
            mfe_r_p90,
            mae_r_p50,
            mae_r_p90,
            timeout_rate
        FROM strategy_bucket_outcomes_14d
        WHERE n > 0
        ORDER BY ev_r_after_costs DESC
    """
    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute(sql)
        for row in cur.fetchall():
            (symbol, regime_label, scenario, direction, n,
             win_rate, avg_r, ev_r, mfe_p50, mfe_p90,
             mae_p50, mae_p90, timeout_rate) = row
            yield BucketRow(
                symbol=symbol or "GLOBAL",
                regime_label=regime_label or "na",
                scenario=scenario or "na",
                direction=direction or "na",
                n=int(n or 0),
                win_rate=float(win_rate or 0.0),
                avg_r=float(avg_r or 0.0),
                ev_r_after_costs=float(ev_r or 0.0),
                mfe_r_p50=float(mfe_p50) if mfe_p50 is not None else None,
                mfe_r_p90=float(mfe_p90) if mfe_p90 is not None else None,
                mae_r_p50=float(mae_p50) if mae_p50 is not None else None,
                mae_r_p90=float(mae_p90) if mae_p90 is not None else None,
                timeout_rate=float(timeout_rate or 0.0),
            )
        cur.close()
        conn.close()
    except Exception as e:
        log.error("fetch_rows failed: %s", e)


_INSERT_DECISION_SQL = """
INSERT INTO strategy_bucket_metrics (
    ts, symbol, vol_regime, trend_regime, liquidity_class, entry_profile,
    trail_profile, n, win_rate, ev_r, avg_r,
    bootstrap_ci_low, bootstrap_ci_high, decision, policy_hash
) VALUES (
    now(), %s, %s, %s, 'na', 'na', %s,
    %s, %s, %s, %s, %s, %s, %s, %s
)
"""


def write_decision(d: BucketDecision) -> None:
    """INSERT одного решения в strategy_bucket_metrics."""
    parts = d.bucket_key.split("|")
    symbol = parts[0] if parts else "GLOBAL"
    regime = parts[1] if len(parts) > 1 else "na"
    scenario = parts[2] if len(parts) > 2 else "na"
    trail_profile = (d.proposed_policy.get("trail_profile") or "na") if d.proposed_policy else "na"

    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute(_INSERT_DECISION_SQL, (
            symbol, regime, scenario, trail_profile,
            d.n,
            round(d.ev_r / d.n, 4) if d.n > 0 else 0.0,  # win_rate proxy
            round(d.ev_r, 6),
            round(d.avg_r, 6),
            round(d.ci_low, 6) if d.ci_low == d.ci_low else None,    # NaN guard
            round(d.ci_high, 6) if d.ci_high == d.ci_high else None,
            d.decision,
            json.dumps(d.proposed_policy) if d.proposed_policy else None,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.warning("write_decision failed for %s: %s", d.bucket_key, e)
    else:
        if _decisions is not None:
            try:
                _decisions.labels(decision=d.decision).inc()
            except Exception:
                pass


# Redis key that _RegimeExecOverridesReader watches
_AUTOCAL_REGIME_EXEC_KEY = "autocal:regime_exec:state"


def _make_redis_client() -> redis.Redis:
    return redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def publish_snapshot(snap: dict[str, Any]) -> None:
    """Атомарно пишет snapshot в Redis (SET + expire 25h)."""
    rc = _make_redis_client()
    payload = json.dumps(snap, separators=(",", ":"))
    rc.set(_AUTOCAL_REGIME_EXEC_KEY, payload, ex=25 * 3600)
    log.info(
        "promotion snapshot published: %d enforce_proposed buckets ts_ms=%s",
        len(snap.get("buckets", {})),
        snap.get("ts_ms"),
    )


# ─────────────────────────── main loop ───────────────────────────────────────

def main() -> None:
    if _PROM_OK:
        try:
            port = int(os.getenv("PROMOTION_METRICS_PORT", "9841"))
            from prometheus_client import start_http_server
            start_http_server(port)
            log.info("Prometheus on :%d", port)
        except Exception as e:
            log.warning("Prometheus start failed: %s", e)

    enforce = os.getenv("PROMOTION_ENFORCE", "0").strip() in {"1", "true", "yes"}
    hmac_secret = os.getenv("REGIME_EXEC_AUTOCAL_HMAC_SECRET", "").strip()
    interval_s = int(os.getenv("PROMOTION_INTERVAL_SEC", str(6 * 3600)))

    runner = PromotionRunner(
        fetch_rows=fetch_rows,
        write_decision=write_decision,
        publish_snapshot=publish_snapshot,
        gates=PromotionGates.from_env(),
        enforce=enforce,
        hmac_secret=hmac_secret,
    )

    log.info(
        "PromotionRunner started | enforce=%s hmac=%s interval=%ds",
        enforce, bool(hmac_secret), interval_s,
    )

    while True:
        t0 = time.time()
        try:
            decisions = runner.run_once()
            elapsed = time.time() - t0
            log.info("run_once: %d decisions in %.1fs", len(decisions), elapsed)
            if _run_total is not None:
                _run_total.labels(status="ok").inc()
            if _last_run_ts is not None:
                _last_run_ts.set(int(time.time() * 1000))
        except Exception as e:
            log.error("run_once failed: %s", e)
            if _run_total is not None:
                try:
                    _run_total.labels(status="error").inc()
                except Exception:
                    pass

        sleep_s = max(60, interval_s - (time.time() - t0))
        log.info("next run in %.0fs", sleep_s)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
