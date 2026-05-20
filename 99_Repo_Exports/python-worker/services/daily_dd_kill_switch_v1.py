#!/usr/bin/env python3
"""Daily Equity-Drawdown Kill-Switch — v1.

Цель:
  Halt всей эмиссии сигналов когда дневной realized PnL пробивает порог.
  Дополняет существующий RISK_MAX_DAILY_LOSS_PCT (который делает FORCE_FLATTEN
  открытых позиций) — этот сервис блокирует ВХОД В НОВЫЕ.

Триггер (OR):
  - sum(r_multiple) сегодня (UTC) <= -ABS(KILL_DAILY_R_LIMIT)         # дефолт 15R
  - sum(pnl_pct)    сегодня (UTC) <= -ABS(KILL_DAILY_PCT_LIMIT)       # дефолт 20%

Поведение:
  - Sticky-armed: если хоть раз сработал — остаётся armed до UTC 00:00.
  - Reset: автоматически на следующий UTC-день (breached_day_utc != today_utc).
  - Manual reset: HDEL risk:daily_dd:state kill_armed breached_at_ms reason.
  - Fail-open: если Postgres недоступен — kill_armed НЕ выставляется,
    metric daily_dd_data_missing_total растёт.

Источник PnL: trades_closed (canonical), колонки `r_multiple`, `pnl_pct`,
гипертабла по `exit_ts`. Подсчёт через `WHERE exit_ts >= date_trunc('day', now() AT TIME ZONE 'UTC')`.

Modes:
  DAILY_DD_KILLSWITCH_MODE = shadow | enforce   (default: shadow)
    shadow  — пишет state + увеличивает would_veto-метрики, но reader не armed.
    enforce — gate реально режет сигналы (VETO_DAILY_DD_KILLSWITCH).
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import redis
from psycopg2.extras import DictCursor

from core.redis_keys import RK

logger = logging.getLogger("daily_dd_kill_switch")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger.addHandler(h)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


# ────────────────────────────── config ──────────────────────────────

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://trading:password@scanner-postgres:5432/trade",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

CHECK_INTERVAL_SEC = int(os.getenv("DAILY_DD_CHECK_INTERVAL_SEC", "60") or "60")

# Triggers (positive numbers; код сам берёт abs()).
KILL_DAILY_R_LIMIT = float(os.getenv("KILL_DAILY_R_LIMIT", "15") or "15")
KILL_DAILY_PCT_LIMIT = float(os.getenv("KILL_DAILY_PCT_LIMIT", "20") or "20")

# Mode — kill armed только если ENV == 'enforce'; в shadow пишем state, gate не режет.
MODE = (os.getenv("DAILY_DD_KILLSWITCH_MODE", "shadow") or "shadow").strip().lower()
ENABLED = (os.getenv("DAILY_DD_KILLSWITCH_ENABLED", "1") or "1").strip() in ("1", "true", "on", "yes")

# Prometheus exporter port.
PROM_PORT = int(os.getenv("DAILY_DD_PROM_PORT", "9700") or "9700")


# ───────────────────────────── metrics ──────────────────────────────

try:
    from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

    G_R_SUM = Gauge("daily_pnl_r_sum", "Sum of r_multiple for trades closed today (UTC)")
    G_PCT_SUM = Gauge("daily_pnl_pct_sum", "Sum of pnl_pct for trades closed today (UTC)")
    G_TRADES = Gauge("daily_dd_trades_count", "Number of trades closed today (UTC)")
    G_KILL_ARMED = Gauge(
        "daily_dd_kill_armed",
        "1 if kill switch is currently armed (sticky for the UTC day)",
        ["mode"],
    )
    G_R_THR = Gauge("daily_dd_threshold_r", "Configured R limit (absolute value)")
    G_PCT_THR = Gauge("daily_dd_threshold_pct", "Configured pct limit (absolute value)")
    G_LAST_CHECK = Gauge("daily_dd_last_check_ms", "Epoch-ms of last successful check")
    C_TRIGGERED = Counter(
        "daily_dd_killswitch_triggered_total",
        "Count of state-transitions to armed=1 (per UTC day, per mode)",
        ["mode", "reason"],
    )
    C_RESET = Counter(
        "daily_dd_killswitch_reset_total",
        "Auto-reset events at UTC midnight",
    )
    C_DATA_MISSING = Counter(
        "daily_dd_data_missing_total",
        "Failed queries to trades_closed (fail-open: kill stays unarmed)",
        ["reason"],
    )
    _METRICS_OK = True
except Exception:  # pragma: no cover
    _METRICS_OK = False
    G_R_SUM = G_PCT_SUM = G_TRADES = G_KILL_ARMED = G_R_THR = G_PCT_THR = G_LAST_CHECK = None  # type: ignore
    C_TRIGGERED = C_RESET = C_DATA_MISSING = None  # type: ignore


# ────────────────────────────── core ────────────────────────────────


def _utc_day_str(ts_ms: int | None = None) -> str:
    """YYYY-MM-DD в UTC."""
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _query_daily_pnl(cur) -> tuple[float, float, int]:
    """Возвращает (r_sum, pct_sum, trades_count) за сегодняшний UTC-день.

    NULL r_multiple/pnl_pct трактуются как 0 (COALESCE) — гипотетически плохо
    для R-логики (фактический PnL > 0 будет занижен), но это симметрично
    относительно знака и не даёт ложных kill-ов.

    Raises:
        psycopg2.Error — если таблицы нет/SQL fail; вызывающий должен ловить.
    """
    sql = """
        SELECT
            COALESCE(SUM(r_multiple), 0)::float8 AS r_sum,
            COALESCE(SUM(pnl_pct),    0)::float8 AS pct_sum,
            COUNT(*)::bigint                     AS trades_count
        FROM trades_closed
        WHERE exit_ts >= date_trunc('day', now() AT TIME ZONE 'UTC')
          AND is_final_close = TRUE
    """
    cur.execute(sql)
    row = cur.fetchone()
    if row is None:
        return 0.0, 0.0, 0
    return float(row["r_sum"] or 0.0), float(row["pct_sum"] or 0.0), int(row["trades_count"] or 0)


def _read_existing_state(r: redis.Redis) -> dict[str, str]:
    try:
        raw: Any = r.hgetall(RK.DAILY_DD_STATE) or {}
        return {str(k): str(v) for k, v in raw.items()}
    except Exception:
        return {}


def _check_breach(r_sum: float, pct_sum: float) -> tuple[bool, str]:
    """Возвращает (breached, reason)."""
    r_lim = abs(KILL_DAILY_R_LIMIT)
    pct_lim = abs(KILL_DAILY_PCT_LIMIT)
    reasons = []
    if r_lim > 0 and r_sum <= -r_lim:
        reasons.append(f"r_sum={r_sum:.2f}<=-{r_lim:.2f}R")
    if pct_lim > 0 and pct_sum <= -pct_lim:
        reasons.append(f"pct_sum={pct_sum:.2f}<=-{pct_lim:.2f}%")
    return bool(reasons), ";".join(reasons)


def _write_state(
    r: redis.Redis,
    *,
    r_sum: float,
    pct_sum: float,
    trades_count: int,
    kill_armed: bool,
    breached_at_ms: int,
    breached_day_utc: str,
    reason: str,
    now_ms: int,
) -> None:
    payload = {
        "r_sum": f"{r_sum:.6f}",
        "pct_sum": f"{pct_sum:.6f}",
        "trades_count": str(trades_count),
        "kill_armed": "1" if kill_armed else "0",
        "mode": MODE,
        "threshold_r": f"{abs(KILL_DAILY_R_LIMIT):.4f}",
        "threshold_pct": f"{abs(KILL_DAILY_PCT_LIMIT):.4f}",
        "breached_at_ms": str(breached_at_ms),
        "breached_day_utc": breached_day_utc,
        "reason": reason,
        "updated_at_ms": str(now_ms),
    }
    try:
        # HSET multi-field; preserve any extra fields user may have set manually.
        r.hset(RK.DAILY_DD_STATE, mapping=payload)
    except Exception as e:
        logger.warning("daily_dd: HSET failed: %s", e)


def check_once(*, r: redis.Redis, conn) -> None:
    """Один проход: query → compute → write state."""
    now_ms = int(time.time() * 1000)
    today_utc = _utc_day_str(now_ms)

    prev = _read_existing_state(r)
    prev_armed = prev.get("kill_armed", "0") == "1"
    prev_day = prev.get("breached_day_utc", "")
    prev_reason = prev.get("reason", "")
    prev_breached_at = int(prev.get("breached_at_ms", "0") or "0")

    # Auto-reset на UTC-midnight rollover.
    if prev_armed and prev_day and prev_day != today_utc:
        logger.info("daily_dd: UTC rollover %s → %s; clearing kill_armed", prev_day, today_utc)
        prev_armed = False
        prev_breached_at = 0
        prev_reason = ""
        if _METRICS_OK and C_RESET is not None:
            C_RESET.inc()

    # Query Postgres.
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            r_sum, pct_sum, trades_count = _query_daily_pnl(cur)
        conn.commit()
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.warning("daily_dd: trades_closed not found; fail-open")
        if _METRICS_OK and C_DATA_MISSING is not None:
            C_DATA_MISSING.labels(reason="undefined_table").inc()
        return
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("daily_dd: query failed (fail-open): %s", e)
        if _METRICS_OK and C_DATA_MISSING is not None:
            C_DATA_MISSING.labels(reason="query_error").inc()
        return

    breached_now, reason_now = _check_breach(r_sum, pct_sum)

    # Sticky: если уже armed сегодня — не сбрасываем (даже если PnL восстановится).
    if prev_armed:
        kill_armed = True
        breached_at = prev_breached_at or now_ms
        breached_day = prev_day or today_utc
        reason = prev_reason or reason_now or "sticky"
    elif breached_now:
        kill_armed = True
        breached_at = now_ms
        breached_day = today_utc
        reason = reason_now
        if _METRICS_OK and C_TRIGGERED is not None:
            C_TRIGGERED.labels(mode=MODE, reason=("r" if "r_sum" in reason_now else "pct")).inc()
        logger.critical(
            "🛡️ DAILY_DD_KILL_SWITCH ARMED mode=%s r_sum=%.2f pct_sum=%.2f trades=%d reason=%s",
            MODE, r_sum, pct_sum, trades_count, reason_now,
        )
    else:
        kill_armed = False
        breached_at = 0
        breached_day = ""
        reason = ""

    _write_state(
        r,
        r_sum=r_sum,
        pct_sum=pct_sum,
        trades_count=trades_count,
        kill_armed=kill_armed,
        breached_at_ms=breached_at,
        breached_day_utc=breached_day,
        reason=reason,
        now_ms=now_ms,
    )

    if _METRICS_OK and G_R_SUM is not None and G_PCT_SUM is not None and G_TRADES is not None \
            and G_KILL_ARMED is not None and G_R_THR is not None and G_PCT_THR is not None \
            and G_LAST_CHECK is not None:
        G_R_SUM.set(r_sum)
        G_PCT_SUM.set(pct_sum)
        G_TRADES.set(trades_count)
        G_KILL_ARMED.labels(mode=MODE).set(1 if kill_armed else 0)
        G_R_THR.set(abs(KILL_DAILY_R_LIMIT))
        G_PCT_THR.set(abs(KILL_DAILY_PCT_LIMIT))
        G_LAST_CHECK.set(now_ms)


def _safe_start_http_server(port: int) -> None:
    if not _METRICS_OK:
        return
    try:
        start_http_server(port)  # type: ignore[name-defined]
    except Exception as e:
        logger.warning("daily_dd: prometheus exporter failed to start: %s", e)


def main() -> int:
    if not ENABLED:
        logger.info("daily_dd: DAILY_DD_KILLSWITCH_ENABLED=0 — sleeping idle")
        _safe_start_http_server(PROM_PORT)
        while True:
            time.sleep(3600)

    logger.info(
        "daily_dd: starting mode=%s interval=%ds R_lim=%.2f pct_lim=%.2f port=%d",
        MODE, CHECK_INTERVAL_SEC, KILL_DAILY_R_LIMIT, KILL_DAILY_PCT_LIMIT, PROM_PORT,
    )

    _safe_start_http_server(PROM_PORT)

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)

    # Persistent Postgres connection with reconnect on error.
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(PG_DSN)
                conn.set_session(autocommit=False)
            check_once(r=r, conn=conn)
        except Exception as e:
            logger.exception("daily_dd: loop iteration failed: %s", e)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None
            if _METRICS_OK and C_DATA_MISSING is not None:
                C_DATA_MISSING.labels(reason="connection_error").inc()
        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    sys.exit(main() or 0)
