#!/usr/bin/env python3
"""
ETL V3: trades_closed.ind_* → signal_facts + trade_performance
=============================================================
Заполняет ML-таблицы из Legacy-источника trades_closed.

Ключевое открытие: индикаторные фичи (delta_z, obi, weak_progress, atr_th)
хранятся в trades_closed.ind_* колонках, а НЕ в signals.raw_ctx (там всё нулевое).

Запуск:
  # На хосте (порт 5434):
  PG_DSN=postgresql://trading:trading_password@localhost:5434/scanner_analytics \\
    python scripts/build_ml_dataset_from_scratch.py

  # В контейнере:
  docker exec scanner-python-worker python /app/scripts/build_ml_dataset_from_scratch.py
"""

import os
import sys
import json
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ETL-V3")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    log.error("psycopg2 not installed")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Toxic-day thresholds (ENV-tunable)
# ---------------------------------------------------------------------------
# A day is "toxic" if it has MANY trades but almost no winners:
#   - N ≥ TOXIC_MIN_N     (spam / regime-collapse flood)
#   - hit_rate_03 < TOXIC_MAX_HR  (P(R≥0.3) near-zero)
# These days poison the training set and invert model predictions.
TOXIC_MIN_N = int(os.getenv("ML_TOXIC_DAY_MIN_N", "2000"))
TOXIC_MAX_HR = float(os.getenv("ML_TOXIC_DAY_MAX_HR", "0.01"))  # < 1%
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")


def _get_redis():
    try:
        import redis as _redis
        url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = _redis.from_url(url, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        log.warning("Cannot connect to Redis for Telegram notify: %s", e)
        return None


def _notify_telegram(r, text: str) -> None:
    if r is None:
        return
    try:
        r.xadd(
            NOTIFY_STREAM,
            {"type": "report", "text": text, "parse_mode": "HTML", "source": "etl_v3"},
            maxlen=50_000,
        )
        log.info("Telegram notify sent to %s", NOTIFY_STREAM)
    except Exception as e:
        log.warning("Failed to send Telegram notify: %s", e)


def get_dsn():
    for var in ("ANALYTICS_DB_DSN", "PG_DSN", "DATABASE_URL"):
        dsn = os.getenv(var)
        if dsn and "scanner_analytics" in dsn:
            return dsn
    return f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@localhost:5434/scanner_analytics"


def run_etl():
    dsn = get_dsn()
    log.info("Connecting to %s", dsn.split("@")[-1])
    conn = psycopg2.connect(dsn, connect_timeout=10)
    cur = conn.cursor()

    # Step 1: Clean slate
    log.info("STEP 1: Truncate")
    cur.execute("TRUNCATE signal_facts")
    cur.execute("DELETE FROM trade_performance")
    conn.commit()

    # Step 2a: trade_performance
    log.info("STEP 2a: Backfill trade_performance")
    cur.execute("""
        INSERT INTO trade_performance
            (signal_id, ts_open, ts_close, symbol, direction, r, hit, holding_ms)
        SELECT DISTINCT ON (sid)
            sid,
            to_timestamp(entry_ts_ms / 1000.0) AT TIME ZONE 'UTC',
            to_timestamp(exit_ts_ms  / 1000.0) AT TIME ZONE 'UTC',
            symbol,
            CASE WHEN direction = 'LONG' THEN 1 ELSE -1 END,
            r_multiple,
            pnl_net > 0,
            exit_ts_ms - entry_ts_ms
        FROM trades_closed
        WHERE sid IS NOT NULL AND r_multiple IS NOT NULL
        ORDER BY sid, exit_ts_ms DESC
        ON CONFLICT (signal_id) DO UPDATE SET r = EXCLUDED.r, hit = EXCLUDED.hit
    """)
    tp_n = cur.rowcount
    log.info("  → %d rows", tp_n)

    # Step 2b: signal_facts from trades_closed.ind_*
    #   ind_delta_z       → delta_spike_z (double precision)
    #   ind_obi           → obi_avg_20    (double precision, may be 0.0)
    #   ind_weak_progress → weak_progress_ratio (boolean → float)
    #   ind_atr_th_bps    → atr_14        (double precision)
    log.info("STEP 2b: Backfill signal_facts (from trades_closed.ind_*)")
    cur.execute("""
        INSERT INTO signal_facts
            (ts, signal_id, symbol, direction, signal_family,
             conf_score, atr_14, delta_spike_z, obi_avg_20, weak_progress_ratio)
        SELECT DISTINCT ON (sid)
            to_timestamp(entry_ts_ms / 1000.0) AT TIME ZONE 'UTC',
            sid, symbol,
            CASE WHEN direction = 'LONG' THEN 1 ELSE -1 END,
            COALESCE(source, 'crypto_orderflow'),
            0.0,
            COALESCE(ind_atr_th_bps, 0.0),
            COALESCE(ind_delta_z, 0.0),
            COALESCE(ind_obi, 0.0),
            CASE WHEN ind_weak_progress THEN 1.0 ELSE 0.0 END
        FROM trades_closed
        WHERE sid IS NOT NULL AND r_multiple IS NOT NULL
        ORDER BY sid, entry_ts_ms DESC
        ON CONFLICT (ts, signal_id) DO NOTHING
    """)
    sf_n = cur.rowcount
    log.info("  → %d rows", sf_n)
    conn.commit()

    # Step 3: Diagnostics
    log.info("STEP 3: Diagnostics")
    cur.execute("""
        SELECT count(*),
               round(avg(r)::numeric, 4),
               round(avg(CASE WHEN hit THEN 1.0 ELSE 0.0 END)::numeric, 4)
        FROM trade_performance
    """)
    n, avg_r, hr = cur.fetchone()
    log.info("  trade_performance: %d, avg_R=%s, hit_rate=%s", n, avg_r, hr)

    cur.execute("""
        SELECT count(*),
               count(DISTINCT symbol),
               round(avg(delta_spike_z)::numeric, 4),
               round(stddev(delta_spike_z)::numeric, 4),
               round(avg(obi_avg_20)::numeric, 4),
               round(avg(weak_progress_ratio)::numeric, 4)
        FROM signal_facts
    """)
    n, sym, dz, sdz, obi, wp = cur.fetchone()
    log.info("  signal_facts: %d, %d symbols, dz=%s±%s, obi=%s, wp=%s", n, sym, dz, sdz, obi, wp)

    cur.execute("""
        SELECT count(*)
        FROM signal_facts s JOIN trade_performance t ON t.signal_id = s.signal_id
    """)
    log.info("  JOIN quality: %d matched rows", cur.fetchone()[0])

    # Per-symbol
    cur.execute("""
        SELECT s.symbol, count(*), round(avg(t.r)::numeric,4),
               round(avg(CASE WHEN t.hit THEN 1.0 ELSE 0.0 END)::numeric,4),
               round(avg(s.delta_spike_z)::numeric,4)
        FROM signal_facts s
        JOIN trade_performance t ON t.signal_id = s.signal_id
        GROUP BY s.symbol ORDER BY count(*) DESC LIMIT 10
    """)
    for sym, n, ar, hrate, dz in cur.fetchall():
        log.info("    %-14s N=%-5d R=%-8s HR=%-6s dz=%s", sym, n, ar, hrate, dz)

    # -----------------------------------------------------------------------
    # Step 3.5: Toxic-day detection & removal
    # A "toxic day" = N >= TOXIC_MIN_N AND hit_rate(R>=0.3) < TOXIC_MAX_HR
    # These days flood training data with near-zero positive examples after
    # a regime-collapse event (e.g. 2026-04-09: 12819 rows, hit_rate=0.3%).
    # -----------------------------------------------------------------------
    log.info(
        "STEP 3.5: Toxic-day scan (min_n=%d, max_hr=%.1f%%)",
        TOXIC_MIN_N, TOXIC_MAX_HR * 100,
    )
    cur.execute("""
        SELECT
            date_trunc('day', s.ts)::date     AS day,
            COUNT(*)                           AS n,
            ROUND(AVG(CASE WHEN t.r >= 0.3 THEN 1.0 ELSE 0.0 END)::numeric, 4)  AS hit_rate_03,
            ROUND(AVG(t.r)::numeric, 4)        AS avg_r
        FROM signal_facts s
        JOIN trade_performance t ON s.signal_id = t.signal_id
        GROUP BY 1
        HAVING COUNT(*) >= %(min_n)s
           AND AVG(CASE WHEN t.r >= 0.3 THEN 1.0 ELSE 0.0 END) < %(max_hr)s
        ORDER BY n DESC
    """, {"min_n": TOXIC_MIN_N, "max_hr": TOXIC_MAX_HR})
    toxic_days = cur.fetchall()  # [(day, n, hit_rate_03, avg_r), ...]

    if toxic_days:
        r_notify = _get_redis()
        total_removed = 0
        lines = []
        for day, n_day, hr03, ar in toxic_days:
            day_str = str(day)
            log.warning(
                "  ⚠️  TOXIC DAY detected: %s  N=%d  hit_rate_03=%.2f%%  avg_R=%s — REMOVING",
                day_str, n_day, float(hr03) * 100, ar,
            )
            # Remove signal_facts for this day
            cur.execute("""
                DELETE FROM signal_facts
                WHERE date_trunc('day', ts)::date = %(day)s
            """, {"day": day_str})
            sf_del = cur.rowcount
            # Remove orphaned trade_performance rows (no longer linked to signal_facts)
            cur.execute("""
                DELETE FROM trade_performance tp
                WHERE NOT EXISTS (
                    SELECT 1 FROM signal_facts sf WHERE sf.signal_id = tp.signal_id
                )
                  AND date_trunc('day', tp.ts_open)::date = %(day)s
            """, {"day": day_str})
            tp_del = cur.rowcount
            total_removed += sf_del
            lines.append(
                f"  • <code>{day_str}</code>  N={n_day}  HR={float(hr03)*100:.1f}%"
                f"  avgR={ar}  → удалено SF={sf_del} TP={tp_del}"
            )
        conn.commit()

        log.warning("  Removed %d toxic rows total from %d days", total_removed, len(toxic_days))

        # Telegram alert
        tg_text = (
            f"⚠️ <b>ETL V3 — Обнаружены и удалены токсичные дни</b>\n\n"
            f"Дни с N≥{TOXIC_MIN_N} и hit_rate&lt;{TOXIC_MAX_HR*100:.0f}% "
            f"(<i>режим коллапса / всплеск ложных сигналов</i>) исключены из датасета:\n\n"
            + "\n".join(lines)
            + f"\n\n<b>Итого удалено:</b> <code>{total_removed}</code> строк из "
            f"<code>{len(toxic_days)}</code> дн.\n"
            f"Тренировка ML продолжится на очищенных данных.\n\n"
            f"<i>Порог: ML_TOXIC_DAY_MIN_N={TOXIC_MIN_N}, ML_TOXIC_DAY_MAX_HR={TOXIC_MAX_HR*100:.0f}%</i>"
        )
        _notify_telegram(r_notify, tg_text)
    else:
        log.info(
            "  ✅ No toxic days found (all days pass N≥%d + HR≥%.1f%% check)",
            TOXIC_MIN_N, TOXIC_MAX_HR * 100,
        )

    # Step 4: Logistic Regression (inline)
    log.info("STEP 4: Logistic Regression Calibration")
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        cur.execute("""
            SELECT s.delta_spike_z, s.obi_avg_20, s.weak_progress_ratio, s.atr_14, t.hit
            FROM signal_facts s
            JOIN trade_performance t ON t.signal_id = s.signal_id
        """)
        rows = cur.fetchall()
        feats = ["delta_spike_z", "obi_avg_20", "weak_progress_ratio", "atr_14"]
        X = np.nan_to_num(np.array([[r[0], r[1], r[2], r[3]] for r in rows], dtype=np.float64))
        y = np.array([int(r[4]) for r in rows], dtype=np.int32)

        if len(np.unique(y)) < 2:
            log.warning("Single class — skip calibration")
        else:
            scaler = StandardScaler()
            X_s = scaler.fit_transform(X)
            mdl = LogisticRegression(max_iter=1000)
            mdl.fit(X_s, y)
            w = mdl.coef_[0]
            nw = w / (np.abs(w).sum() or 1.0)
            log.info("  Accuracy: %.4f", mdl.score(X_s, y))
            for f, c, wn in zip(feats, w, nw):
                log.info("    %-22s coef=%+.4f weight=%+.4f", f, c, wn)

            result = {
                "phase": 2, "version": "1.0.0", "sample_size": len(rows),
                "accuracy": round(float(mdl.score(X_s, y)), 4),
                "suggested_weights": {f"w_{f}": round(float(wn), 4) for f, wn in zip(feats, nw)},
            }
            out = os.getenv("WEIGHTS_OUTPUT", "/var/lib/trade/suggested_weights.json")
            os.makedirs(os.path.dirname(out), exist_ok=True)
            with open(out, "w") as fp:
                json.dump(result, fp, indent=2)
            log.info("  Saved to %s", out)

    except ImportError:
        log.warning("sklearn not available — skipping calibration")

    conn.close()
    log.info("✅ ETL V3 complete.")


if __name__ == "__main__":
    run_etl()
