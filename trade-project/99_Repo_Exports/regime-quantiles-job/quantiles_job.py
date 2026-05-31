"""
quantiles_job.py — периодический пересчёт квантилей ADX и ATR% для каждой пары (symbol,timeframe)

- берём список уникальных (symbol,timeframe) из regime_snapshot
- считаем percentiles за LOOKBACK_DAYS по полям:
    adx  -> p40, p60, p75
    atrPct ("atrPct" в БД — camelCase, поэтому дальше используем кавычки) -> p25, p50, p75
- сохраняем в regime_quantiles (UPSERT)
- повторяем каждые INTERVAL_SEC

Важно:
- Применяем MIN_SAMPLES: если наблюдений мало, пропускаем пару, чтобы не засорять статистику.
- Все параметры в .env
"""

# --- stdlib ---
import json
import logging
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, List, Optional, Tuple

# --- third-party ---
import psycopg2
import redis
from prometheus_client import Gauge, start_http_server

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
REDIS_URL: Optional[str] = os.getenv("REDIS_URL")
INTERVAL_SEC: int = int(os.getenv("REGIME_QUANTILES_INTERVAL_SEC", "900"))
LOOKBACK_DAYS: int = int(os.getenv("REGIME_QUANTILES_LOOKBACK_DAYS", "14"))
MIN_SAMPLES: int = int(os.getenv("REGIME_QUANTILES_MIN_SAMPLES", "500"))
ATR_PCT_MIN: float = float(os.getenv("REGIME_ATR_PCT_MIN", "0.000001"))
ATR_PCT_MAX: float = float(os.getenv("REGIME_ATR_PCT_MAX", "0.20"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("quantiles_job")

# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------
quantiles_fresh_seconds = Gauge(
    "regime_quantiles_fresh_seconds",
    "Time since last computation for this timeframe",
    ["timeframe"],
)
quantiles_sample_count = Gauge(
    "regime_quantiles_sample_count",
    "Samples used for quantiles calculation",
    ["symbol", "timeframe"],
)

# ---------------------------------------------------------------------------
# Redis singleton (lazy init, one connection per process)
# ---------------------------------------------------------------------------
_redis_client: Optional[redis.Redis] = None  # type: ignore[type-arg]


def _get_redis() -> Optional[redis.Redis]:  # type: ignore[type-arg]
    """Return a shared Redis client instance (lazy init).

    Returns None if REDIS_URL is not configured.
    The client is configured with health-check and retry settings to
    survive transient disconnects (e.g. ConnectionResetError).
    """
    global _redis_client
    if not REDIS_URL:
        return None
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            health_check_interval=30,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_error=[ConnectionError, TimeoutError, redis.exceptions.ConnectionError],
        )
    return _redis_client


def _reset_redis() -> None:
    """Discard the current Redis client so the next call recreates it."""
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.close()
        except Exception:
            pass
    _redis_client = None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
@contextmanager
def get_conn() -> Generator:
    """Yield a psycopg2 connection and close it after use. Includes retries for transient errors."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    
    conn = None
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            break
        except psycopg2.OperationalError as e:
            if attempt == max_attempts:
                raise
            sleep_time = 2 ** attempt
            if attempt >= max_attempts - 1:
                log.warning("DB connection failed (attempt %d/%d), retrying in %ds: %s", attempt, max_attempts, sleep_time, e)
            else:
                log.debug("DB connection failed (attempt %d/%d), retrying in %ds: %s", attempt, max_attempts, sleep_time, e)
            time.sleep(sleep_time)

    try:
        yield conn
    finally:
        if conn:
            conn.close()


def compute_all_quantiles(conn) -> List[Tuple[str, str, dict]]:
    """Compute quantiles for ALL (symbol, timeframe) pairs in a single query.

    Applies MIN_SAMPLES filter and ATR% range guards via SQL HAVING / WHERE.
    Uses fully parameterised query — no f-string interpolation of user values.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              symbol,
              timeframe,
              COUNT(*) AS sample_count,
              MIN(ts)  AS src_min,
              MAX(ts)  AS src_max,
              percentile_cont(0.40) WITHIN GROUP (ORDER BY adx)      AS adx_p40,
              percentile_cont(0.60) WITHIN GROUP (ORDER BY adx)      AS adx_p60,
              percentile_cont(0.75) WITHIN GROUP (ORDER BY adx)      AS adx_p75,
              percentile_cont(0.25) WITHIN GROUP (ORDER BY "atrPct") AS atrp_p25,
              percentile_cont(0.50) WITHIN GROUP (ORDER BY "atrPct") AS atrp_p50,
              percentile_cont(0.75) WITHIN GROUP (ORDER BY "atrPct") AS atrp_p75,
              percentile_cont(0.90) WITHIN GROUP (ORDER BY "atrPct") AS atrp_p90
            FROM regime_snapshot
            WHERE ts >= (now() - interval '1 day' * %s)
              AND adx IS NOT NULL
              AND "atrPct" IS NOT NULL
              AND "atrPct" >= %s
              AND "atrPct" <= %s
            GROUP BY symbol, timeframe
            HAVING COUNT(*) >= %s
            ORDER BY symbol, timeframe
            """,
            (LOOKBACK_DAYS, ATR_PCT_MIN, ATR_PCT_MAX, MIN_SAMPLES),
        )

        results: List[Tuple[str, str, dict]] = []
        for row in cur.fetchall():
            (
                symbol, tf, sample_count, src_min, src_max,
                adx_p40, adx_p60, adx_p75,
                atrp_p25, atrp_p50, atrp_p75, atrp_p90,
            ) = row
            results.append(
                (
                    symbol,
                    tf,
                    {
                        "sample_count": int(sample_count),
                        "src_time_min": src_min,
                        "src_time_max": src_max,
                        "adx_p40":  float(adx_p40)  if adx_p40  is not None else 0.0,
                        "adx_p60":  float(adx_p60)  if adx_p60  is not None else 0.0,
                        "adx_p75":  float(adx_p75)  if adx_p75  is not None else 0.0,
                        "atrp_p25": float(atrp_p25) if atrp_p25 is not None else 0.0,
                        "atrp_p50": float(atrp_p50) if atrp_p50 is not None else 0.0,
                        "atrp_p75": float(atrp_p75) if atrp_p75 is not None else 0.0,
                        "atrp_p90": float(atrp_p90) if atrp_p90 is not None else 0.0,
                    },
                )
            )
        return results


# ---------------------------------------------------------------------------
# Redis cache
# ---------------------------------------------------------------------------
def cache_quantiles(symbol: str, timeframe: str, q: dict) -> None:
    """Write ATR% quantiles to Redis Hash atrpct:quantiles:{timeframe}.

    Field: {symbol} -> JSON payload.
    Uses the process-wide Redis singleton — no per-call connection overhead.
    On transient connection errors, resets the singleton and retries once.
    """
    r = _get_redis()
    if r is None:
        return

    key = f"atrpct:quantiles:{timeframe}"
    payload = {
        "p25": q["atrp_p25"],
        "p50": q["atrp_p50"],
        "p75": q["atrp_p75"],
        "p90": q["atrp_p90"],
        "sample_count": q["sample_count"],
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "window_days": LOOKBACK_DAYS,
    }

    for attempt in range(2):
        try:
            r.hset(key, symbol, json.dumps(payload))
            r.expire(key, 21600)  # 6 hours — always renew on update
            return  # success
        except (ConnectionError, TimeoutError, redis.exceptions.ConnectionError) as exc:
            log.warning(
                "Redis transient error for %s/%s (attempt %d/2): %s",
                symbol, timeframe, attempt + 1, exc,
            )
            _reset_redis()
            r = _get_redis()
            if r is None:
                return
        except Exception:
            log.warning(
                "Redis cache error for %s/%s:\n%s",
                symbol, timeframe, traceback.format_exc(),
            )
            return


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------
def upsert_quantiles(conn, symbol: str, timeframe: str, q: dict) -> None:
    """UPSERT one row into regime_quantiles, then write to Redis cache."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO regime_quantiles
                (id, symbol, timeframe,
                 adx_p40, adx_p60, adx_p75,
                 atrp_p25, atrp_p50, atrp_p75, atrp_p90,
                 sample_count, window_days, computed_at, src_time_min, src_time_max)
            VALUES
                (gen_random_uuid(), %s, %s,
                 %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, now(), %s, %s)
            ON CONFLICT (symbol, timeframe)
            DO UPDATE SET
                adx_p40      = EXCLUDED.adx_p40,
                adx_p60      = EXCLUDED.adx_p60,
                adx_p75      = EXCLUDED.adx_p75,
                atrp_p25     = EXCLUDED.atrp_p25,
                atrp_p50     = EXCLUDED.atrp_p50,
                atrp_p75     = EXCLUDED.atrp_p75,
                atrp_p90     = EXCLUDED.atrp_p90,
                sample_count = EXCLUDED.sample_count,
                window_days  = EXCLUDED.window_days,
                computed_at  = now(),
                src_time_min = EXCLUDED.src_time_min,
                src_time_max = EXCLUDED.src_time_max
            """,
            (
                symbol, timeframe,
                q["adx_p40"], q["adx_p60"], q["adx_p75"],
                q["atrp_p25"], q["atrp_p50"], q["atrp_p75"], q["atrp_p90"],
                q["sample_count"],
                LOOKBACK_DAYS,
                q["src_time_min"], q["src_time_max"],
            ),
        )
    conn.commit()

    # Write to Redis cache after successful DB commit
    cache_quantiles(symbol, timeframe, q)


# ---------------------------------------------------------------------------
# Job main cycle
# ---------------------------------------------------------------------------

# Track last update timestamps for freshness metric
last_update_ts: dict = {}

# Running total of processed pairs (for log sampling)
_log_counter: int = 0


def tick() -> None:
    """One full computation cycle.

    - Fetch all eligible (symbol, timeframe) pairs and compute quantiles.
    - Upsert each into regime_quantiles and update Redis cache.
    - Update Prometheus metrics.
    """
    global _log_counter

    with get_conn() as conn:
        all_quantiles = compute_all_quantiles(conn)
        if not all_quantiles:
            log.warning("No eligible (symbol, timeframe) pairs found — waiting…")
            return

        processed_count = 0
        for symbol, tf, q in all_quantiles:
            upsert_quantiles(conn, symbol, tf, q)

            # Prometheus: sample count per pair
            quantiles_sample_count.labels(symbol=symbol, timeframe=tf).set(q["sample_count"])
            # Record time of last successful update
            last_update_ts[tf] = time.time()

            processed_count += 1

        _log_counter += processed_count

        # Log a detailed summary every 10 000 pairs processed (cumulative)
        # or on the very first completed run (_log_counter == processed_count)
        if _log_counter == processed_count or (_log_counter % 10_000) < processed_count:
            sample_sym, sample_tf, sample_q = all_quantiles[-1]
            log.info(
                "processed %d pairs (total: %d) | sample %s/%s "
                "ADX(p40=%.2f, p60=%.2f, p75=%.2f) "
                "ATR%%(p25=%.6f, p50=%.6f, p75=%.6f, p90=%.6f) n=%d",
                processed_count, _log_counter,
                sample_sym, sample_tf,
                sample_q["adx_p40"], sample_q["adx_p60"], sample_q["adx_p75"],
                sample_q["atrp_p25"], sample_q["atrp_p50"], sample_q["atrp_p75"], sample_q["atrp_p90"],
                sample_q["sample_count"],
            )
        else:
            log.info("processed %d pairs (total so far: %d)", processed_count, _log_counter)


def main() -> None:
    # Fail fast: validate critical config before entering the loop
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Set it via environment variable before starting."
        )

    metrics_port = int(os.getenv("METRICS_PORT", "8001"))
    start_http_server(metrics_port)
    log.info(
        "start: interval=%ds, lookback=%dd, min_samples=%d, metrics_port=%d",
        INTERVAL_SEC, LOOKBACK_DAYS, MIN_SAMPLES, metrics_port,
    )

    while True:
        try:
            tick()

            # Update freshness gauges for all known timeframes
            now = time.time()
            for tf, last_ts in last_update_ts.items():
                quantiles_fresh_seconds.labels(timeframe=tf).set(now - last_ts)

        except Exception:
            # Log full traceback but keep the job running
            log.error("Unhandled error in tick():\n%s", traceback.format_exc())

        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
