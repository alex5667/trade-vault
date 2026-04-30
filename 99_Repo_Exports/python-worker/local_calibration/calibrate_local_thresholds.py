#!/usr/bin/env python3
"""
Offline calibration script for local signal thresholds.

This script loads historical signal data from PostgreSQL, calculates local calibration
parameters for each (symbol, session, regime) cluster, and stores the results
in the signal_local_calibration table.

Usage:
    python -m local_calibration.calibrate_local_thresholds

Environment variables:
    PG_DSN: PostgreSQL connection string (default: postgresql://user:pass@localhost:5432/trade)
    CALIB_LOOKBACK_DAYS: Number of days to look back (default: 365)
    CALIB_MIN_TRADES_CLUSTER: Minimum trades per cluster (default: 300)
    CALIB_MIN_TRADES_BUCKET: Minimum trades per bucket (default: 30)
    CALIB_MIN_MEAN_PNL_R: Minimum mean PnL per bucket (default: 0.0)
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extras import DictCursor

# Optional: redis stream source (for environments without PG history)
try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

# --------------------
# Конфиг из ENV
# --------------------

PG_DSN = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/trade")
CALIB_SOURCE = os.getenv("CALIB_SOURCE", "pg")  # 'pg' or 'redis'
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TRADES_CLOSED_STREAM = os.getenv("TRADES_CLOSED_STREAM", "trades:closed")
TRADES_CLOSED_START_ID = os.getenv("TRADES_CLOSED_START_ID", "0-0")

LOOKBACK_DAYS = int(os.getenv("CALIB_LOOKBACK_DAYS", "365"))
MIN_TRADES_CLUSTER = int(os.getenv("CALIB_MIN_TRADES_CLUSTER", "300"))
MIN_TRADES_BUCKET = int(os.getenv("CALIB_MIN_TRADES_BUCKET", "30"))
MIN_MEAN_PNL_R = float(os.getenv("CALIB_MIN_MEAN_PNL_R", "0.0"))

# какие метрики калибруем
METRICS = (
    "delta_spike_z"
    "obi"
    "weak_progress"
    "atr_quantile"
)


# --------------------
# Модели
# --------------------

@dataclass
class SignalRow:
    symbol: str
    session: str
    regime: str

    delta_spike_z: float | None
    obi: float | None
    weak_progress: float | None
    atr_quantile: float | None

    pnl_r: float


ClusterKey = Tuple[str, str, str]  # (symbol, session, regime)


@dataclass
class MetricCalibration:
    q90: float
    q95: float
    q98: float
    chosen_threshold: float
    count_samples: int
    cdf_points: List[Dict[str, float]]


# --------------------
# Утилиты
# --------------------

def quantile(xs: List[float], q: float) -> float:
    if not xs:
        return math.nan
    xs_sorted = sorted(xs)
    k = int(q * (len(xs_sorted) - 1))
    return xs_sorted[k]


def build_empirical_cdf(xs: List[float], num_points: int = 101) -> List[Dict[str, float]]:
    if not xs:
        return []
    xs_sorted = sorted(xs)
    n = len(xs_sorted)
    pts: List[Dict[str, float]] = []
    for i in range(num_points):
        q = i / (num_points - 1)
        k = int(q * (n - 1))
        pts.append({"value": float(xs_sorted[k]), "q": float(q)})
    return pts


def bucket_by_quantiles(xs: List[float], ys: List[float], num_buckets: int = 5):
    """
    xs – метрика (например delta_spike_z)
    ys – результат (pnl_r)
    Возвращает список бакетов [{q_low, q_high, mean_y, count}, ...].
    """
    assert len(xs) == len(ys)
    if not xs:
        return []

    pairs = sorted(zip(xs, ys), key=lambda p: p[0])
    n = len(pairs)
    bucket_size = max(1, n // num_buckets)

    buckets = []
    for i in range(0, n, bucket_size):
        chunk = pairs[i : i + bucket_size]
        if not chunk:
            continue
        xs_chunk = [x for x, _ in chunk]
        ys_chunk = [y for _, y in chunk]
        q_low = xs_chunk[0]
        q_high = xs_chunk[-1]
        mean_y = sum(ys_chunk) / len(ys_chunk)
        buckets.append(
            {
                "q_low": float(q_low)
                "q_high": float(q_high)
                "mean_y": float(mean_y)
                "count": len(xs_chunk)
            }
        )
    return buckets


def choose_threshold_from_buckets(
    buckets
    min_trades: int = MIN_TRADES_BUCKET
    min_mean_pnl: float = MIN_MEAN_PNL_R
) -> float:
    candidates = [
        b
        for b in buckets
        if b["count"] >= min_trades and b["mean_y"] >= min_mean_pnl
    ]
    if not candidates:
        return math.nan
    best = max(candidates, key=lambda b: b["q_low"])
    return float(best["q_low"])


# --------------------
# Загрузка данных из БД
# --------------------

def load_signals(conn) -> List[SignalRow]:
    """
    Load historical trades from trades_closed table.
    Maps trades_closed columns to expected signal metrics.
    """
    # Use stored generated columns (ind_*) added by migration 034 to avoid
    # JSONB extraction at scan time. The WHERE clause matches the partial index
    # idx_trades_closed_ml_v2, enabling an Index-Only Scan on Timescale chunks.
    sql = f"""
        SELECT
            symbol
            COALESCE(entry_tag, 'mixed') AS session
            'mixed'             AS regime
            ind_delta_z         AS delta_spike_z
            ind_obi             AS obi
            ind_weak_progress   AS weak_progress
            ind_atr_th_bps      AS atr_quantile
            r_multiple          AS pnl_r
        FROM trades_closed
        WHERE exit_ts >= NOW() - INTERVAL '{LOOKBACK_DAYS} days'
          AND r_multiple IS NOT NULL
          AND (tp1_hit = TRUE OR r_multiple > 0)
    """
    rows: List[SignalRow] = []
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql)
        for r in cur:
            rows.append(
                SignalRow(
                    symbol=r["symbol"]
                    session=r["session"]
                    regime=r["regime"]
                    delta_spike_z=r["delta_spike_z"]
                    obi=r["obi"]
                    weak_progress=r["weak_progress"]
                    atr_quantile=r["atr_quantile"]
                    pnl_r=r["pnl_r"]
                )
            )
    return rows


def _to_str(x, default: str = "") -> str:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", "ignore")
        return str(x)
    except Exception:
        return default


def _to_float(x, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def load_signals_from_redis() -> List[SignalRow]:
    """
    Read POSITION_CLOSED events from Redis stream (trades:closed) as an alternative to Postgres.
    """
    if redis is None:
        raise RuntimeError("redis-py is not available")
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    rows: List[SignalRow] = []
    last = TRADES_CLOSED_START_ID
    for _ in range(100000):
        out = r.xread({TRADES_CLOSED_STREAM: last}, count=1000, block=0) or []
        if not out:
            break
        _sname, entries = out[0]
        if not entries:
            break
        for msg_id, fields in entries:
            last = _to_str(msg_id, last)
            d = dict(fields or {})
            et = _to_str(d.get("event_type") or d.get("event") or "").upper()
            if et and et != "POSITION_CLOSED":
                continue
            symbol = _to_str(d.get("symbol"))
            if not symbol:
                continue
            session = _to_str(d.get("entry_tag"), "mixed")
            regime = _to_str(d.get("regime"), "mixed") or "mixed"
            pnl_r = _to_float(d.get("r_mult"), _to_float(d.get("r_multiple"), 0.0))
            if pnl_r == 0.0:
                pnl = _to_float(d.get("pnl"), 0.0)
                risk = _to_float(d.get("risk_usd"), 0.0)
                if risk > 0:
                    pnl_r = pnl / risk
            rows.append(
                SignalRow(
                    symbol=symbol
                    session=session
                    regime=regime
                    delta_spike_z=None
                    obi=None
                    weak_progress=None
                    atr_quantile=None
                    pnl_r=float(pnl_r)
                )
            )
    return rows


def build_clusters(rows: List[SignalRow]) -> Dict[ClusterKey, List[SignalRow]]:
    clusters: Dict[ClusterKey, List[SignalRow]] = {}
    for r in rows:
        key = (r.symbol, r.session, r.regime)
        clusters.setdefault(key, []).append(r)
    return clusters


# --------------------
# Калибровка по одному кластеру
# --------------------

def calibrate_metric_for_cluster(
    metric_name: str
    rows: List[SignalRow]
) -> MetricCalibration | None:
    # забираем метрику и pnl
    metric_values: List[float] = []
    pnl_list: List[float] = []

    for r in rows:
        v = getattr(r, metric_name, None)
        if v is None:
            continue
        metric_values.append(float(v))
        pnl_list.append(float(r.pnl_r))

    if len(metric_values) < MIN_TRADES_CLUSTER:
        return None

    q90 = quantile(metric_values, 0.90)
    q95 = quantile(metric_values, 0.95)
    q98 = quantile(metric_values, 0.98)

    buckets = bucket_by_quantiles(metric_values, pnl_list, num_buckets=5)
    chosen_thr = choose_threshold_from_buckets(buckets)

    cdf_points = build_empirical_cdf(metric_values, num_points=101)

    return MetricCalibration(
        q90=q90
        q95=q95
        q98=q98
        chosen_threshold=chosen_thr
        count_samples=len(metric_values)
        cdf_points=cdf_points
    )


# --------------------
# Запись в БД (UPSERT)
# --------------------

def upsert_calibration(
    conn
    symbol: str
    session: str
    regime: str
    metric: str
    calib: MetricCalibration
) -> None:
    sql = """
        INSERT INTO signal_local_calibration (
            symbol, session, regime, metric
            q90, q95, q98
            chosen_threshold
            count_samples
            cdf_points
            updated_at
        )
        VALUES (%s, %s, %s, %s
                %s, %s, %s
                %s
                %s
                %s
                NOW())
        ON CONFLICT (symbol, session, regime, metric)
        DO UPDATE SET
            q90             = EXCLUDED.q90
            q95             = EXCLUDED.q95
            q98             = EXCLUDED.q98
            chosen_threshold = EXCLUDED.chosen_threshold
            count_samples   = EXCLUDED.count_samples
            cdf_points      = EXCLUDED.cdf_points
            updated_at      = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql
            (
                symbol
                session
                regime
                metric
                calib.q90
                calib.q95
                calib.q98
                calib.chosen_threshold
                calib.count_samples
                json.dumps(calib.cdf_points)
            )
        )


# --------------------
# main
# --------------------

def main() -> None:
    print(f"Starting local calibration with PG_DSN: {PG_DSN}")
    print(f"Lookback days: {LOOKBACK_DAYS}, Min cluster trades: {MIN_TRADES_CLUSTER}")

    if CALIB_SOURCE == "redis":
        rows = load_signals_from_redis()
        print(f"Loaded {len(rows)} signals from Redis stream: {TRADES_CLOSED_STREAM}")
        conn = None
    else:
        conn = psycopg2.connect(PG_DSN)
        try:
            rows = load_signals(conn)
            print(f"Loaded {len(rows)} signals")
        except Exception:
            conn.close()
            raise

    clusters = build_clusters(rows)
    print(f"Found {len(clusters)} clusters")

    try:
        calibrated_clusters = 0
        total_calibrations = 0
        print_counter = 0

        for (symbol, session, regime), cluster_rows in clusters.items():
            cluster_calibrated = False
            for metric in METRICS:
                calib = calibrate_metric_for_cluster(metric, cluster_rows)
                if calib is None:
                    continue

                if conn:
                    upsert_calibration(conn, symbol, session, regime, metric, calib)
                total_calibrations += 1
                cluster_calibrated = True
                
                print_counter += 1
                if print_counter % 1000 == 1 or print_counter == 1:
                    print(f"  Calibrated {symbol} {session} {regime} {metric}: {calib.count_samples} samples")

            if cluster_calibrated:
                calibrated_clusters += 1

        if conn:
            conn.commit()
        print(f"Calibration completed: {calibrated_clusters} clusters, {total_calibrations} metrics")

    except Exception as e:
        print(f"Error during calibration: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
