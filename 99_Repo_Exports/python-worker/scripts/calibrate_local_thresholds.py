#!/usr/bin/env python3
"""
Скрипт для калибровки локальных порогов на основе исторических данных.

Запуск:
    python scripts/calibrate_local_thresholds.py --lookback-days 365 --output /app/data/local_calibration.json

Или через cron для регулярного обновления:
    0 2 * * * /usr/local/bin/python /app/scripts/calibrate_local_thresholds.py --lookback-days 365 --output /app/data/local_calibration.json
"""

import argparse
import logging
import sys
import os
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.local_calibration import LocalCalibrationManager, SignalRow

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

import psycopg2
import psycopg2.extras

def load_signals_from_database(db_url: str, lookback_days: int) -> list[SignalRow]:
    """
    Загружает сигналы из базы данных.
    """
    if not db_url:
        # Fallback to env var if not passed
        db_url = os.getenv("TRADES_DB_DSN", f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@postgres:5432/scanner_analytics")

    logger.info(f"Loading signals from database (last {lookback_days} days)...")
    
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        # Use RealDictCursor to access columns by name
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Use stored generated columns (ind_*) added by migration 034 to avoid
        # JSONB extraction at scan time. The WHERE clause matches the partial index
        # idx_trades_closed_ml_v2, enabling an Index-Only Scan on Timescale chunks.
        query = """
        SELECT
            symbol,
            exit_ts_ms / 1000.0             AS ts_utc,
            COALESCE(entry_tag, 'mixed')    AS pattern_label,
            'mixed'                         AS regime,
            ind_delta_z                     AS delta_spike_z,
            ind_obi                         AS obi,
            CASE WHEN ind_weak_progress THEN 1.0 ELSE 0.0 END AS weak_progress,
            ind_atr_th_bps                  AS atr_quantile,
            r_multiple                      AS pnl_r,
            tp1_hit                         AS hit_tp
        FROM trades_closed
        WHERE exit_ts_ms >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '%s days')) * 1000
          AND r_multiple IS NOT NULL
          AND (tp1_hit = TRUE OR r_multiple > 0)
        ORDER BY exit_ts_ms
        """ % lookback_days


        cursor.execute(query)
        rows = cursor.fetchall()
        
        signal_rows = []
        for r in rows:
            # Map DB row to SignalRow
            # Note: SignalRow expects 'session' but DB might not have it computed.
            # load_from_database in manager will compute session if missing.
            
            # Safe float conversion helpers
            def to_float(x):
                return float(x) if x is not None else None
            
            sr = SignalRow(
                symbol=r["symbol"],
                session="", # Will be computed by manager
                regime=r.get("regime") or "",
                ts_utc=float(r["ts_utc"]),
                delta_spike_z=to_float(r.get("delta_spike_z")),
                obi=to_float(r.get("obi")),
                weak_progress=to_float(r.get("weak_progress")),
                atr_quantile=to_float(r.get("atr_quantile")),
                pnl_r=to_float(r.get("pnl_r")),
                hit_tp=bool(r.get("hit_tp")) if r.get("hit_tp") is not None else None
            )
            signal_rows.append(sr)
            
        logger.info(f"Successfully loaded {len(signal_rows)} signals from DB.")
        return signal_rows

    except Exception as e:
        logger.error(f"Error loading from database: {e}")
        return []
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Calibrate local thresholds for signal filtering')
    parser.add_argument('--lookback-days', type=int, default=365,
                       help='Number of days to look back for historical data')
    parser.add_argument('--output', type=str, required=True,
                       help='Output JSON file for calibration data')
    parser.add_argument('--db-url', type=str,
                       help='Database URL (if not set, uses environment)')
    parser.add_argument('--min-cluster-samples', type=int, default=300,
                       help='Minimum samples required per cluster')

    args = parser.parse_args()

    # Создаем менеджер калибровки
    manager = LocalCalibrationManager()
    manager.min_cluster_samples = args.min_cluster_samples

    # Загружаем данные (в реальной реализации)
    signals = load_signals_from_database(args.db_url, args.lookback_days)

    if not signals:
        logger.warning("No signals loaded - check database connection and schema")
        return

    logger.info(f"Loaded {len(signals)} signals")

    # Имитируем загрузку данных (в реальной реализации замените на реальную загрузку)
    # manager.load_from_database(db_connection, args.lookback_days)

    # Сгружаем сигналы в кластеры
    # Используем внутренний метод менеджера для группировки
    clusters = manager._build_clusters(signals)
    
    logger.info(f"Built {len(clusters)} clusters from {len(signals)} signals")
    
    count_calibrated = 0
    for cluster_key, cluster_rows in clusters.items():
        # Пропускаем кластеры, где мало данных
        if len(cluster_rows) < manager.min_cluster_samples:
            continue
            
        # Калибруем кластер
        calibration = manager._calibrate_cluster(cluster_rows)
        manager.calibrations[cluster_key] = calibration
        count_calibrated += 1
        
    logger.info(f"Calibrated {count_calibrated} clusters out of {len(clusters)} candidates")

    # Сохраняем в файл
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manager.save_to_json(str(output_path))

    logger.info(f"Calibration completed. Saved to {args.output}")
    logger.info(f"Calibrated {len(manager.calibrations)} clusters")

    # Выводим статистику
    for key, calibration in manager.calibrations.items():
        symbol, session, regime = key
        logger.info(f"  {symbol} {session} {regime}: {calibration.sample_count} samples")

if __name__ == "__main__":
    main()
