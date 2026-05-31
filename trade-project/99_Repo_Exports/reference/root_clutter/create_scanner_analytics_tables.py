#!/usr/bin/env python3
"""
Script to apply all scanner_analytics database migrations
"""

import psycopg2
import os

# Database configuration (from docker-compose.yml)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'user': 'postgres',
    'password': '12345',
    'database': 'scanner_analytics'
}

# Migration 1: Create trades_closed and related tables
TRADES_MIGRATION_SQL = """
-- Migration: Create scanner_analytics tables
-- Description: Creates tables for trade analytics in scanner_analytics database
-- Date: 2025-12-17

-- Enable TimescaleDB extension if available (optional)
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- Table: trades_closed
-- ============================================================================
-- Main table for closed trades with baseline vs managed comparison

CREATE TABLE IF NOT EXISTS trades_closed (
    id                      BIGSERIAL PRIMARY KEY,
    order_id                TEXT NOT NULL UNIQUE,
    sid                     TEXT,
    strategy                TEXT,
    source                  TEXT,
    symbol                  TEXT NOT NULL,
    tf                      TEXT,
    direction               TEXT,           -- 'LONG' / 'SHORT'

    entry_ts_ms             BIGINT NOT NULL,
    exit_ts_ms              BIGINT NOT NULL,
    entry_ts                TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(entry_ts_ms / 1000.0)) STORED,
    exit_ts                 TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(exit_ts_ms / 1000.0)) STORED,

    entry_price             DOUBLE PRECISION NOT NULL,
    exit_price              DOUBLE PRECISION NOT NULL,
    lot                     DOUBLE PRECISION NOT NULL,
    notional_usd            DOUBLE PRECISION,

    pnl_net                 DOUBLE PRECISION NOT NULL,
    pnl_gross               DOUBLE PRECISION NOT NULL,
    fees                    DOUBLE PRECISION NOT NULL,
    pnl_pct                 DOUBLE PRECISION,

    -- baseline vs managed
    pnl_if_fixed_exit       DOUBLE PRECISION,
    baseline_exit_reason    TEXT,
    baseline_exit_ts_ms     BIGINT,
    baseline_exit_price     DOUBLE PRECISION,

    -- TP / SL / trailing
    tp1_hit                 BOOLEAN,
    tp2_hit                 BOOLEAN,
    tp3_hit                 BOOLEAN,
    tp_hits                 INTEGER,
    tp_before_sl            INTEGER,
    trailing_started        BOOLEAN,
    trailing_active         BOOLEAN,
    trailing_moves          INTEGER,
    trailing_profile        TEXT,

    -- экскурссии / giveback / missed
    mfe_pnl                 DOUBLE PRECISION,
    mae_pnl                 DOUBLE PRECISION,
    giveback                DOUBLE PRECISION,
    missed_profit           DOUBLE PRECISION,

    -- риск в R
    one_r_money             DOUBLE PRECISION,
    r_multiple              DOUBLE PRECISION,

    duration_ms             BIGINT,
    close_reason            TEXT,
    close_reason_raw        TEXT,
    close_reason_detail     TEXT DEFAULT '',

    entry_tag               TEXT,
    max_favorable_price     DOUBLE PRECISION,
    max_favorable_ts        BIGINT,

    is_final_close          BOOLEAN,
    remaining_qty           DOUBLE PRECISION,
    status                  TEXT,

    -- Health metrics at trade closure time
    health_l2_stale_ratio_tick    DOUBLE PRECISION,  -- L2 stale ratio (tick-relative)
    health_l2_stale_ratio_now     DOUBLE PRECISION,  -- L2 stale ratio (now-relative)
    health_avg_l2_age_ms          DOUBLE PRECISION,  -- Avg L2 age (ms)
    health_avg_l2_age_tick_ms     DOUBLE PRECISION,  -- Avg L2 age tick (ms)
    health_signal_emit_rate       DOUBLE PRECISION,  -- Signal emit rate (signals/sec)
    health_dlq_rate               DOUBLE PRECISION,  -- DLQ rate (errors/sec)

    created_at              TIMESTAMPTZ DEFAULT now()
);

-- Convert to hypertable if TimescaleDB is available
-- SELECT create_hypertable('trades_closed', 'exit_ts', if_not_exists => TRUE);

-- Indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_trades_closed_symbol_exit
    ON trades_closed(symbol, exit_ts);

CREATE INDEX IF NOT EXISTS idx_trades_closed_source_symbol_exit
    ON trades_closed(source, symbol, exit_ts);

CREATE INDEX IF NOT EXISTS idx_trades_closed_entry_tag_exit
    ON trades_closed(entry_tag, exit_ts);

CREATE INDEX IF NOT EXISTS idx_trades_closed_sid
    ON trades_closed(sid);
"""

# Migration 2: Create signal_family_baseline tables
BASELINE_MIGRATION_SQL = """
-- Migration: Create signal family baseline tables
-- Description: Creates tables for baseline calculations
-- Date: 2025-12-26

-- Enable TimescaleDB extension if available (optional)
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Таблица с результатами выполненных сигналов (исходные данные для baseline)
CREATE TABLE IF NOT EXISTS signal_exec_summary (
    signal_id      BIGINT PRIMARY KEY,
    symbol         TEXT        NOT NULL,   -- XAUUSD, BTCUSDT, ...
    family         TEXT        NOT NULL,   -- 'volatility_spike', 'reclaim', ...
    opened_at      TIMESTAMPTZ NOT NULL,
    closed_at      TIMESTAMPTZ NOT NULL,

    result_r       DOUBLE PRECISION NOT NULL, -- итог сделки в R
    mfe_r          DOUBLE PRECISION,          -- max favorable excursion (в R)
    mae_r          DOUBLE PRECISION,          -- max adverse excursion (в R)

    -- опционально:
    ttd_sec        DOUBLE PRECISION,  -- time-to-decay/до реализации edge, если считаешь
    extra_json     JSONB              -- любой доп. контекст
);

-- Создание гипертаблицы для оптимизации по времени
-- SELECT create_hypertable('signal_exec_summary', 'opened_at', if_not_exists => TRUE);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_signal_exec_summary_lookup
ON signal_exec_summary (symbol, family, opened_at);

-- 2. Таблица с baseline-квантилями для signal family
CREATE TABLE IF NOT EXISTS signal_family_baseline (
    symbol       TEXT        NOT NULL,
    family       TEXT        NOT NULL,
    metric       TEXT        NOT NULL,  -- 'hit_rate', 'expectancy_R'
    window_size  INTEGER     NOT NULL,  -- N сигналов в окне
    horizon_days INTEGER     NOT NULL,  -- сколько истории учитывали (например, 180)

    p05          DOUBLE PRECISION,
    p10          DOUBLE PRECISION,
    p25          DOUBLE PRECISION,
    p50          DOUBLE PRECISION,
    p75          DOUBLE PRECISION,
    p90          DOUBLE PRECISION,
    p95          DOUBLE PRECISION,

    sample_size  INTEGER     NOT NULL,  -- сколько окон реально посчитали
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, family, metric, window_size, horizon_days)
);

-- Индекс для быстрого поиска baseline
CREATE INDEX IF NOT EXISTS idx_signal_family_baseline_lookup
ON signal_family_baseline (symbol, family, metric);
"""

# Migration 3: Create signal_facts and trade_performance tables
SIGNAL_FACTS_MIGRATION_SQL = """
-- Migration: Create signal_facts and trade_performance tables
-- Description: Creates tables for signal facts and trade performance
-- Date: 2026-01-04

-- Основная таблица сигналов с L3-метриками
CREATE TABLE IF NOT EXISTS signal_facts (
    -- Hypertable partitioning
    ts                      TIMESTAMPTZ NOT NULL,

    -- Signal identity
    signal_id               TEXT        NOT NULL,
    symbol                  TEXT        NOT NULL,
    direction               SMALLINT    NOT NULL,  -- +1 long, -1 short, 0 neutral
    signal_family           TEXT        NOT NULL,

    -- Scores
    conf_score              DOUBLE PRECISION NOT NULL,

    -- Basic metrics
    atr_14                  DOUBLE PRECISION DEFAULT 0.0,
    delta_spike_z           DOUBLE PRECISION DEFAULT 0.0,
    obi_avg_20              DOUBLE PRECISION DEFAULT 0.0,
    weak_progress_ratio     DOUBLE PRECISION DEFAULT 0.0,

    -- L3-Lite metrics
    l3_spread_bps              DOUBLE PRECISION DEFAULT 0.0,
    l3_microprice_shift_bps_20 DOUBLE PRECISION DEFAULT 0.0,
    l3_microprice_velocity_bps DOUBLE PRECISION DEFAULT 0.0,

    l3_obi_5                   DOUBLE PRECISION DEFAULT 0.0,
    l3_obi_20                  DOUBLE PRECISION DEFAULT 0.0,
    l3_obi_50                  DOUBLE PRECISION DEFAULT 0.0,
    l3_obi_persistence_score   DOUBLE PRECISION DEFAULT 0.0,

    l3_cancel_to_trade_bid_5s  DOUBLE PRECISION DEFAULT 0.0,
    l3_cancel_to_trade_ask_5s  DOUBLE PRECISION DEFAULT 0.0,
    l3_cancel_to_trade_bid_20s DOUBLE PRECISION DEFAULT 0.0,
    l3_cancel_to_trade_ask_20s DOUBLE PRECISION DEFAULT 0.0,

    l3_queue_pressure_bid      DOUBLE PRECISION DEFAULT 0.0,
    l3_queue_pressure_ask      DOUBLE PRECISION DEFAULT 0.0,
    l3_market_depth_imbalance  DOUBLE PRECISION DEFAULT 0.0,

    -- Metadata
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (ts, signal_id)
);

-- Индексы для signal_facts
CREATE INDEX IF NOT EXISTS idx_signal_facts_symbol_ts ON signal_facts (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_signal_facts_family_ts ON signal_facts (signal_family, ts DESC);

-- Таблица результатов выполненных сигналов
CREATE TABLE IF NOT EXISTS trade_performance (
    -- Signal reference
    signal_id               TEXT             PRIMARY KEY,
    
    -- Timestamps
    ts_open                 TIMESTAMPTZ      NOT NULL,
    ts_close                TIMESTAMPTZ      NOT NULL,

    -- Signal context
    symbol                  TEXT             NOT NULL,
    direction               SMALLINT         NOT NULL,  -- +1 long, -1 short

    -- Результаты в R
    r                       DOUBLE PRECISION NOT NULL,  -- результат сделки в R
    hit                     BOOLEAN          NOT NULL,  -- win(true) / loss(false)

    -- Holding time
    holding_ms              BIGINT           NULL,

    -- Close reason
    close_reason_raw        TEXT,
    close_reason_bucket     TEXT,            -- 'sl', 'tp', 'manual', etc.

    -- Metadata
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- Индексы для trade_performance
CREATE INDEX IF NOT EXISTS idx_trade_performance_symbol_ts ON trade_performance (symbol, ts_open DESC);
"""

def create_database_if_not_exists():
    """Create scanner_analytics database if it doesn't exist"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'scanner_analytics';")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE scanner_analytics;")
                print("✓ Created scanner_analytics database")
            else:
                print("✓ scanner_analytics database already exists")

        conn.close()
        return True
    except Exception as e:
        print(f"❌ Failed to create/check database: {e}")
        return False

def apply_migration(name, sql):
    """Apply a single migration"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()
        print(f"✓ Applied {name} migration successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to apply {name} migration: {e}")
        return False

def verify_tables():
    """Verify that required tables were created"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Check trades_closed
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'trades_closed';")
            if cur.fetchone():
                print("✓ trades_closed table exists")
            else:
                print("❌ trades_closed table missing")
                return False

            # Check signal_family_baseline
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'signal_family_baseline';")
            if cur.fetchone():
                print("✓ signal_family_baseline table exists")
            else:
                print("❌ signal_family_baseline table missing")
                return False

        conn.close()
        return True
    except Exception as e:
        print(f"❌ Failed to verify tables: {e}")
        return False

def main():
    print("🔄 Creating scanner_analytics database tables...")

    # Create database
    if not create_database_if_not_exists():
        return False

    # Apply migrations
    if not apply_migration("trades_closed", TRADES_MIGRATION_SQL):
        return False

    if not apply_migration("signal_family_baseline", BASELINE_MIGRATION_SQL):
        return False

    if not apply_migration("signal_facts_and_performance", SIGNAL_FACTS_MIGRATION_SQL):
        return False

    # Verify
    if not verify_tables():
        return False

    print("✅ All scanner_analytics tables created successfully!")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
