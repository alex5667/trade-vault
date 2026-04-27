-- Experiment Layer Migration
-- Creates tables for A/B testing of signal filters and features

-- Enable TimescaleDB extension if not already enabled (fail-safe)
DO $$ 
BEGIN 
    CREATE EXTENSION IF NOT EXISTS timescaledb; 
EXCEPTION WHEN OTHERS THEN 
    RAISE NOTICE 'TimescaleDB extension not available, skipping...'; 
END $$;

-- 1. Таблица экспериментов signal_experiment
-- Определения активных и завершенных экспериментов
CREATE TABLE IF NOT EXISTS signal_experiment (
    experiment_id   text primary key,          -- "filt_obi_persist_2025q1"
    name            text not null,             -- человекочитаемое имя
    filter_name     text not null,             -- internal-имя фильтра/фичи
    signal_family   text not null,             -- например "volatility_spike"
    direction       int  not null default 0,   -- +1/-1/0 если эксперимент только в одну сторону

    created_at      timestamptz not null default now(),
    start_at        timestamptz not null,      -- когда стартует эксперимент
    end_at          timestamptz,               -- когда надо остановить (опционально)

    status          text not null default 'draft',
    -- 'draft' | 'running' | 'stopped' | 'completed'

    target_metric   text not null,             -- "expectancy_R", "Sharpe_R", "drawdown_R"
    config          jsonb                      -- произвольные параметры (weights, threshold, etc.)
);

-- Convert to hypertable (chunk by 90 days)
DO $$ 
BEGIN 
    PERFORM create_hypertable('signal_experiment', 'created_at', chunk_time_interval => INTERVAL '90 days', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN 
    RAISE NOTICE 'Could not create hypertable for signal_experiment, skipping...'; 
END $$;

-- Indexes for experiments
CREATE INDEX IF NOT EXISTS idx_signal_experiment_status_start ON signal_experiment (status, start_at);
CREATE INDEX IF NOT EXISTS idx_signal_experiment_family ON signal_experiment (signal_family);
CREATE INDEX IF NOT EXISTS idx_signal_experiment_filter_name ON signal_experiment (filter_name);

-- 2. Теги на уровне сигналов (расширение существующей таблицы signals)
-- Добавляем колонки для экспериментов в существующую таблицу signals
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS signal_family      text,
    ADD COLUMN IF NOT EXISTS experiment_id      text,
    ADD COLUMN IF NOT EXISTS experiment_variant text,   -- "control", "treatment", "treatment_B" ...
    ADD COLUMN IF NOT EXISTS filter_flags       jsonb;  -- { "obi_new_filter_passed": true, "quality_gate_v2_passed": false }

-- Index for experiment queries
CREATE INDEX IF NOT EXISTS idx_signals_experiment_id ON signals (experiment_id);
CREATE INDEX IF NOT EXISTS idx_signals_experiment_variant ON signals (experiment_variant);
CREATE INDEX IF NOT EXISTS idx_signals_experiment_family_ts ON signals (experiment_id, signal_family, ts_signal DESC);

-- 3. Таблица снапшотов метрик по экспериментам
-- Чтобы не считать тяжелые аггрегаты каждый раз на лету
CREATE TABLE IF NOT EXISTS signal_experiment_snapshot (
    experiment_id   text         not null,
    as_of           timestamptz  not null,
    variant         text         not null,  -- "control", "treatment"

    signals_total   integer      not null,
    traded_total    integer      not null,
    winners_total   integer      not null,  -- pnl_R >= success_threshold
    losers_total    integer      not null,

    expectancy_r    double precision,
    sharpe_r        double precision,
    max_dd_r        double precision,
    cl_ratio        double precision,      -- expectancy / |avg_loss_R|
    winrate         double precision,

    precision       double precision,
    recall          double precision,
    f1              double precision,

    extra           jsonb,                 -- доп.метрики

    primary key (experiment_id, as_of, variant)
);

-- Convert to hypertable (chunk by 7 days)
DO $$ 
BEGIN 
    PERFORM create_hypertable('signal_experiment_snapshot', 'as_of', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN 
    RAISE NOTICE 'Could not create hypertable for signal_experiment_snapshot, skipping...'; 
END $$;

-- Indexes for snapshots
CREATE INDEX IF NOT EXISTS idx_experiment_snapshot_experiment_as_of ON signal_experiment_snapshot (experiment_id, as_of DESC);
CREATE INDEX IF NOT EXISTS idx_experiment_snapshot_variant ON signal_experiment_snapshot (variant);

-- Add comments for documentation
COMMENT ON TABLE signal_experiment IS 'Definitions of A/B experiments for signal filters and features';
COMMENT ON TABLE signal_experiment_snapshot IS 'Pre-calculated metrics snapshots for experiment evaluation';
COMMENT ON COLUMN signals.experiment_id IS 'ID of experiment this signal participated in';
COMMENT ON COLUMN signals.experiment_variant IS 'Which variant (control/treatment) this signal was assigned to';
COMMENT ON COLUMN signals.filter_flags IS 'JSON object with results of filter applications for experiment analysis';

-- Create a view for experiment analysis
CREATE OR REPLACE VIEW experiment_signal_summary AS
SELECT
    s.signal_id,
    s.ts_signal,
    s.symbol,
    s.side,
    s.setup_type,
    s.experiment_id,
    s.experiment_variant,
    s.filter_flags,
    sp.realized_R,
    sp.outcome,
    CASE WHEN sp.realized_R >= 0.2 THEN 1 ELSE 0 END as is_winner,
    CASE WHEN sp.outcome IN ('realized', 'stopped') THEN 1 ELSE 0 END as was_traded
FROM signals s
LEFT JOIN signal_performance sp ON s.signal_id = sp.signal_id
WHERE s.experiment_id IS NOT NULL;

COMMENT ON VIEW experiment_signal_summary IS 'Unified view for experiment analysis with trade outcomes';























































