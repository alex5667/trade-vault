-- Схема для результатов сигналов (trade_performance)
-- TimescaleDB

-- Таблица результатов выполненных сигналов
CREATE TABLE IF NOT EXISTS trade_performance (
    -- Hypertable partitioning
    ts_open                 TIMESTAMPTZ      NOT NULL,  -- время открытия сделки
    ts_close                TIMESTAMPTZ      NOT NULL,  -- время закрытия сделки

    -- Signal reference
    signal_id               TEXT             NOT NULL,

    -- Signal context
    symbol                  TEXT             NOT NULL,
    direction               SMALLINT         NOT NULL,  -- +1 long, -1 short

    -- Результаты в R
    r                       DOUBLE PRECISION NOT NULL,  -- результат сделки в R
    hit                     BOOLEAN          NOT NULL,  -- win(true) / loss(false)

    -- Holding time
    holding_ms              BIGINT           NULL,

    -- Дополнительные метрики (опционально)
    slippage_bps            DOUBLE PRECISION DEFAULT 0.0,
    adverse_bps             DOUBLE PRECISION DEFAULT 0.0,

    -- Close reason
    close_reason_raw        TEXT,
    close_reason_bucket     TEXT,            -- 'sl', 'tp', 'manual', etc.

    -- Metadata
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (signal_id)
);

-- Создание гипертаблицы (если TimescaleDB доступен)
-- SELECT create_hypertable('trade_performance', 'ts_open', if_not_exists => TRUE);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_trade_performance_signal_id
ON trade_performance (signal_id);

CREATE INDEX IF NOT EXISTS idx_trade_performance_symbol_ts
ON trade_performance (symbol, ts_open DESC);

CREATE INDEX IF NOT EXISTS idx_trade_performance_hit_ts
ON trade_performance (hit, ts_open DESC);

CREATE INDEX IF NOT EXISTS idx_trade_performance_r_ts
ON trade_performance (r, ts_open DESC);
