-- Таблицы для системы контроля качества сигналов (Regime Guard)
-- Создавать в TimescaleDB

-- 1. Базовые пороги по family (исторические квантиль/лимиты)
CREATE TABLE IF NOT EXISTS signal_family_baseline (
    family         text        NOT NULL,
    venue          text        NOT NULL,
    symbol         text        NOT NULL,
    timeframe      text        NOT NULL,
    -- квантили и лимиты, рассчитанные по истории
    wr_p10         double precision NOT NULL,
    wr_p50         double precision NOT NULL,
    exp_r_p10      double precision NOT NULL,
    exp_r_p50      double precision NOT NULL,
    dd_r_limit     double precision NOT NULL,  -- допустимая просадка по R (отрицательное число, например -7.0)
    min_trades     integer    NOT NULL DEFAULT 50, -- с какого числа сделок считать статистику достоверной
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (family, venue, symbol, timeframe)
);

-- 2. История состояний режима (regime state)
CREATE TABLE IF NOT EXISTS signal_family_regime_state (
    ts_state       timestamptz NOT NULL,
    family         text        NOT NULL,
    venue          text        NOT NULL,
    symbol         text        NOT NULL,
    timeframe      text        NOT NULL,

    status         text        NOT NULL, -- 'active' | 'degraded' | 'disabled'

    wr_window      double precision,
    exp_r_window   double precision,
    dd_r_window    double precision,
    trades_window  integer,

    reason         text,                -- текстовое объяснение, что сломалось
    disable_until  timestamptz,         -- до какого времени off (конец сессии/дня)
    threshold_mult double precision,    -- во сколько раз подняты пороги (для degraded)

    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Создание гипертаблицы для оптимизации по времени
SELECT create_hypertable('signal_family_regime_state', 'ts_state');

-- Индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_signal_family_regime_state_lookup
ON signal_family_regime_state (family, venue, symbol, timeframe, ts_state DESC);

-- 3. "Чёрные зоны" по новостям
CREATE TABLE IF NOT EXISTS signal_news_blackzone (
    id             bigserial PRIMARY KEY,
    venue          text        NOT NULL,      -- 'mt5', 'binance', ...
    symbol_pattern text        NOT NULL,      -- 'XAUUSD', 'XAU*', '%'
    family_pattern text        NOT NULL,      -- 'volatilitySpike', 'weakProgress', '%' (для всех)
    timeframe      text        NOT NULL,      -- '1m', '5m', '%'

    ts_start       timestamptz NOT NULL,
    ts_end         timestamptz NOT NULL,

    mode           text        NOT NULL,      -- 'blocked' | 'strict'
    description    text,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Индексы для быстрого поиска активных зон
CREATE INDEX IF NOT EXISTS idx_signal_news_blackzone_active
ON signal_news_blackzone (venue, ts_start, ts_end)
WHERE ts_end > now();

CREATE INDEX IF NOT EXISTS idx_signal_news_blackzone_lookup
ON signal_news_blackzone (venue, symbol_pattern, family_pattern, timeframe);
