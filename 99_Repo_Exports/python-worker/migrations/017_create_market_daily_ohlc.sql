CREATE TABLE IF NOT EXISTS market_daily_ohlc (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (symbol, date)
);
CREATE INDEX IF NOT EXISTS market_daily_ohlc_symbol_date_idx ON market_daily_ohlc (symbol, date DESC);
