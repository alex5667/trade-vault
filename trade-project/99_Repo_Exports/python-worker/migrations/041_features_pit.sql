-- python-worker/migrations/041_features_pit.sql
-- Создание Feature Store с point-in-time корректностью для ML без look-ahead bias.

CREATE TABLE IF NOT EXISTS features_pit (
    symbol      TEXT,
    ts          BIGINT,  -- epoch ms (время фиксации feature)
    feature_set JSONB,   -- все features в момент ts
    PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable('features_pit', 'ts',
    chunk_time_interval => 3600000,
    if_not_exists => TRUE);
