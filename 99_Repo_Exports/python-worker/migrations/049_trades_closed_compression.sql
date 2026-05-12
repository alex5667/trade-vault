-- +goose Up
-- +goose StatementBegin

-- Устанавливаем TimescaleDB compression для trades_closed
ALTER TABLE trades_closed SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'trade_id DESC'
);

-- Добавляем политику компрессии для старых данных (старше 7 дней)
SELECT add_compression_policy('trades_closed', INTERVAL '7 days');

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Удаляем политику компрессии и отключаем компрессию
SELECT remove_compression_policy('trades_closed', if_exists => true);
ALTER TABLE trades_closed SET (timescaledb.compress = false);

-- +goose StatementEnd
