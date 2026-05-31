# scanner_analytics setup (Postgres + TimescaleDB)

## 1) Создать БД и включить TimescaleDB
```bash
docker exec -it postgres psql -U postgres
```
Внутри psql:
```
CREATE DATABASE scanner_analytics;
\c scanner_analytics
CREATE EXTENSION IF NOT EXISTS timescaledb;
ALTER DATABASE scanner_analytics SET timescaledb.telemetry_level = 'off';
\i docs/scanner_analytics_schema.sql
```

## 2) Пример docker-compose (один Postgres, две БД)
См. `docker-compose.analytics.example.yml`.
- `trade_back` → `postgresql://postgres:postgres@postgres:5432/trade_back`
- `scanner_infra`/Python-скрипты → `postgresql://postgres:postgres@postgres:5432/scanner_analytics`

## 3) DSN для Python аналитики
- Env: `TRADES_DB_DSN="postgresql://postgres:postgres@postgres:5432/scanner_analytics"`
- Хелпер: `python-worker/services/analytics_db.py`
    - `fetch_trades_closed(limit, symbol, source)`
    - `fetch_daily_metrics(...)`
    - `fetch_entry_tag_metrics(...)`

## 4) Метрики/ретеншн (опционально, позже)
```
SELECT add_retention_policy('trades_closed', INTERVAL '90 days');
SELECT add_compression_policy('trades_closed', INTERVAL '7 days');
ALTER TABLE trades_closed SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol,source');
```

## 5) REST-метрики (идея интерфейса)
scanner_infra предоставляет HTTP:
- `GET /metrics/trades?symbol=ETHUSDT&source=CryptoOrderFlow&window=last_30`
- `GET /metrics/entry-tags?symbol=ETHUSDT&date=2025-12-11`

trade_back запрашивает эти эндпоинты и рендерит UI; прямой доступ к `scanner_analytics` не требуется.

