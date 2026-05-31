-- sql/006_timescale_prod_policies.sql

DO $$
BEGIN
  ---------------------------------------------------------
  -- 1. FIX CHUNK INTERVALS (Снизить RAM-нагрузку)
  ---------------------------------------------------------
  -- Для таблиц на базе TIMESTAMPTZ
  PERFORM set_chunk_time_interval('bbo_ts', INTERVAL '1 day');
  PERFORM set_chunk_time_interval('fills', INTERVAL '1 day');
  PERFORM set_chunk_time_interval('tca_fill_metrics', INTERVAL '1 day');
  -- Для таблиц на базе BIGINT (ms) -> 86400000 ms
  PERFORM set_chunk_time_interval('decision_snapshot', 86400000); 

  ---------------------------------------------------------
  -- 2. ENABLE COMPRESSION (Снизить I/O и расход диска)
  ---------------------------------------------------------
  -- microbars (Огромный объем исторических данных)
  ALTER TABLE microbars SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'ts_ms DESC'
  );
  PERFORM add_compression_policy('microbars', INTERVAL '7 days');
  
  -- fills (Транзакции)
  ALTER TABLE fills SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sym,venue',
    timescaledb.compress_orderby = 'ts DESC'
  );
  PERFORM add_compression_policy('fills', INTERVAL '7 days');

  -- tca_fill_metrics (Метрики сделок)
  ALTER TABLE tca_fill_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sym,venue',
    timescaledb.compress_orderby = 'ts DESC'
  );
  PERFORM add_compression_policy('tca_fill_metrics', INTERVAL '7 days');

  ---------------------------------------------------------
  -- 3. ENFORCE RETENTION POLICIES (Защита от переполнения диска)
  ---------------------------------------------------------
  -- Экстремально тяжелые данные: удаляем быстро
  PERFORM add_retention_policy('bbo_ts', INTERVAL '10 days');
  
  -- Средние сырые данные: оставляем для краткосрочных реплеев
  PERFORM add_retention_policy('microbars', INTERVAL '30 days');
  PERFORM add_retention_policy('decision_snapshot', INTERVAL '60 days');
  
  -- Легкие данные TCA / Labeling: храним для бэктестов и аналитики
  PERFORM add_retention_policy('tb_labels', INTERVAL '180 days');
  PERFORM add_retention_policy('fills', INTERVAL '180 days');
  PERFORM add_retention_policy('tca_fill_metrics', INTERVAL '180 days');

EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'Policy application skipped or already exists: %', SQLERRM;
END $$;
