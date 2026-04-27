-- P9: retention + partitioning helpers for execution journal/quarantine ledger
-- FIX 1: партиционированные таблицы не могут иметь UNIQUE/PK без колонки партиционирования.
--        Используем явные колонки без PK-ограничения; PRIMARY KEY включает колонку партиции.
-- FIX 2: функция purge_execution_hot_tables исправлена под реальную схему execution_quarantine_ledger
--        (колонки: id, sid, symbol, action, severity, reason, source, quarantine_key, applied,
--         state_jsonb, event_ts_ms, created_at_ms — нет event_type/payload_jsonb).

-- Archive для execution_order_events
-- PK включает event_ts_ms (колонка партиционирования) — PostgreSQL требует это.
CREATE TABLE IF NOT EXISTS execution_order_events_archive (
    id            BIGINT NOT NULL,
    sid           TEXT   NOT NULL,
    symbol        TEXT   NOT NULL DEFAULT '',
    event_type    TEXT   NOT NULL,
    event_ts_ms   BIGINT NOT NULL,
    payload_jsonb JSONB  NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT execution_order_events_archive_pkey PRIMARY KEY (id, event_ts_ms)
) PARTITION BY RANGE (event_ts_ms);

-- Archive для execution_quarantine_ledger
-- PK включает created_ts_ms (колонка партиционирования).
CREATE TABLE IF NOT EXISTS execution_quarantine_ledger_archive (
    id             BIGINT   NOT NULL,
    sid            TEXT     NOT NULL,
    symbol         TEXT     NOT NULL DEFAULT '',
    action         TEXT     NOT NULL,
    severity       TEXT     NOT NULL DEFAULT '',
    reason         TEXT     NOT NULL DEFAULT '',
    source         TEXT     NOT NULL DEFAULT '',
    quarantine_key TEXT     NOT NULL DEFAULT '',
    applied        BOOLEAN  NOT NULL DEFAULT TRUE,
    state_jsonb    JSONB    NOT NULL DEFAULT '{}'::jsonb,
    event_ts_ms    BIGINT   NOT NULL,
    created_at_ms  BIGINT   NOT NULL,
    CONSTRAINT execution_quarantine_ledger_archive_pkey PRIMARY KEY (id, created_at_ms)
) PARTITION BY RANGE (created_at_ms);

CREATE OR REPLACE FUNCTION ensure_monthly_range_partition(parent_table text, partition_prefix text, ts_ms bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    start_ts  TIMESTAMPTZ;
    end_ts    TIMESTAMPTZ;
    part_name TEXT;
    from_ms   BIGINT;
    to_ms     BIGINT;
BEGIN
    start_ts  := date_trunc('month', to_timestamp(ts_ms / 1000.0) AT TIME ZONE 'utc');
    end_ts    := start_ts + INTERVAL '1 month';
    part_name := partition_prefix || to_char(start_ts, 'YYYYMM');
    from_ms   := (EXTRACT(EPOCH FROM start_ts) * 1000)::BIGINT;
    to_ms     := (EXTRACT(EPOCH FROM end_ts)   * 1000)::BIGINT;
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
        part_name, parent_table, from_ms, to_ms
    );
END$$;

-- FIX: копируем все колонки execution_quarantine_ledger явно (нет event_type/payload_jsonb)
CREATE OR REPLACE FUNCTION purge_execution_hot_tables(cutoff_ts_ms BIGINT)
RETURNS TABLE(purged_events BIGINT, purged_quarantine BIGINT) LANGUAGE plpgsql AS $$
DECLARE
    ev_count BIGINT := 0;
    q_count  BIGINT := 0;
BEGIN
    -- Архивируем execution_order_events
    INSERT INTO execution_order_events_archive
        (id, sid, symbol, event_type, event_ts_ms, payload_jsonb)
    SELECT   id, sid, symbol, event_type, event_ts_ms, payload_jsonb
    FROM execution_order_events
    WHERE event_ts_ms < cutoff_ts_ms;
    GET DIAGNOSTICS ev_count = ROW_COUNT;
    DELETE FROM execution_order_events WHERE event_ts_ms < cutoff_ts_ms;

    -- Архивируем execution_quarantine_ledger (реальная схема: нет event_type/payload_jsonb)
    INSERT INTO execution_quarantine_ledger_archive
        (id, sid, symbol, action, severity, reason, source, quarantine_key,
         applied, state_jsonb, event_ts_ms, created_at_ms)
    SELECT   id, sid, symbol, action, severity, reason, source, quarantine_key,
             applied, state_jsonb, event_ts_ms, created_at_ms
    FROM execution_quarantine_ledger
    WHERE created_at_ms < cutoff_ts_ms;
    GET DIAGNOSTICS q_count = ROW_COUNT;
    DELETE FROM execution_quarantine_ledger WHERE created_at_ms < cutoff_ts_ms;

    RETURN QUERY SELECT ev_count, q_count;
END$$;
