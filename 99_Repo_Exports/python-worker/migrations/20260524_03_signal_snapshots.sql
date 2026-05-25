-- Signal snapshots: persistent 30d archive of `signals:of:inputs` payloads.
-- Backs `train_v15_lgbm.py` and any future ML training that needs > Redis retention.
--
-- Why: Redis `signals:of:inputs` retention ~5k entries = ~4d.
--      `trades:closed` retention 30d. Join rate was 7%.
--
-- TimescaleDB hypertable: time-partitioned by ts (chunk = 1 day),
-- 30d retention via timescaledb retention policy.
--
-- Producer: services/signal_snapshot_persister.py (XREADGROUP from
-- signals:of:inputs, batched INSERT into this table).
-- Consumer: tools/train_v15_lgbm.py (SELECT WHERE ts >= now - 30d).
--
-- Added: 2026-05-24, migration 20260524_03

CREATE TABLE IF NOT EXISTS signal_snapshots (
    sid             TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,           -- emit time (from inner.ts_ms or stream entry id)
    ts_ms           BIGINT NOT NULL,                -- raw epoch ms (avoids tz mistakes in joins)
    symbol          TEXT NOT NULL,
    direction       TEXT,                           -- LONG/SHORT
    kind            TEXT,                           -- of/iceberg/delta_spike/…
    regime          TEXT,                           -- na/range/trending_bull/…
    -- Aggregates frequently used by trainers and quick joins (also queryable
    -- without parsing the JSONB):
    confidence      DOUBLE PRECISION,
    ml_shadow_conf01 DOUBLE PRECISION,
    scorer_mode     TEXT,                           -- shadow / canary_shadow / ml_canary_enforce / ml_canary_fallback
    -- The full indicators dict (after publisher's _stringify). JSONB is GIN-able
    -- and TOAST-compressed automatically — much smaller than raw text on disk.
    indicators      JSONB,
    -- Compressed full envelope kept for forensic reproduction:
    payload_gz      BYTEA,                          -- gzip(json.dumps(envelope))
    payload_size_bytes INTEGER,                     -- raw size before gzip — coverage monitor
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT signal_snapshots_pk PRIMARY KEY (sid, ts)
);

-- Hypertable: 1-day chunks (matches retention granularity)
SELECT create_hypertable(
    'signal_snapshots',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for trainer queries
CREATE INDEX IF NOT EXISTS ix_signal_snapshots_symbol_ts
    ON signal_snapshots (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS ix_signal_snapshots_ts_ms
    ON signal_snapshots (ts_ms);
CREATE INDEX IF NOT EXISTS ix_signal_snapshots_scorer_mode
    ON signal_snapshots (scorer_mode)
    WHERE scorer_mode IS NOT NULL;
-- For the most common train query: time-range + symbol filter
CREATE INDEX IF NOT EXISTS ix_signal_snapshots_ts_symbol
    ON signal_snapshots (ts DESC, symbol);

-- 30d retention. Daily background job drops chunks older than 30 days.
-- Idempotent on re-apply.
SELECT add_retention_policy(
    'signal_snapshots',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Compression: 7-day-old chunks get compressed (saves ~10× disk).
-- Indicators column is large JSONB; segmentby symbol clusters hot reads.
ALTER TABLE signal_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy(
    'signal_snapshots',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

COMMENT ON TABLE signal_snapshots IS
    '30d archive of signals:of:inputs payloads. Backs ML training when Redis stream retention is too short. Producer: services/signal_snapshot_persister.py. Consumer: tools/train_v15_lgbm.py.';

COMMENT ON COLUMN signal_snapshots.payload_gz IS
    'gzip-compressed JSON envelope. Use for forensic reproduction; for ML training read indicators JSONB directly.';
