-- 012_ml_feature_snapshot.sql
-- Hypertable for ML feature snapshots: stores the feature vector, schema metadata,
-- labels, and outcomes for every signal that passed through the ML gate.
-- Used for: offline retraining, calibration audits, missing-mask monitoring,
--           champion/challenger comparison, and dataset versioning.
--
-- Retention: 180 days (raw features are large; calibration windows need ~90 days).
-- Compression: after 14 days (JSONB compresses well with TimescaleDB).

CREATE TABLE IF NOT EXISTS ml_feature_snapshot (
    ts               timestamptz   NOT NULL,
    sid              text          NOT NULL,
    symbol           text          NOT NULL,
    direction        text          NOT NULL,
    scenario_base    text          NOT NULL DEFAULT '',
    scenario_v4      text          NOT NULL DEFAULT '',
    schema_name      text          NOT NULL,
    schema_version   int           NOT NULL,
    schema_hash      text          NOT NULL,
    feature_cols_hash text         NOT NULL DEFAULT '',
    label_config_hash text,
    dq_policy_hash   text,
    -- Feature vector and missing-mask summary
    features         jsonb         NOT NULL DEFAULT '{}',
    missing          jsonb         NOT NULL DEFAULT '[]',
    missing_count    smallint      NOT NULL DEFAULT 0,
    -- Labels (written after outcome is known; NULL until settled)
    y_edge           smallint,
    y_edge_cost_aware smallint,
    tb_outcome       text,
    edge_after_cost_bps double precision,
    -- Metadata
    gate_mode        text          NOT NULL DEFAULT 'SHADOW',
    p_edge           real,
    created_at       timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (ts, sid)
);

SELECT create_hypertable(
    'ml_feature_snapshot', 'ts',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshot_symbol_ts
    ON ml_feature_snapshot (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshot_schema_ts
    ON ml_feature_snapshot (schema_name, schema_hash, ts DESC);

CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshot_sid
    ON ml_feature_snapshot (sid);

-- Compression: after 14 days JSONB features compress ~5-10x
SELECT add_compression_policy(
    'ml_feature_snapshot',
    compress_after => INTERVAL '14 days',
    if_not_exists  => TRUE
);

-- Retention: 180 days (need 90+ days for calibration windows and 14-day canary)
SELECT add_retention_policy(
    'ml_feature_snapshot',
    drop_after    => INTERVAL '180 days',
    if_not_exists => TRUE
);
