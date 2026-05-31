-- of_inputs_history: persistent archive of signals:of:inputs for ML training.
-- Stores full JSONB payload + indexed hot columns for fast joins with trades_closed.
-- Added 2026-05-20: fixes "мало данных" blocker — Redis stream (maxlen=5000) loses
-- old signals; training dataset was limited to ~2.8 days of history.

BEGIN;

CREATE TABLE IF NOT EXISTS of_inputs_history (
    id              BIGSERIAL           NOT NULL,
    ts_ms           BIGINT              NOT NULL,
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT now(),
    sid             TEXT                NOT NULL,
    symbol          TEXT                NOT NULL,
    direction       TEXT,
    feature_schema_version  SMALLINT    DEFAULT 14,
    regime          TEXT,
    confidence      DOUBLE PRECISION,
    is_virtual      BOOLEAN             DEFAULT false,
    payload         JSONB               NOT NULL,
    CONSTRAINT of_inputs_history_sid_key UNIQUE (sid)
);

-- Primary lookup: join with trades_closed by sid + symbol/time range for dataset builder
CREATE INDEX IF NOT EXISTS idx_of_inputs_history_sid           ON of_inputs_history (sid);
CREATE INDEX IF NOT EXISTS idx_of_inputs_history_sym_ts        ON of_inputs_history (symbol, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_of_inputs_history_ts_ms         ON of_inputs_history (ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_of_inputs_history_schema_ver    ON of_inputs_history (feature_schema_version, ts_ms DESC);

COMMIT;
