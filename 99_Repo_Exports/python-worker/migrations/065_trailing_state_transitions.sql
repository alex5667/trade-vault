-- 065_trailing_state_transitions — Phase B trailing state audit
-- 2026-05-23

CREATE TABLE IF NOT EXISTS trailing_state_transitions (
  ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
  sid              TEXT NOT NULL,
  position_id      TEXT,
  symbol           TEXT NOT NULL,
  side             TEXT NOT NULL,
  from_state       TEXT,
  to_state         TEXT NOT NULL,
  event_type       TEXT NOT NULL,
  price            DOUBLE PRECISION,
  old_sl           DOUBLE PRECISION,
  new_sl           DOUBLE PRECISION,
  high_watermark   DOUBLE PRECISION,
  low_watermark    DOUBLE PRECISION,
  atr_value        DOUBLE PRECISION,
  atr_mult         DOUBLE PRECISION,
  reason_code      TEXT NOT NULL,
  idempotency_key  TEXT,
  profile          TEXT,
  profile_hash     TEXT,
  policy_hash      TEXT,
  payload          JSONB
);

SELECT create_hypertable('trailing_state_transitions', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_tst_sid ON trailing_state_transitions (sid, ts DESC);
CREATE INDEX IF NOT EXISTS idx_tst_symbol ON trailing_state_transitions (symbol, ts DESC);
