-- 007_create_trades_closed_p0.sql
-- Create P0 analytic layer for trades_closed

CREATE TABLE IF NOT EXISTS trades_closed_p0 (
    -- join key (matches trades_closed.order_id)
    order_id                TEXT NOT NULL,

    -- time for hypertable (matches trades_closed.exit_ts_ms logic)
    exit_ts_ms              BIGINT NOT NULL,
    exit_ts                 TIMESTAMPTZ,

    -- P0 slicing (context)
    scenario                TEXT,
    regime                  TEXT,
    session                 TEXT,
    entry_reason            TEXT,

    -- P0 excursions/timing (bps + ms)
    mae_bps                 DOUBLE PRECISION,
    mfe_bps                 DOUBLE PRECISION,
    time_to_mfe_ms          BIGINT,
    hold_ms                 BIGINT,

    -- execution-cost proxies at entry
    spread_bps_at_entry     DOUBLE PRECISION,
    slippage_bps_est        DOUBLE PRECISION,
    book_age_ms             BIGINT,

    -- snapshot of features (trimmed/whitelisted)
    features_json           JSONB,

    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now(),

    -- Timescale requirement: PK/UNIQUE must include time dimension
    PRIMARY KEY (order_id, exit_ts)
);

-- Trigger to populate exit_ts from exit_ts_ms
CREATE OR REPLACE FUNCTION populate_exit_ts_p0() RETURNS TRIGGER AS $$
BEGIN
    NEW.exit_ts := to_timestamp(NEW.exit_ts_ms / 1000.0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_populate_exit_ts_p0 ON trades_closed_p0;
CREATE TRIGGER trg_populate_exit_ts_p0
BEFORE INSERT OR UPDATE ON trades_closed_p0
FOR EACH ROW EXECUTE FUNCTION populate_exit_ts_p0();

-- Convert to hypertable (if not exists prevents error if already HT)
SELECT create_hypertable('trades_closed_p0', 'exit_ts', if_not_exists => TRUE);

-- Indexes for efficient slicing
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_order_id ON trades_closed_p0(order_id);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_order_exitms ON trades_closed_p0(order_id, exit_ts_ms);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_exit ON trades_closed_p0(exit_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_scenario_exit ON trades_closed_p0(scenario, exit_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_regime_exit ON trades_closed_p0(regime, exit_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_session_exit ON trades_closed_p0(session, exit_ts DESC);
