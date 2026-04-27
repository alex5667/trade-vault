CREATE TABLE IF NOT EXISTS trades_closed_p0 (
    order_id                TEXT NOT NULL,
    exit_ts_ms              BIGINT NOT NULL,
    exit_ts                 TIMESTAMPTZ,

    scenario                TEXT,
    regime                  TEXT,
    session                 TEXT,
    entry_reason            TEXT,

    mae_bps                 DOUBLE PRECISION,
    mfe_bps                 DOUBLE PRECISION,
    time_to_mfe_ms          BIGINT,
    hold_ms                 BIGINT,

    spread_bps_at_entry     DOUBLE PRECISION,
    slippage_bps_est        DOUBLE PRECISION,
    book_age_ms             BIGINT,

    features_json           JSONB,

    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (order_id, exit_ts)
);

-- Trigger to populate exit_ts from exit_ts_ms
CREATE OR REPLACE FUNCTION populate_exit_ts() RETURNS TRIGGER AS $$
BEGIN
    NEW.exit_ts := to_timestamp(NEW.exit_ts_ms / 1000.0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_populate_exit_ts ON trades_closed_p0;
CREATE TRIGGER trg_populate_exit_ts
BEFORE INSERT OR UPDATE ON trades_closed_p0
FOR EACH ROW EXECUTE FUNCTION populate_exit_ts();

SELECT create_hypertable('trades_closed_p0', 'exit_ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_order_id ON trades_closed_p0(order_id);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_exit ON trades_closed_p0(exit_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_scenario_exit ON trades_closed_p0(scenario, exit_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_regime_exit ON trades_closed_p0(regime, exit_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_session_exit ON trades_closed_p0(session, exit_ts DESC);
