-- scanner_analytics schema (TimescaleDB)
-- Usage:
--   1) psql -U postgres -h <host> -p 5432 -d postgres
--   2) CREATE DATABASE scanner_analytics;
--   3) \c scanner_analytics
--   4) \i docs/scanner_analytics_schema.sql
--
-- Last reconciled with live DB: 2026-05-17
-- Source of truth: live `scanner_analytics` schema (information_schema.columns).
-- When adding new columns to trades_closed*, ALWAYS update:
--   1) migration in python-worker/migrations/*.sql
--   2) INSERT/UPDATE in python-worker/services/analytics_db.py and/or batch_trade_writer.py
--   3) this file (CREATE TABLE block + phase-grouped comment)
-- See CLAUDE.md → "Schema Governance: trades_closed Table".

-- Enable extension (requires TimescaleDB image/extension installed)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Disable telemetry (optional, harmless if lacks permission)
ALTER DATABASE scanner_analytics SET timescaledb.telemetry_level = 'off';

-- =========================================================================
-- trades_closed (100 columns, reconciled 2026-05-17)
-- =========================================================================
-- Hypertable on exit_ts (TimescaleDB).
-- Generated columns ind_* are derived from config_json -> 'indicators'.
-- =========================================================================
CREATE TABLE IF NOT EXISTS trades_closed (
    id                            BIGSERIAL PRIMARY KEY,
    order_id                      TEXT NOT NULL UNIQUE,
    sid                           TEXT,
    strategy                      TEXT,
    source                        TEXT,
    symbol                        TEXT NOT NULL,
    tf                            TEXT,
    direction                     TEXT,

    -- ── Time (ms epoch + generated TIMESTAMPTZ) ────────────────────────────
    entry_ts_ms                   BIGINT NOT NULL,
    exit_ts_ms                    BIGINT NOT NULL,
    entry_ts                      TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(entry_ts_ms / 1000.0)) STORED,
    exit_ts                       TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(exit_ts_ms / 1000.0)) STORED,

    -- ── Prices & sizing ───────────────────────────────────────────────────
    entry_price                   DOUBLE PRECISION NOT NULL,
    exit_price                    DOUBLE PRECISION NOT NULL,
    lot                           DOUBLE PRECISION NOT NULL,
    notional_usd                  DOUBLE PRECISION,

    -- ── PnL & fees ────────────────────────────────────────────────────────
    pnl_net                       DOUBLE PRECISION NOT NULL,
    pnl_gross                     DOUBLE PRECISION NOT NULL,
    fees                          DOUBLE PRECISION NOT NULL,
    pnl_pct                       DOUBLE PRECISION,

    -- ── Counterfactual baseline (fixed-exit comparison) ───────────────────
    pnl_if_fixed_exit             DOUBLE PRECISION,
    baseline_exit_reason          TEXT,
    baseline_exit_ts_ms           BIGINT,
    baseline_exit_price           DOUBLE PRECISION,

    -- ── TP/Trailing ladder ────────────────────────────────────────────────
    tp1_hit                       BOOLEAN,
    tp2_hit                       BOOLEAN,
    tp3_hit                       BOOLEAN,
    tp_hits                       INTEGER,
    tp_before_sl                  INTEGER,
    trailing_started              BOOLEAN,
    trailing_active               BOOLEAN,
    trailing_moves                INTEGER,
    trailing_profile              TEXT,

    -- ── MFE / MAE / giveback / missed ─────────────────────────────────────
    mfe_pnl                       DOUBLE PRECISION,
    mae_pnl                       DOUBLE PRECISION,
    giveback                      DOUBLE PRECISION,
    missed_profit                 DOUBLE PRECISION,

    -- ── R-metrics & duration ──────────────────────────────────────────────
    one_r_money                   DOUBLE PRECISION,
    r_multiple                    DOUBLE PRECISION,
    duration_ms                   BIGINT,

    -- ── Close reason ──────────────────────────────────────────────────────
    close_reason                  TEXT,
    close_reason_raw              TEXT,
    close_reason_detail           TEXT DEFAULT '',

    -- ── Entry tag & favorable extrema ─────────────────────────────────────
    entry_tag                     TEXT,
    max_favorable_price           DOUBLE PRECISION,
    max_favorable_ts              BIGINT,

    -- ── Lifecycle flags ───────────────────────────────────────────────────
    is_final_close                BOOLEAN,
    remaining_qty                 DOUBLE PRECISION,
    status                        TEXT,

    -- ── Book Health Metrics (migration 008) ───────────────────────────────
    health_l2_stale_ratio_tick    DOUBLE PRECISION,
    health_l2_stale_ratio_now     DOUBLE PRECISION,
    health_avg_l2_age_ms          DOUBLE PRECISION,
    health_avg_l2_age_tick_ms     DOUBLE PRECISION,
    health_signal_emit_rate       DOUBLE PRECISION,
    health_dlq_rate               DOUBLE PRECISION,

    created_at                    TIMESTAMPTZ DEFAULT now(),

    -- ── Virtual Trade Flag (migration 025) ────────────────────────────────
    is_virtual                    BOOLEAN DEFAULT FALSE,

    -- ── ATR Policy Selection (fix_analytics_db.sql) ───────────────────────
    atr_policy_ver                INTEGER,
    atr_policy_tag                TEXT,
    atr_policy_scenario           TEXT,
    atr_policy_regime             TEXT,
    atr_policy_bucket             TEXT,
    atr_stop_ttl_mode             TEXT,
    atr_trailing_mode             TEXT,
    atr_recovery_run_id           TEXT,
    atr_restore_cert_status       TEXT,

    -- ── Strategy Contract Scalars (fix_analytics_db.sql, Phase 0.3/1) ─────
    sc_contract_ver               INTEGER          DEFAULT 2,
    sc_risk_horizon_bucket        TEXT             DEFAULT '',
    sc_hold_target_ms             BIGINT           DEFAULT 0,
    sc_alpha_half_life_ms         BIGINT           DEFAULT 0,
    sc_max_signal_age_ms          BIGINT           DEFAULT 0,
    sc_atr_age_ms                 BIGINT           DEFAULT 0,
    sc_atr_source                 TEXT             DEFAULT '',
    sc_atr_pct                    DOUBLE PRECISION DEFAULT 0,
    sc_vol_ratio_fast_slow        DOUBLE PRECISION DEFAULT 0,
    sc_vol_ratio_z                DOUBLE PRECISION DEFAULT 0,

    -- ── Config snapshot (migration 016) ───────────────────────────────────
    config_json                   JSONB,

    -- ── Signal Enforcement Metadata (migration 028 / live: BOOLEAN) ───────
    meta_enforce_cov_bucket       TEXT             DEFAULT '',
    meta_enforce_applied          BOOLEAN          DEFAULT FALSE,

    -- ── ATR Policy ext (fix_analytics_db.sql, cont'd) ─────────────────────
    atr_policy_source             TEXT             DEFAULT '',
    atr_restore_cert_id           TEXT             DEFAULT '',
    atr_policy_snapshot_json      JSONB,

    -- ── Phase 0.3: Horizon Contract & ATR Scalars (migration 043) ─────────
    horizon_contract              JSONB,
    horizon_bucket                TEXT             DEFAULT '',
    atr_tf_ms                     BIGINT           DEFAULT 0,

    -- ── Phase 2.4: Live Surface Selection ─────────────────────────────────
    live_surface_applied          BOOLEAN          DEFAULT FALSE,
    live_surface_reason_code      TEXT             DEFAULT '',
    baseline_sl_price             DOUBLE PRECISION DEFAULT 0,
    baseline_tp1_price            DOUBLE PRECISION DEFAULT 0,
    selected_sl_price             DOUBLE PRECISION DEFAULT 0,
    selected_tp1_price            DOUBLE PRECISION DEFAULT 0,

    -- ── ML v2: indicators projected from config_json (generated) ──────────
    ind_delta_z                   DOUBLE PRECISION GENERATED ALWAYS AS (((config_json -> 'indicators'::text) ->> 'delta_z'::text)::double precision) STORED,
    ind_obi                       DOUBLE PRECISION GENERATED ALWAYS AS (((config_json -> 'indicators'::text) ->> 'obi'::text)::double precision) STORED,
    ind_weak_progress             BOOLEAN          GENERATED ALWAYS AS (((config_json -> 'indicators'::text) ->> 'weak_progress'::text) = 'true'::text) STORED,
    ind_atr_th_bps                DOUBLE PRECISION GENERATED ALWAYS AS (((config_json -> 'indicators'::text) ->> 'atr_unified_th_bps'::text)::double precision) STORED,

    -- ── Trailing Surface Selection (migration 028 ext) ────────────────────
    trailing_surface_applied      BOOLEAN          DEFAULT FALSE,
    trailing_surface_reason_code  TEXT,
    baseline_trailing_offset_atr  DOUBLE PRECISION,
    selected_trailing_offset_atr  DOUBLE PRECISION,

    -- ── Strong Gate Outcome (migration 028) ───────────────────────────────
    strong_gate_ok                BOOLEAN,

    -- ── Gate veto reason (migration 058, 2026-05-22) ──────────────────────
    v_gate_reason                 TEXT
);

-- Trigger ts populator (preserves entry_ts/exit_ts on UPSERT, see migrations)
-- CREATE TRIGGER trg_populate_trades_closed_ts ... (defined by migration)

-- Hypertable on close time
SELECT create_hypertable('trades_closed', 'exit_ts', if_not_exists => TRUE);

-- Indexes for analytics (mirror live DB)
CREATE INDEX IF NOT EXISTS idx_trades_closed_symbol_exit
    ON trades_closed(symbol, exit_ts);
CREATE INDEX IF NOT EXISTS idx_trades_closed_source_symbol_exit
    ON trades_closed(source, symbol, exit_ts);
CREATE INDEX IF NOT EXISTS idx_trades_closed_entry_tag_exit
    ON trades_closed(entry_tag, exit_ts);
CREATE INDEX IF NOT EXISTS idx_trades_closed_sid
    ON trades_closed(sid);
CREATE INDEX IF NOT EXISTS idx_trades_closed_source_exit_ts
    ON trades_closed(source, exit_ts_ms DESC) WHERE exit_ts_ms > 0;
CREATE INDEX IF NOT EXISTS idx_trades_closed_sym_virtual_exit_ts
    ON trades_closed(symbol, is_virtual, exit_ts_ms DESC) WHERE is_virtual IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trades_closed_sym_virtual_ts
    ON trades_closed(symbol, is_virtual, exit_ts_ms DESC) WHERE is_virtual IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trades_closed_ml_v2
    ON trades_closed(exit_ts DESC, symbol, entry_tag)
    INCLUDE (r_multiple, ind_delta_z, ind_obi, ind_weak_progress, ind_atr_th_bps)
    WHERE r_multiple IS NOT NULL AND (tp1_hit = TRUE OR r_multiple > 0::double precision);

-- =========================================================================
-- trades_closed_p0 (post-trade features, reconciled 2026-05-17)
-- =========================================================================
-- One row per (order_id, exit_ts). Upserted by batch_trade_writer.py.
-- Stores microstructure + regime/scenario context captured at entry/exit.
-- =========================================================================
CREATE TABLE IF NOT EXISTS trades_closed_p0 (
    order_id                      TEXT NOT NULL,
    exit_ts                       TIMESTAMPTZ NOT NULL,
    exit_ts_ms                    BIGINT,

    scenario                      TEXT,
    regime                        TEXT,
    session                       TEXT,
    entry_reason                  TEXT,

    mae_bps                       DOUBLE PRECISION,
    mfe_bps                       DOUBLE PRECISION,
    time_to_mfe_ms                BIGINT,
    hold_ms                       BIGINT,

    spread_bps_at_entry           DOUBLE PRECISION,
    slippage_bps_est              DOUBLE PRECISION,
    book_age_ms                   BIGINT,

    features_json                 JSONB,

    created_at                    TIMESTAMPTZ DEFAULT now(),
    updated_at                    TIMESTAMPTZ DEFAULT now(),

    -- Mirrors of trades_closed flags for self-contained analytics joins
    is_virtual                    BOOLEAN          DEFAULT FALSE,
    meta_enforce_cov_bucket       TEXT             DEFAULT '',
    meta_enforce_applied          BOOLEAN          DEFAULT FALSE,

    trailing_surface_applied      BOOLEAN          DEFAULT FALSE,
    trailing_surface_reason_code  TEXT,
    baseline_trailing_offset_atr  DOUBLE PRECISION,
    selected_trailing_offset_atr  DOUBLE PRECISION,

    strong_gate_ok                BOOLEAN,

    -- ── Gate veto reason (migration 058, 2026-05-22) ──────────────────────
    v_gate_reason                 TEXT,

    PRIMARY KEY (order_id, exit_ts)
);

-- =========================================================================
-- signals (one row per emitted signal, reconciled 2026-05-17)
-- =========================================================================
CREATE TABLE IF NOT EXISTS signals (
    signal_id                     UUID PRIMARY KEY,
    ts_signal                     TIMESTAMPTZ NOT NULL,
    symbol                        TEXT NOT NULL,
    side                          TEXT NOT NULL,
    setup_type                    TEXT NOT NULL,
    price_at_signal               DOUBLE PRECISION NOT NULL,
    final_score                   DOUBLE PRECISION NOT NULL,
    atr_1m                        DOUBLE PRECISION,
    atr_5m                        DOUBLE PRECISION,
    tick_size                     DOUBLE PRECISION,
    contract_size                 DOUBLE PRECISION,
    extra_json                    JSONB DEFAULT '{}'::jsonb,
    session                       TEXT,
    regime                        TEXT,
    delta_spike_z                 DOUBLE PRECISION,
    obi                           DOUBLE PRECISION,
    weak_progress                 DOUBLE PRECISION,
    raw_ctx                       JSONB
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts        ON signals(symbol, ts_signal DESC);
CREATE INDEX IF NOT EXISTS idx_signals_setup_ts         ON signals(setup_type, ts_signal DESC);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_setup_ts  ON signals(symbol, setup_type, ts_signal DESC);

-- =========================================================================
-- Raw ticks (optional)
-- =========================================================================
CREATE TABLE IF NOT EXISTS ticks (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT,
    symbol      TEXT NOT NULL,
    ts_ms       BIGINT NOT NULL,
    ts          TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    price       DOUBLE PRECISION,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    side        TEXT,
    meta        JSONB
);
SELECT create_hypertable('ticks', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);

-- =========================================================================
-- Daily aggregates
-- =========================================================================
CREATE TABLE IF NOT EXISTS daily_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    source              TEXT,
    symbol              TEXT NOT NULL,

    trades_count        INTEGER,
    wins                INTEGER,
    losses              INTEGER,
    breakeven           INTEGER,

    pnl_net_sum         DOUBLE PRECISION,
    pnl_net_avg         DOUBLE PRECISION,
    pnl_net_std         DOUBLE PRECISION,

    expectancy_r        DOUBLE PRECISION,
    payoff_r            DOUBLE PRECISION,
    payoff_usd          DOUBLE PRECISION,
    kelly_r             DOUBLE PRECISION,

    wr                  DOUBLE PRECISION,
    sharpe              DOUBLE PRECISION,
    sortino             DOUBLE PRECISION,
    mdd_usd             DOUBLE PRECISION,

    wr_fixed            DOUBLE PRECISION,
    expectancy_fixed_r  DOUBLE PRECISION,
    payoff_fixed_r      DOUBLE PRECISION,
    payoff_fixed_usd    DOUBLE PRECISION,
    delta_expectancy_r  DOUBLE PRECISION,

    created_at          TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS daily_metrics_uniq
    ON daily_metrics(date, source, symbol);

-- =========================================================================
-- Entry tag aggregates
-- =========================================================================
CREATE TABLE IF NOT EXISTS entry_tag_metrics (
    id                              BIGSERIAL PRIMARY KEY,
    date                            DATE NOT NULL,
    source                          TEXT,
    symbol                          TEXT NOT NULL,
    entry_tag                       TEXT NOT NULL,

    trades_count                    INTEGER,
    wins                            INTEGER,
    losses                          INTEGER,
    breakeven                       INTEGER,

    pnl_net_sum                     DOUBLE PRECISION,
    pnl_net_avg                     DOUBLE PRECISION,
    expectancy_r                    DOUBLE PRECISION,
    payoff_r                        DOUBLE PRECISION,
    payoff_usd                      DOUBLE PRECISION,
    wr                              DOUBLE PRECISION,

    wr_fixed                        DOUBLE PRECISION,
    expectancy_fixed_r              DOUBLE PRECISION,
    payoff_fixed_r                  DOUBLE PRECISION,
    payoff_fixed_usd                DOUBLE PRECISION,
    delta_expectancy_r              DOUBLE PRECISION,

    giveback_avg_usd                DOUBLE PRECISION,
    giveback_avg_r                  DOUBLE PRECISION,
    giveback_avg_ratio              DOUBLE PRECISION,
    giveback_share                  DOUBLE PRECISION,

    missed_avg_usd                  DOUBLE PRECISION,
    missed_avg_r                    DOUBLE PRECISION,
    missed_avg_ratio                DOUBLE PRECISION,
    missed_share                    DOUBLE PRECISION,

    mfe_avg_r                       DOUBLE PRECISION,
    mae_avg_r                       DOUBLE PRECISION,

    trailing_share                  DOUBLE PRECISION,
    trailing_close_share            DOUBLE PRECISION,
    trailing_wr                     DOUBLE PRECISION,
    trailing_expectancy_r           DOUBLE PRECISION,
    trailing_expectancy_fixed_r     DOUBLE PRECISION,
    trailing_delta_expectancy_r     DOUBLE PRECISION,

    created_at                      TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS entry_tag_metrics_uniq
    ON entry_tag_metrics(date, source, symbol, entry_tag);

-- Optional policies (enable later as needed)
-- SELECT add_retention_policy('trades_closed', INTERVAL '90 days');
-- SELECT add_compression_policy('trades_closed', INTERVAL '7 days');
-- ALTER TABLE trades_closed SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol,source');
