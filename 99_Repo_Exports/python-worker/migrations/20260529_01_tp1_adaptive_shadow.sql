-- Migration: create tp1_adaptive_shadow hypertable
-- Added in Plan 3 (AdaptiveTP1Policy v1), migration 20260529_01, 2026-05-29
--
-- Purpose: record SHADOW/PAPER/ENFORCE decisions of AdaptiveTP1Policy so we can
-- replay TP1_R argmax_EV decisions against realised outcomes (mfe/mae/close_reason).
--
-- Source:
--   producer: signals/level_enricher.py → AdaptiveTP1Decision (ctx.tp1_adaptive_*)
--   ingest: separate persister (tp1_adaptive_shadow_persister, follow-up task)
--
-- NOTE: this migration ONLY creates the table — no producer/ingestor wired yet.
-- The table stays empty until the persister is deployed; signal pipeline is unaffected.

CREATE TABLE IF NOT EXISTS tp1_adaptive_shadow (
    ts_ms                   BIGINT NOT NULL,
    -- ts is materialised at INSERT time by the BEFORE INSERT trigger
    -- (TimescaleDB forbids generated columns as the partitioning dimension).
    -- Persister sends ts_ms only; trigger sets ts := to_timestamp(ts_ms/1000).
    ts                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    sid                     TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    kind                    TEXT NOT NULL,
    side                    TEXT NOT NULL,
    regime                  TEXT,
    session                 TEXT,

    entry_price             DOUBLE PRECISION NOT NULL,
    sl_price                DOUBLE PRECISION NOT NULL,

    baseline_tp1_price      DOUBLE PRECISION NOT NULL,
    baseline_tp1_rr         DOUBLE PRECISION NOT NULL,

    adaptive_tp1_price      DOUBLE PRECISION,
    adaptive_tp1_rr         DOUBLE PRECISION,

    p_hit_baseline          DOUBLE PRECISION,
    p_hit_adaptive          DOUBLE PRECISION,

    ev_baseline_r           DOUBLE PRECISION,
    ev_adaptive_r           DOUBLE PRECISION,
    ev_delta_r              DOUBLE PRECISION,
    cost_r                  DOUBLE PRECISION,

    spread_bps              DOUBLE PRECISION,
    slippage_bps            DOUBLE PRECISION,
    fee_bps                 DOUBLE PRECISION,
    samples                 INTEGER,

    reason_code             TEXT NOT NULL,
    mode                    TEXT NOT NULL,

    -- realised (joined later via signal_outcome_join / trades_closed)
    realized_close_reason   TEXT,
    realized_pnl_r          DOUBLE PRECISION,
    realized_mfe_r          DOUBLE PRECISION,
    realized_mae_r          DOUBLE PRECISION,
    realized_hit_baseline   BOOLEAN,
    realized_hit_adaptive   BOOLEAN,

    model_ver               TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- PK uses ts (partition dimension) + sid. BEFORE INSERT trigger materialises
    -- ts from ts_ms, so the persister only needs to send ts_ms.
    PRIMARY KEY (ts, sid)
);

-- BEFORE INSERT trigger keeps ts in sync with ts_ms so the persister only
-- needs to send ts_ms. Idempotent: replaces existing function.
CREATE OR REPLACE FUNCTION tp1_adaptive_shadow_set_ts()
RETURNS trigger AS $$
BEGIN
    -- Always derive ts from ts_ms; persister never sends ts.
    NEW.ts := to_timestamp(NEW.ts_ms / 1000.0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_tp1_adaptive_shadow_set_ts ON tp1_adaptive_shadow;
CREATE TRIGGER trg_tp1_adaptive_shadow_set_ts
    BEFORE INSERT ON tp1_adaptive_shadow
    FOR EACH ROW EXECUTE FUNCTION tp1_adaptive_shadow_set_ts();

-- Timescale hypertable (idempotent)
SELECT create_hypertable('tp1_adaptive_shadow', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS tp1_adaptive_shadow_symbol_ts_idx
    ON tp1_adaptive_shadow (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS tp1_adaptive_shadow_reason_idx
    ON tp1_adaptive_shadow (reason_code, ts DESC);

CREATE INDEX IF NOT EXISTS tp1_adaptive_shadow_mode_ts_idx
    ON tp1_adaptive_shadow (mode, ts DESC);

-- 30d retention for shadow-stage data
SELECT add_retention_policy('tp1_adaptive_shadow', INTERVAL '30 days', if_not_exists => TRUE);
