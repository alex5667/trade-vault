-- Phase C.1 (P1 outcome-calibrated): hypertables for outcome-aware regime
-- promotion + per-decision trailing audit.
--
-- strategy_bucket_metrics — aggregated KPIs per (symbol×vol_regime×trend_regime×
-- liquidity_class×entry_profile×trail_profile). Источник правды для гейтов
-- "promote-to-enforce" в RegimeConditionalExecutionEngine.
--
-- trailing_decisions — per-event журнал решений orchestrator-а
-- (TRAILING_STARTED / SL_MOVED / DLQ_PUSH). Используется для отладки и
-- регрессионных проверок trailing behaviour.

-- ─────────────────────── strategy_bucket_metrics ────────────────────────────
CREATE TABLE IF NOT EXISTS strategy_bucket_metrics (
    ts                  TIMESTAMPTZ NOT NULL,
    symbol              TEXT        NOT NULL,
    vol_regime          TEXT        NOT NULL,
    trend_regime        TEXT        NOT NULL,
    liquidity_class     TEXT        NOT NULL DEFAULT 'na',
    entry_profile       TEXT        NOT NULL DEFAULT 'na',
    trail_profile       TEXT        NOT NULL DEFAULT 'na',
    n                   INTEGER     NOT NULL,
    win_rate            DOUBLE PRECISION,
    ev_r                DOUBLE PRECISION,
    avg_r               DOUBLE PRECISION,
    mfe_r_p50           DOUBLE PRECISION,
    mfe_r_p90           DOUBLE PRECISION,
    mae_r_p50           DOUBLE PRECISION,
    mae_r_p90           DOUBLE PRECISION,
    slippage_bps_p50    DOUBLE PRECISION,
    slippage_bps_p95    DOUBLE PRECISION,
    timeout_rate        DOUBLE PRECISION,
    adverse_proxy_p95   DOUBLE PRECISION,
    bootstrap_ci_low    DOUBLE PRECISION,
    bootstrap_ci_high   DOUBLE PRECISION,
    decision            TEXT        NOT NULL,  -- shadow|enforce_proposed|skip
    policy_hash         TEXT,
    profile_hash        TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

SELECT create_hypertable(
    'strategy_bucket_metrics', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS strategy_bucket_metrics_bucket_idx
    ON strategy_bucket_metrics (symbol, vol_regime, trend_regime, ts DESC);

CREATE INDEX IF NOT EXISTS strategy_bucket_metrics_decision_idx
    ON strategy_bucket_metrics (decision, ts DESC);

-- Retention: 180 дней. Promotion-сервис смотрит rolling window 14–30 дней;
-- держим больше для пост-mortem.
SELECT add_retention_policy('strategy_bucket_metrics', INTERVAL '180 days', if_not_exists => TRUE);

-- ─────────────────────── trailing_decisions ─────────────────────────────────
CREATE TABLE IF NOT EXISTS trailing_decisions (
    ts                  TIMESTAMPTZ NOT NULL,
    sid                 TEXT        NOT NULL,
    symbol              TEXT        NOT NULL,
    position_id         TEXT,
    event_type          TEXT        NOT NULL,   -- TRAILING_STARTED|SL_MOVED|DLQ_PUSH|EXIT
    profile             TEXT        NOT NULL,
    side                TEXT,
    tp_level            INTEGER,
    old_sl              DOUBLE PRECISION,
    new_sl              DOUBLE PRECISION,
    trail_distance      DOUBLE PRECISION,
    atr_value           DOUBLE PRECISION,
    atr_mult            DOUBLE PRECISION,
    regime_bucket       TEXT,
    reason_code         TEXT        NOT NULL,
    idempotency_key     TEXT        NOT NULL,
    policy_hash         TEXT,
    profile_hash        TEXT,
    schema_ver          INTEGER     NOT NULL DEFAULT 2,
    payload             JSONB,
    created_at          TIMESTAMPTZ DEFAULT now()
);

SELECT create_hypertable(
    'trailing_decisions', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Идемпотентность повторных событий (TP1_HIT при перезапуске listener-а).
CREATE UNIQUE INDEX IF NOT EXISTS trailing_decisions_idempotency_uidx
    ON trailing_decisions (idempotency_key, ts);

CREATE INDEX IF NOT EXISTS trailing_decisions_sid_idx
    ON trailing_decisions (sid, ts DESC);

CREATE INDEX IF NOT EXISTS trailing_decisions_event_idx
    ON trailing_decisions (event_type, ts DESC);

-- 90 дней — для разбора инцидентов хватит, остальное архивируется.
SELECT add_retention_policy('trailing_decisions', INTERVAL '90 days', if_not_exists => TRUE);
