-- Migration 20260524_02: News events audit + gate decisions hypertables.
-- Purpose: full audit trail for news ingestion and gate hard/soft blocks.
-- Retention: 180 days.

-- ── news_events ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS news_events (
    ts              TIMESTAMPTZ     NOT NULL,
    event_id        TEXT            NOT NULL,
    source_uid      TEXT,
    source          TEXT,
    title           TEXT,
    url             TEXT,
    event_type      TEXT,
    grade_id        INT,
    risk            TEXT,
    sentiment       TEXT,
    confidence      DOUBLE PRECISION,
    published_ts_ms BIGINT,
    asof_ts_ms      BIGINT,
    expires_ms      BIGINT,
    raw             JSONB,
    PRIMARY KEY (ts, event_id)
);

SELECT create_hypertable('news_events', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_news_events_event_id   ON news_events(event_id);
CREATE INDEX IF NOT EXISTS idx_news_events_type_ts    ON news_events(event_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_news_events_grade_ts   ON news_events(grade_id, ts DESC);

SELECT add_retention_policy('news_events', INTERVAL '180 days', if_not_exists => TRUE);

-- ── news_gate_decisions ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS news_gate_decisions (
    ts              TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    event_id        TEXT,
    action          TEXT            NOT NULL,
    hard_block      BOOLEAN         NOT NULL,
    reason_code     TEXT,
    risk_factor_bps INT,
    mode            TEXT,
    latency_us      INT,
    meta            JSONB,
    PRIMARY KEY (ts, symbol)
);

SELECT create_hypertable('news_gate_decisions', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_news_gate_symbol_ts
    ON news_gate_decisions(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_news_gate_reason_ts
    ON news_gate_decisions(reason_code, ts DESC);

SELECT add_retention_policy('news_gate_decisions', INTERVAL '180 days', if_not_exists => TRUE);
