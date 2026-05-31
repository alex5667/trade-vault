-- migration: 20260530_04_order_execution_events.sql
-- Step 2 of Plan 3 — TCA execution lifecycle journal (DECISION → FILL → CLOSE).
--
-- Design rationale:
--   * Append-only event log; each row is one stage transition for one signal.
--   * Stages form an ordered set; v_order_execution_latency derives per-stage gaps.
--   * payload_json is JSONB so any stage-specific extras (broker order_id, retry
--     count, reject_reason text, fill px/qty) ride along without DDL.
--   * Hypertable on epoch_ms BIGINT — matches signal_outcome / signal_feature_snapshots.
--   * Designed for both real-broker and shadow-paper paths; shadow rows carry
--     stage='DECISION'/'SIGNAL_PUBLISHED' only and exit early. Real rows
--     continue through QUEUE_READ … FILL … CLOSE.
-- Added: Plan 3 / Step 2, 2026-05-30

CREATE TABLE IF NOT EXISTS order_execution_events (
    -- ── Partition key + identity ────────────────────────────────────────────
    ts_ms              BIGINT           NOT NULL,    -- epoch ms UTC
    sid                TEXT             NOT NULL,
    stage              TEXT             NOT NULL,    -- see Stages comment below
    seq                SMALLINT         NOT NULL DEFAULT 0,
                                                     -- monotonic per (sid, stage) for retries

    -- ── Symbol / side metadata (redundant, but spares joins) ────────────────
    symbol             TEXT             NOT NULL,
    side               SMALLINT         NOT NULL,    -- +1 long / -1 short
    venue              TEXT,                          -- binance, bybit, paper, …

    -- ── Order identifiers (NULL until assigned) ────────────────────────────
    client_order_id    TEXT,
    exchange_order_id  TEXT,

    -- ── Pricing + size (units are venue-native) ────────────────────────────
    px                 DOUBLE PRECISION,
    qty                DOUBLE PRECISION,
    notional_usd       DOUBLE PRECISION,

    -- ── Outcome of this stage ──────────────────────────────────────────────
    status             TEXT             NOT NULL,    -- ok, partial, reject, cancel, error, shadow
    reason_code        TEXT,                          -- machine-readable; e.g. risk_veto, broker_rate_limit
    latency_ms         DOUBLE PRECISION,              -- gap from previous stage (filled by writer)

    -- ── Stage-specific payload ──────────────────────────────────────────────
    payload_json       JSONB            NOT NULL DEFAULT '{}'::jsonb,

    created_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (ts_ms, sid, stage, seq)
);

-- Stages (string, not enum — allows additive evolution without DDL):
--   DECISION              — gate produced TAKE decision
--   SIGNAL_PUBLISHED      — XADD to signals stream
--   ORDER_QUEUE_XADD      — order intent enqueued for executor
--   GATEWAY_READ          — executor read the intent (XREADGROUP)
--   BROKER_SEND           — request sent to exchange API
--   BROKER_ACK            — exchange acked (order accepted)
--   FILL                  — fully filled
--   PARTIAL_FILL          — partially filled (status=partial)
--   CLOSE                 — position closed (TP/SL/timeout)
--   REJECT                — exchange rejected the order
--   CANCEL                — order cancelled (by us or exchange)

SELECT create_hypertable(
    'order_execution_events',
    'ts_ms',
    chunk_time_interval => 86400000,
    if_not_exists       => TRUE
);

SELECT set_integer_now_func('order_execution_events', 'now_ms', replace_if_exists => TRUE);

-- ── Indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS ix_oee_sid
    ON order_execution_events (sid, ts_ms);

CREATE INDEX IF NOT EXISTS ix_oee_stage_time
    ON order_execution_events (stage, ts_ms DESC);

CREATE INDEX IF NOT EXISTS ix_oee_sym_time
    ON order_execution_events (symbol, ts_ms DESC);

-- ── Retention + compression ──────────────────────────────────────────────────

SELECT add_retention_policy(
    'order_execution_events',
    drop_after    => 7776000000,   -- 90 days in ms
    if_not_exists => TRUE
);

ALTER TABLE order_execution_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,venue,stage'
);

SELECT add_compression_policy(
    'order_execution_events',
    compress_after => 604800000,   -- 7 days in ms
    if_not_exists  => TRUE
);

-- ── Derived view: per-signal stage latencies ─────────────────────────────────

CREATE OR REPLACE VIEW v_order_execution_latency AS
SELECT
    sid,
    MAX(symbol)                                                              AS symbol,
    MAX(venue)                                                               AS venue,
    MIN(ts_ms) FILTER (WHERE stage = 'DECISION')                             AS decision_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'SIGNAL_PUBLISHED')                     AS signal_published_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'ORDER_QUEUE_XADD')                     AS queue_xadd_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'GATEWAY_READ')                         AS gateway_read_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'BROKER_SEND')                          AS broker_send_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'BROKER_ACK')                           AS broker_ack_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'FILL')                                 AS fill_ts_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'CLOSE')                                AS close_ts_ms,

    -- Stage-to-stage deltas (NULL when either endpoint is missing)
    MIN(ts_ms) FILTER (WHERE stage = 'SIGNAL_PUBLISHED')
      - MIN(ts_ms) FILTER (WHERE stage = 'DECISION')                         AS decision_to_publish_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'GATEWAY_READ')
      - MIN(ts_ms) FILTER (WHERE stage = 'ORDER_QUEUE_XADD')                 AS queue_lag_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'BROKER_ACK')
      - MIN(ts_ms) FILTER (WHERE stage = 'BROKER_SEND')                      AS broker_rtt_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'FILL')
      - MIN(ts_ms) FILTER (WHERE stage = 'BROKER_ACK')                       AS ack_to_fill_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'FILL')
      - MIN(ts_ms) FILTER (WHERE stage = 'DECISION')                         AS decision_to_fill_ms,
    MIN(ts_ms) FILTER (WHERE stage = 'CLOSE')
      - MIN(ts_ms) FILTER (WHERE stage = 'FILL')                             AS hold_ms
FROM order_execution_events
GROUP BY sid;

COMMENT ON VIEW v_order_execution_latency IS
    'Per-signal lifecycle latencies; NULL means stage never recorded.';
