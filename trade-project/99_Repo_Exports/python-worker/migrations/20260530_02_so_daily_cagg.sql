-- migration: 20260530_02_so_daily_cagg.sql
-- Phase 0: so_daily continuous aggregate — daily OOS performance metrics.
--
-- Refresh policy: every 15 min, lag 1 h (avoid touching freshly-compressed chunks).
-- Query pattern: heavy analytics ONLY via so_daily CAGG, never scan raw signal_outcome
--               for time-series queries — keeps ingest path isolated.
-- Robust stats: win_rate / avg_r / median_r (p50) over labelled records only.
-- Added: Phase 0, 2026-05-30

CREATE MATERIALIZED VIEW IF NOT EXISTS so_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(86400000, decision_time_ms)             AS bucket_ms,
    symbol,
    source,
    COALESCE(regime, 'na')                              AS regime,

    count(*)                                            AS n,
    count(*) FILTER (WHERE label IS NOT NULL)           AS n_resolved,
    count(*) FILTER (WHERE label = 1)                   AS wins,
    count(*) FILTER (WHERE label = -1)                  AS losses,
    count(*) FILTER (WHERE label = 0)                   AS timeouts,

    -- Precision / payoff
    CASE WHEN count(*) FILTER (WHERE label IS NOT NULL) > 0
         THEN count(*) FILTER (WHERE label = 1)::float /
              count(*) FILTER (WHERE label IS NOT NULL)::float
         ELSE NULL
    END                                                 AS win_rate,

    avg(realized_r) FILTER (WHERE label IS NOT NULL)    AS avg_r,

    -- Robust: median is more stable than mean for heavy-tailed R distributions
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY realized_r
    ) FILTER (WHERE label IS NOT NULL)                  AS median_r,

    -- Excursion stats for TP/TTL tuning
    avg(mfe_r) FILTER (WHERE label IS NOT NULL)         AS avg_mfe_r,
    avg(mae_r) FILTER (WHERE label IS NOT NULL)         AS avg_mae_r,

    -- Calibration quality
    avg(calib_prob) FILTER (WHERE calib_prob IS NOT NULL) AS avg_calib_prob,

    -- Execution quality (populated by Phase 4)
    avg(exec_slippage_bps) FILTER (WHERE exec_slippage_bps IS NOT NULL) AS avg_slip_bps,

    -- Resolution lag health
    avg(resolved_time_ms - decision_time_ms)
        FILTER (WHERE resolved_time_ms IS NOT NULL)     AS avg_resolution_lag_ms

FROM signal_outcome
WHERE label IS NOT NULL
GROUP BY 1, 2, 3, 4;

-- Refresh every 15 min; always refresh from 24 h back to catch late-arriving resolutions
SELECT add_continuous_aggregate_policy(
    'so_daily',
    start_offset      => NULL,          -- refresh from the beginning of unrefreshed data
    end_offset        => 3600000,       -- don't touch data newer than 1 h (avoids compression conflicts)
    schedule_interval => INTERVAL '15 min',
    if_not_exists     => TRUE
);
