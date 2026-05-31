-- of_gate ok_rate rollup template (v1)
-- Safe division with NULL for no-data windows. Designed for TimescaleDB/Postgres.
-- Replace :bucket_interval, :start_ms, :end_ms with your values.
--
-- Usage:
--   SELECT * FROM of_gate_ok_rate_rollup
--   WHERE bucket >= NOW() - INTERVAL '1 hour';

WITH raw AS (
    SELECT
        time_bucket(':bucket_interval'::interval, to_timestamp(ts_ms / 1000.0)) AS bucket,
        symbol,
        scenario_v4,
        -- Cast to int safely (Redis stores as string)
        (ok::int)        AS ok_val,
        (ok_soft::int)   AS ok_soft_val
    FROM of_gate_metrics_v1
    WHERE
        ts_ms BETWEEN :start_ms AND :end_ms
        AND ok IN ('0', '1')
        AND ok_soft IN ('0', '1')
),
agg AS (
    SELECT
        bucket,
        symbol,
        scenario_v4,
        COUNT(*)                                                AS n,
        SUM(ok_val)                                            AS ok_hard,
        SUM(ok_val) + SUM(ok_soft_val)                        AS ok_any,
        SUM(ok_soft_val)                                       AS ok_soft
    FROM raw
    GROUP BY bucket, symbol, scenario_v4
)
SELECT
    bucket,
    symbol,
    scenario_v4,
    n,
    ok_hard,
    ok_any,
    ok_soft,
    -- NULL when n = 0 (no eligible rows) instead of 0/0 integer division error
    CASE WHEN n > 0 THEN ok_hard::float / n::float ELSE NULL END     AS ok_rate_strict,
    CASE WHEN n > 0 THEN ok_any::float  / n::float ELSE NULL END     AS ok_rate_soft,
    CASE WHEN ok_any > 0 THEN ok_soft::float / ok_any::float ELSE NULL END AS soft_share
FROM agg
ORDER BY bucket DESC, n DESC;
