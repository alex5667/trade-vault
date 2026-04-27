-- Update TTD Configuration Job
-- Calculates TTD quantiles from historical performance data
-- Should be run periodically (e.g., daily) to update expiry settings

-- Calculate TTD quantiles for signals that actually reached 1R target
INSERT INTO signal_ttd_config (symbol, setup_type, ttd_q50_bars, ttd_q75_bars, ttd_q90_bars, expiry_bars, updated_at)
SELECT
    symbol,
    setup_type,
    -- 50th percentile (median)
    percentile_disc(0.5) WITHIN GROUP (ORDER BY ttd_bars) AS ttd_q50_bars,
    -- 75th percentile (good signals reach target within this time)
    percentile_disc(0.75) WITHIN GROUP (ORDER BY ttd_bars) AS ttd_q75_bars,
    -- 90th percentile (very good signals)
    percentile_disc(0.9) WITHIN GROUP (ORDER BY ttd_bars) AS ttd_q90_bars,
    -- Use 75th percentile as default expiry (conservative approach)
    percentile_disc(0.75) WITHIN GROUP (ORDER BY ttd_bars) AS expiry_bars,
    now() as updated_at
FROM signal_performance
WHERE
    -- Only consider signals that actually reached the R target
    mfe_R >= 1.0
    -- Have valid TTD data
    AND ttd_bars IS NOT NULL
    -- Recent data (last 60 days)
    AND ts_signal >= now() - INTERVAL '60 days'
    -- Minimum sample size per group
    AND setup_type IN (
        SELECT setup_type
        FROM signal_performance
        WHERE ts_signal >= now() - INTERVAL '60 days'
        GROUP BY symbol, setup_type
        HAVING COUNT(*) >= 10  -- At least 10 signals per symbol/setup combination
    )
GROUP BY symbol, setup_type
-- Only update if we have enough data
HAVING COUNT(*) >= 10
ON CONFLICT (symbol, setup_type) DO UPDATE SET
    ttd_q50_bars = EXCLUDED.ttd_q50_bars,
    ttd_q75_bars = EXCLUDED.ttd_q75_bars,
    ttd_q90_bars = EXCLUDED.ttd_q90_bars,
    expiry_bars = EXCLUDED.expiry_bars,
    updated_at = now();

-- Log the update
DO $$
DECLARE
    updated_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO updated_count FROM signal_ttd_config WHERE updated_at >= now() - INTERVAL '1 minute';
    RAISE NOTICE 'Updated TTD config for % symbol/setup combinations', updated_count;
END $$;
