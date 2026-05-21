-- Migration 20260520_04: Backfill position_events from trades_closed
-- Reason: position_events was empty; conf_score_weight_tuning_bundle reads from it.
-- Safe to re-run: ON CONFLICT DO NOTHING.
-- After this backfill, ongoing writes should be added to analytics_db.py.

INSERT INTO position_events (
    ts_ms,
    ts,
    position_id,
    sid,
    symbol,
    event_type,
    payload_json,
    meta_json,
    stream_id,
    inserted_at
)
SELECT
    tc.exit_ts_ms,
    tc.exit_ts,
    tc.id::text,
    tc.sid,
    tc.symbol,
    'POSITION_CLOSED',
    jsonb_build_object(
        'sid',          tc.sid,
        'symbol',       tc.symbol,
        'direction',    tc.direction,
        'ts_ms',        tc.exit_ts_ms,
        'event_type',   'POSITION_CLOSED',
        -- canonical names expected by common/ml_labeling.py
        'r_mult',       tc.r_multiple,
        'pnl',          tc.pnl_net,
        'pnl_net',      tc.pnl_net,
        'risk_usd',     tc.notional_usd,
        'reason',       tc.close_reason,
        'reason_raw',   COALESCE(tc.close_reason_raw, tc.close_reason),
        'close_reason', tc.close_reason,
        'is_virtual',   CASE WHEN tc.is_virtual THEN 1 ELSE 0 END,
        'source',       tc.source
    ),
    '{}'::jsonb,
    'backfill:trades_closed:' || tc.id::text,
    NOW()
FROM trades_closed tc
WHERE tc.sid IS NOT NULL
  AND tc.sid != ''
ON CONFLICT DO NOTHING;

-- Verify
SELECT COUNT(*) AS position_events_after_backfill FROM position_events;
