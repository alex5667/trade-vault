CREATE OR REPLACE VIEW execution_journal AS
SELECT 
    sid,
    symbol,
    action AS side,
    fsm_state,
    status,
    (state_jsonb->>'entry_px')::numeric AS entry_price,
    created_at_ms AS entry_ts_ms,
    updated_at_ms,
    position_mode,
    position_side,
    venue,
    state_jsonb
FROM execution_orders;
