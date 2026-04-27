-- P4.8: Indexes for risk_mismatch_quarantine_ledger and risk_mismatch_summary_mv.
-- Unique index on (window_name, tier) is required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS risk_mismatch_summary_mv_window_tier_idx ON risk_mismatch_summary_mv (window_name, tier);
CREATE INDEX IF NOT EXISTS risk_mismatch_quarantine_ledger_sid_ts_idx ON risk_mismatch_quarantine_ledger (sid, created_ts_ms DESC) WHERE sid IS NOT NULL;
CREATE INDEX IF NOT EXISTS risk_mismatch_quarantine_ledger_symbol_ts_idx ON risk_mismatch_quarantine_ledger (symbol, created_ts_ms DESC);
