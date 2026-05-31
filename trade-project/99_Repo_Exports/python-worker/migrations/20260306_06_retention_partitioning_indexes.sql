CREATE INDEX IF NOT EXISTS execution_order_events_archive_sid_idx
  ON execution_order_events_archive (sid, event_ts_ms DESC);
-- FIX: колонка называется created_at_ms, не created_ts_ms (см. схему execution_quarantine_ledger)
CREATE INDEX IF NOT EXISTS execution_quarantine_ledger_archive_sid_idx
  ON execution_quarantine_ledger_archive (sid, created_at_ms DESC);
