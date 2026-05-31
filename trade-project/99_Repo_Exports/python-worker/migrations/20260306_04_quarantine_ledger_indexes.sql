-- P8: indexes for quarantine / repair ledger

CREATE INDEX IF NOT EXISTS idx_execution_quarantine_ledger_sid_created
  ON execution_quarantine_ledger (sid, created_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_execution_quarantine_ledger_action_created
  ON execution_quarantine_ledger (action, created_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_execution_quarantine_ledger_state_jsonb_gin
  ON execution_quarantine_ledger USING GIN (state_jsonb);

CREATE INDEX IF NOT EXISTS idx_execution_repair_runs_kind_finished
  ON execution_repair_runs (run_kind, finished_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_execution_repair_runs_summary_jsonb_gin
  ON execution_repair_runs USING GIN (summary_jsonb);
