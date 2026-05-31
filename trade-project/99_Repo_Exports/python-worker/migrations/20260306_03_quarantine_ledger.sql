-- P8: quarantine / automated repair ledger

CREATE TABLE IF NOT EXISTS execution_quarantine_ledger (
  id BIGSERIAL PRIMARY KEY,
  sid TEXT NOT NULL,
  symbol TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  quarantine_key TEXT NOT NULL DEFAULT '',
  applied BOOLEAN NOT NULL DEFAULT TRUE,
  state_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  event_ts_ms BIGINT NOT NULL,
  created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_repair_runs (
  id BIGSERIAL PRIMARY KEY,
  run_kind TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  summary_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at_ms BIGINT NOT NULL,
  finished_at_ms BIGINT NOT NULL
);
