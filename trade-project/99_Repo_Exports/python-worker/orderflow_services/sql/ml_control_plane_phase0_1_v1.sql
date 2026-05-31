-- Phase 0.1 additions for scanner_infra-only ML control plane.

CREATE INDEX IF NOT EXISTS idx_ml_training_runs_family_ts
  ON ml_training_runs (family, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_ml_model_runtime_1m_model_ts
  ON ml_model_runtime_1m (model_id, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_ml_model_runtime_1m_symbol_ts
  ON ml_model_runtime_1m (symbol, ts_ms DESC);
