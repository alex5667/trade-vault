CREATE TABLE IF NOT EXISTS llm_operator_rca_experiment_exposures (
  request_id text NOT NULL,
  experiment_id text NOT NULL,
  arm text NOT NULL,
  provider text,
  model_name text,
  prompt_version text,
  policy_version text,
  ts_ms bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (request_id, experiment_id)
);

CREATE TABLE IF NOT EXISTS llm_operator_rca_experiment_winner_decisions (
  experiment_id text NOT NULL,
  ts_ms bigint NOT NULL,
  decision text NOT NULL,
  winning_arm text,
  winning_provider text,
  winning_model_name text,
  winning_prompt_version text,
  winning_score double precision,
  runner_up_arm text,
  runner_up_score double precision,
  delta_score double precision,
  reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (experiment_id, ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_experiment_winner_decisions_exp_ts
  ON llm_operator_rca_experiment_winner_decisions (experiment_id, ts_ms DESC);
