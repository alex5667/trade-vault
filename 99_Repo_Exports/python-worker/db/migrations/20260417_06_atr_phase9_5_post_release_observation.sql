CREATE TABLE IF NOT EXISTS atr_post_release_observations (
  observation_id text PRIMARY KEY,
  change_id text NOT NULL,
  change_class text NOT NULL,
  target_scope text NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL,
  observation_until timestamptz NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_pro_change_id ON atr_post_release_observations(change_id);

CREATE TABLE IF NOT EXISTS atr_post_release_checks (
  check_id text PRIMARY KEY,
  observation_id text NOT NULL REFERENCES atr_post_release_observations(observation_id) ON DELETE CASCADE,
  check_name text NOT NULL,
  status text NOT NULL,
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_prc_obs_id ON atr_post_release_checks(observation_id);

CREATE TABLE IF NOT EXISTS atr_promotion_holds (
  hold_id text PRIMARY KEY,
  observation_id text NOT NULL REFERENCES atr_post_release_observations(observation_id) ON DELETE CASCADE,
  scope_value text NOT NULL,
  hold_reason_code text NOT NULL,
  severity text NOT NULL,
  status text NOT NULL,
  hold_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  cleared_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_ph_obs_id ON atr_promotion_holds(observation_id);
CREATE INDEX IF NOT EXISTS idx_atr_ph_scope ON atr_promotion_holds(scope_value);
CREATE INDEX IF NOT EXISTS idx_atr_ph_status ON atr_promotion_holds(status);

CREATE OR REPLACE VIEW v_ops_post_release_observation_board AS
SELECT
  change_id,
  change_class,
  target_scope,
  status,
  started_at,
  observation_until
FROM atr_post_release_observations
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW v_ops_promotion_hold_board AS
SELECT
  scope_value,
  hold_reason_code,
  severity,
  status,
  created_at
FROM atr_promotion_holds
WHERE status = 'active'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  created_at DESC;
