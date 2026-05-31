CREATE TABLE IF NOT EXISTS atr_drift_governance_events (
  event_id text PRIMARY KEY,
  drift_family text NOT NULL,            -- feature | decision | execution_cost | protective | config_surface | dataset_repr
  scope_value text NOT NULL,
  severity text NOT NULL,                -- warn | error | critical
  status text NOT NULL,                  -- open | acknowledged | refresh_requested | resolved
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_dataset_refresh_requests (
  request_id text PRIMARY KEY,
  dataset_class text NOT NULL,
  scope_json jsonb NOT NULL,
  trigger_event_id text NOT NULL,
  status text NOT NULL,                  -- requested | building | review | approved | activated | rejected
  owner text NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_dataset_baseline_validity (
  dataset_id text PRIMARY KEY,           -- references atr_golden_datasets(dataset_id)
  valid_from timestamptz NOT NULL,
  valid_until timestamptz NOT NULL,
  status text NOT NULL,                  -- valid | expiring | expired
  summary_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Note: atr_golden_dataset_reviews already exists in replay_certification.sql
-- We add a specific table for REFRESH reviews if needed, or reuse the existing one.
-- User suggested atr_dataset_refresh_reviews, let's add it for explicit refresh flow.
CREATE TABLE IF NOT EXISTS atr_dataset_refresh_reviews (
  review_id text PRIMARY KEY,
  request_id text NOT NULL,              -- references atr_dataset_refresh_requests(request_id)
  reviewer text NOT NULL,
  status text NOT NULL,                  -- approved | rejected | refresh_required
  review_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Views for Auditor/Operator
CREATE OR REPLACE VIEW v_ops_drift_governance_board AS
SELECT
  drift_family,
  scope_value,
  severity,
  status,
  created_at,
  reason_code
FROM atr_drift_governance_events
WHERE status <> 'resolved'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  created_at DESC;

CREATE OR REPLACE VIEW v_ops_dataset_refresh_board AS
SELECT
  dataset_class,
  status,
  owner,
  created_at,
  activated_at
FROM atr_dataset_refresh_requests
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_ops_dataset_validity_board AS
SELECT
  dataset_id,
  status,
  valid_from,
  valid_until,
  updated_at
FROM atr_dataset_baseline_validity
ORDER BY valid_until ASC;
