CREATE TABLE IF NOT EXISTS atr_daily_triage_boards (
  board_id text PRIMARY KEY,
  day date NOT NULL,
  overall_status text NOT NULL,          -- GREEN | YELLOW | RED | BLACK
  sections_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_daily_triage_decisions (
  decision_id text PRIMARY KEY,
  board_id text NOT NULL,
  decision_type text NOT NULL,           -- NO_ACTION | WATCH | SAME_DAY_FIX | FREEZE_SCOPE | FREEZE_RELEASES | ROLLBACK_REVIEW | INCIDENT_OPEN
  actor text NOT NULL,
  reason_code text NOT NULL,
  decision_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_daily_triage_actions (
  action_id text PRIMARY KEY,
  board_id text NOT NULL,
  section text NOT NULL,
  owner text NOT NULL,
  priority text NOT NULL,                -- P0 | P1 | P2 | P3
  status text NOT NULL,                  -- open | in_progress | done | verified | dropped
  title text NOT NULL,
  reason_code text NOT NULL,
  due_at timestamptz NOT NULL,
  action_json jsonb NOT NULL,
  verification_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE OR REPLACE VIEW v_ops_daily_triage_board AS
SELECT
  day,
  overall_status,
  created_at
FROM atr_daily_triage_boards
ORDER BY day DESC;

CREATE OR REPLACE VIEW v_ops_daily_triage_actions AS
SELECT
  section,
  owner,
  priority,
  status,
  due_at,
  created_at
FROM atr_daily_triage_actions
ORDER BY
  CASE priority
    WHEN 'P0' THEN 1
    WHEN 'P1' THEN 2
    WHEN 'P2' THEN 3
    ELSE 4
  END,
  due_at ASC;
