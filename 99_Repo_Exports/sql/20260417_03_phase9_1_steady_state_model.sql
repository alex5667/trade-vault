CREATE TABLE IF NOT EXISTS atr_weekly_operating_scorecards (
    scorecard_id text PRIMARY KEY,
    week_start date NOT NULL,
    week_end date NOT NULL,
    overall_status text NOT NULL, -- GO | GO_WITH_CONSTRAINTS | HOLD | FREEZE_ESCALATION | ROLLBACK_REVIEW_REQUIRED
    domains_json jsonb NOT NULL,
    summary_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_weekly_review_decisions (
    decision_id text PRIMARY KEY,
    scorecard_id text NOT NULL,
    decision_type text NOT NULL, -- GO | GO_WITH_CONSTRAINTS | HOLD | FREEZE_ESCALATION | ROLLBACK_REVIEW_REQUIRED
    actor text NOT NULL,
    reason_code text NOT NULL,
    decision_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_weekly_action_items (
    action_id text PRIMARY KEY,
    scorecard_id text NOT NULL,
    domain text NOT NULL,
    owner text NOT NULL,
    priority text NOT NULL, -- P0 | P1 | P2 | P3
    status text NOT NULL, -- open | in_progress | done | verified | dropped
    title text NOT NULL,
    reason_code text NOT NULL,
    due_at timestamptz NOT NULL,
    action_json jsonb NOT NULL,
    verification_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE OR REPLACE VIEW v_ops_weekly_scorecard_board AS
SELECT 
    week_start, 
    week_end, 
    overall_status, 
    created_at 
FROM atr_weekly_operating_scorecards 
ORDER BY week_start DESC;

CREATE OR REPLACE VIEW v_ops_weekly_action_board AS
SELECT 
    domain, 
    owner, 
    priority, 
    status, 
    due_at, 
    created_at 
FROM atr_weekly_action_items 
ORDER BY 
    CASE priority 
        WHEN 'P0' THEN 1 
        WHEN 'P1' THEN 2 
        WHEN 'P2' THEN 3 
        ELSE 4 
    END, 
    due_at ASC;
