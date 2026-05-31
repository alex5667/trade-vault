BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_scorecards (
    id                bigserial PRIMARY KEY,
    decision_id       varchar(255) NOT NULL,
    arm               varchar(100) NOT NULL,
    exposure_n        int NOT NULL,
    result_n          int NOT NULL,
    feedback_n        int NOT NULL,
    avg_quality       float8 NOT NULL,
    avg_usefulness    float8 NOT NULL,
    accepted_rate     float8 NOT NULL,
    result_coverage   float8 NOT NULL,
    feedback_coverage float8 NOT NULL,
    score             float8 NOT NULL,
    eligible          int NOT NULL,
    ts_ms             bigint NOT NULL,
    created_at        timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_scorecards_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_scorecards(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_scorecards_did
    ON llm_route_incident_rca_mirror_rca_winner_apply_scorecards(decision_id);


CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_evaluator_decisions (
    id               bigserial PRIMARY KEY,
    decision_id      varchar(255) NOT NULL,
    recommendation   varchar(100) NOT NULL,
    winner_arm       varchar(100) NOT NULL,
    incumbent_arm    varchar(100) NOT NULL,
    score_margin     float8 NOT NULL,
    ts_ms            bigint NOT NULL,
    created_at       timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_evaluator_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_evaluator_decisions(ts_ms DESC);

COMMIT;
