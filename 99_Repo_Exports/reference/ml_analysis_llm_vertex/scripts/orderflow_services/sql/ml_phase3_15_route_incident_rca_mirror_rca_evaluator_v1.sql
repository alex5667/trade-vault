BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_scorecards (
    id              bigserial PRIMARY KEY,
    arm             text NOT NULL,
    exposure_n      int NOT NULL,
    result_n        int NOT NULL,
    feedback_n      int NOT NULL,
    avg_quality     numeric(5,4) NOT NULL,
    avg_usefulness  numeric(5,4) NOT NULL,
    accepted_rate   numeric(5,4) NOT NULL,
    score           numeric(5,4) NOT NULL,
    eligible        boolean NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_scorecards_ts_arm
    ON llm_route_incident_rca_mirror_rca_scorecards(ts_ms DESC, arm);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_evaluator_decisions (
    id              bigserial PRIMARY KEY,
    decision        text NOT NULL,
    scorecards_json jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_evaluator_decisions_ts
    ON llm_route_incident_rca_mirror_rca_evaluator_decisions(ts_ms DESC);

COMMIT;
