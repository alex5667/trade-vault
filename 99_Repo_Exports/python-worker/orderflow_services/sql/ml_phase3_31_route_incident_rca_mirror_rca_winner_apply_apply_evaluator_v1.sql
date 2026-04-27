BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_scorecards (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    arm                 text NOT NULL,
    exposure_n          integer NOT NULL,
    result_n            integer NOT NULL,
    feedback_n          integer NOT NULL,
    avg_quality         double precision NOT NULL,
    avg_usefulness      double precision NOT NULL,
    accepted_rate       double precision NOT NULL,
    result_coverage     double precision NOT NULL,
    feedback_coverage   double precision NOT NULL,
    coverage_multiplier double precision NOT NULL,
    score_raw           double precision NOT NULL,
    score               double precision NOT NULL,
    eligible            integer NOT NULL,
    reason_codes_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_scorecards_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_scorecards(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions (
    id               bigserial PRIMARY KEY,
    ts_ms            bigint NOT NULL,
    decision         text NOT NULL,
    reason_code      text NOT NULL,
    winner_arm       text NOT NULL,
    incumbent_arm    text NOT NULL,
    winner_score     double precision NOT NULL,
    incumbent_score  double precision NOT NULL,
    decision_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions(ts_ms DESC);

COMMIT;
