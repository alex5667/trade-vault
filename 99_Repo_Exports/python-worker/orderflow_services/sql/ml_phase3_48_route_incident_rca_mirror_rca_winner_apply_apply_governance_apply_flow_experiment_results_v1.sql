BEGIN;

CREATE TABLE IF NOT EXISTS llm_rca_gov_apply_flow_exp_res (
    id            bigserial PRIMARY KEY,
    request_id    text NOT NULL,
    bundle_id     text NOT NULL,
    experiment_arm text NOT NULL,
    ts_ms         bigint NOT NULL,
    severity      text NOT NULL,
    provider_mode text NOT NULL,
    result_json   jsonb NOT NULL,
    request_json  jsonb NOT NULL,
    bundle_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_rca_gov_apply_flow_exp_res_ts
    ON llm_rca_gov_apply_flow_exp_res(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_rca_gov_apply_flow_exp_feedback (
    id               bigserial PRIMARY KEY,
    request_id       text NOT NULL,
    bundle_id        text NOT NULL,
    ts_ms            bigint NOT NULL,
    quality_score    double precision NOT NULL,
    usefulness_score double precision NOT NULL,
    accepted         integer NOT NULL,
    reason_code      text NOT NULL,
    feedback_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_rca_gov_apply_flow_exp_fb_ts
    ON llm_rca_gov_apply_flow_exp_feedback(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_rca_gov_apply_flow_exp_scorec (
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
    scorecard_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_rca_gov_apply_flow_exp_sc_ts
    ON llm_rca_gov_apply_flow_exp_scorec(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_rca_gov_apply_flow_exp_win_dec (
    id           bigserial PRIMARY KEY,
    ts_ms        bigint NOT NULL,
    decision     text NOT NULL,
    reason_code  text NOT NULL,
    winner_arm   text NOT NULL,
    decision_json jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_rca_gov_apply_flow_exp_wd_ts
    ON llm_rca_gov_apply_flow_exp_win_dec(ts_ms DESC);

COMMIT;
