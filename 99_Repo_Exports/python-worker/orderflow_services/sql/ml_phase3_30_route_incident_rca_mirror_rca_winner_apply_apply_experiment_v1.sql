BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_decisions (
    id                bigserial PRIMARY KEY,
    bundle_id         text NOT NULL,
    ts_ms             bigint NOT NULL,
    trigger_type      text NOT NULL,
    trigger_severity  text NOT NULL,
    decision          text NOT NULL,
    reason_code       text NOT NULL,
    primary_arm       text NOT NULL,
    shadow_arms_json  jsonb NOT NULL,
    bundle_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures (
    id               bigserial PRIMARY KEY,
    bundle_id        text NOT NULL,
    ts_ms            bigint NOT NULL,
    trigger_type     text NOT NULL,
    trigger_severity text NOT NULL,
    arm              text NOT NULL,
    is_primary       integer NOT NULL,
    mode             text NOT NULL,
    exposure_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures(ts_ms DESC);

COMMIT;
