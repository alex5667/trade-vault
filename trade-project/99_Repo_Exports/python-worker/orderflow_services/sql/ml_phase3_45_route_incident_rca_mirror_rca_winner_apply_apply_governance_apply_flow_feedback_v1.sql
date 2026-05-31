BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results (
    id            bigserial PRIMARY KEY,
    request_id    text NOT NULL,
    bundle_id     text NOT NULL,
    ts_ms         bigint NOT NULL,
    severity      text NOT NULL,
    provider_mode text NOT NULL,
    result_json   jsonb NOT NULL,
    request_json  jsonb NOT NULL,
    bundle_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback (
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
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback_rollups (
    id               bigserial PRIMARY KEY,
    ts_ms            bigint NOT NULL,
    window_min       integer NOT NULL,
    n                integer NOT NULL,
    avg_quality      double precision NOT NULL,
    avg_usefulness   double precision NOT NULL,
    accepted_rate    double precision NOT NULL,
    low_quality_rate double precision NOT NULL,
    rollup_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback_rollups_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_governance_decisions (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    target_bridge_mode text NOT NULL,
    decision_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_governance_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_governance_decisions(ts_ms DESC);

COMMIT;
