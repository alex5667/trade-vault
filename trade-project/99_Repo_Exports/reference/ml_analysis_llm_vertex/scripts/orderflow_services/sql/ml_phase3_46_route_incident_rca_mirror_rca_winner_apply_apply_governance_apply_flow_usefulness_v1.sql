BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_rollups (
    id                            bigserial PRIMARY KEY,
    ts_ms                         bigint NOT NULL,
    window_min                    integer NOT NULL,
    vertex_n                      integer NOT NULL,
    vertex_avg_quality            double precision NOT NULL,
    vertex_avg_usefulness         double precision NOT NULL,
    vertex_accepted_rate          double precision NOT NULL,
    vertex_low_usefulness_rate    double precision NOT NULL,
    local_n                       integer NOT NULL,
    local_avg_quality             double precision NOT NULL,
    local_avg_usefulness          double precision NOT NULL,
    local_accepted_rate           double precision NOT NULL,
    local_low_usefulness_rate     double precision NOT NULL,
    rollup_json                   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_rollups_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_decisions (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    current_bridge_mode text NOT NULL,
    target_bridge_mode text NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    decision_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_decisions(ts_ms DESC);

COMMIT;
