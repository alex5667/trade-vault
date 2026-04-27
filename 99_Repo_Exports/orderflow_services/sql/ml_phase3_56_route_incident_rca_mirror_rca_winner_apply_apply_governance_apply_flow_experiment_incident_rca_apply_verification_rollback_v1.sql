BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results (
    id                              bigserial PRIMARY KEY,
    ts_ms                           bigint NOT NULL,
    decision                        text NOT NULL,
    reason_code                     text NOT NULL,
    current_mode                    text NOT NULL,
    target_mode                     text NOT NULL,
    rollback_mode                   text NOT NULL,
    observed_vertex_avg_usefulness  double precision NOT NULL,
    observed_local_avg_usefulness   double precision NOT NULL,
    observed_vertex_accepted_rate   double precision NOT NULL,
    observed_local_accepted_rate    double precision NOT NULL,
    observed_vertex_n               integer NOT NULL,
    observed_local_n                integer NOT NULL,
    verification_json               jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    rollback_mode      text NOT NULL,
    failed_target_mode text NOT NULL,
    applied            integer NOT NULL,
    journal_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_journal_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_journal(ts_ms DESC);

COMMIT;
