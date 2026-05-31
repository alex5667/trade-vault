BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_slo_rollups (
    id                    bigserial PRIMARY KEY,
    ts_ms                 bigint NOT NULL,
    window_min            integer NOT NULL,
    apply_requests        integer NOT NULL,
    applied_n             integer NOT NULL,
    verified_keep_n       integer NOT NULL,
    rollback_decisions_n  integer NOT NULL,
    rollback_applied_n    integer NOT NULL,
    apply_rate            double precision NOT NULL,
    verify_keep_rate      double precision NOT NULL,
    rollback_mttr_p50_sec double precision NOT NULL,
    rollback_mttr_p95_sec double precision NOT NULL,
    rollback_mttr_samples integer NOT NULL,
    reason_codes_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_slo_rollups_ts ON llm_governance_slo_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_retry_results (
    id                   bigserial PRIMARY KEY,
    ts_ms                bigint NOT NULL,
    event_key            text NOT NULL,
    decision             text NOT NULL,
    reason_code          text NOT NULL,
    attempts             integer NOT NULL,
    rollback_mode        text NOT NULL,
    rollback_primary_arm text NOT NULL,
    result_json          jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_retry_results_ts ON llm_governance_retry_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_escalations (
    id           bigserial PRIMARY KEY,
    ts_ms        bigint NOT NULL,
    severity     text NOT NULL,
    summary_json jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_escalations_ts ON llm_governance_escalations(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_incident_bundles (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    contour             text NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    trigger_reason_code text NOT NULL,
    bundle_json         jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_ts ON llm_governance_incident_bundles(ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_bundle_id ON llm_governance_incident_bundles(bundle_id);

CREATE TABLE IF NOT EXISTS llm_governance_apply_flow_rca_bridge_decisions (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    route               text NOT NULL,
    destination_stream  text NOT NULL,
    bundle_json         jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_flow_rca_bridge_decisions_ts ON llm_governance_apply_flow_rca_bridge_decisions(ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_flow_rca_bridge_decisions_bundle_id ON llm_governance_apply_flow_rca_bridge_decisions(bundle_id);

COMMIT;
