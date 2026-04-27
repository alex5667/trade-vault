BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results (
    id           bigserial PRIMARY KEY,
    request_id   varchar(255) NOT NULL,
    bundle_id    varchar(255) NOT NULL,
    result_json  jsonb NOT NULL,
    ts_ms        bigint NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_w_a_a_vrr_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_w_a_a_vrr_bundle
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results(bundle_id);

COMMIT;
