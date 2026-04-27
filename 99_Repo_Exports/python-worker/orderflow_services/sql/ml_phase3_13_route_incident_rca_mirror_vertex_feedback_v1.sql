BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_vertex_rca_results (
    id              bigserial PRIMARY KEY,
    request_id      uuid NOT NULL,
    bundle_id       uuid NOT NULL,
    result_json     jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_vertex_rca_results_ts
    ON llm_route_incident_rca_mirror_vertex_rca_results(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_vertex_rca_results_bundle
    ON llm_route_incident_rca_mirror_vertex_rca_results(bundle_id);

COMMIT;
