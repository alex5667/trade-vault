BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_vertex_rca_results (
    result_id    varchar(255) PRIMARY KEY,
    request_id   varchar(255) NOT NULL,
    provider     varchar(50) NOT NULL,
    severity     varchar(50) NOT NULL,
    result_json  jsonb NOT NULL,
    ts_ms        bigint NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_vertex_rca_res_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_vertex_rca_results(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_vertex_rca_res_req
    ON llm_route_incident_rca_mirror_rca_winner_apply_vertex_rca_results(request_id);

COMMIT;
