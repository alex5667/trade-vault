BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_bridge_decisions (
    id              bigserial PRIMARY KEY,
    bundle_id       uuid NOT NULL,
    ts_ms           bigint NOT NULL,
    decision        text NOT NULL,
    vertex_degraded boolean NOT NULL,
    severity        text NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_bridge_decisions_ts
    ON llm_route_incident_rca_mirror_rca_bridge_decisions(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_bridge_decisions_bundle
    ON llm_route_incident_rca_mirror_rca_bridge_decisions(bundle_id);

COMMIT;
