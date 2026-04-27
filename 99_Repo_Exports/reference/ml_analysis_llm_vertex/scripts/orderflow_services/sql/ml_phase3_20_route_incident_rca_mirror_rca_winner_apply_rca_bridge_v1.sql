BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions (
    id           bigserial PRIMARY KEY,
    bundle_id    varchar(255) NOT NULL,
    decision     varchar(50) NOT NULL,
    bundle_json  jsonb NOT NULL,
    severity     varchar(50) NOT NULL,
    ts_ms        bigint NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions_bundle
    ON llm_route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions(bundle_id);

COMMIT;
