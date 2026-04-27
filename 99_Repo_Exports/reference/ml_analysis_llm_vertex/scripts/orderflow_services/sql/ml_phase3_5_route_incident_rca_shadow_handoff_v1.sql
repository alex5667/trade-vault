BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_shadow_handoff_decisions (
    id                   bigserial PRIMARY KEY,
    request_id           text NOT NULL,
    incident_id          text NOT NULL,
    ts_ms                bigint NOT NULL,
    decision             text NOT NULL,
    reason_code          text NOT NULL,
    mode                 text NOT NULL,
    source_stream        text NOT NULL,
    handoff_shadow_stream text NOT NULL,
    legacy_shadow_stream text NOT NULL,
    handoff_payload_json jsonb NOT NULL,
    legacy_payload_json  jsonb NOT NULL,
    original_payload_json jsonb NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_shadow_handoff_decisions_ts
    ON llm_route_incident_rca_shadow_handoff_decisions(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_shadow_handoff_decisions_identifiers
    ON llm_route_incident_rca_shadow_handoff_decisions(request_id, incident_id);

COMMIT;
