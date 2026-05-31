BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_shadow_comparisons (
    id                  bigserial PRIMARY KEY,
    correlation_key     text NOT NULL,
    incident_id         text NOT NULL,
    ts_ms               bigint NOT NULL,
    status              text NOT NULL,
    score               double precision NOT NULL,
    reason_codes_json   jsonb NOT NULL,
    handoff_payload_json jsonb NOT NULL,
    legacy_payload_json jsonb NOT NULL,
    comparison_json     jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_shadow_comparisons_ts
    ON llm_route_incident_rca_shadow_comparisons(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_shadow_comparisons_corr
    ON llm_route_incident_rca_shadow_comparisons(correlation_key);

COMMIT;
