CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_route_incident_bundles (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(255) NOT NULL UNIQUE,
    severity VARCHAR(50),
    primary_reason_codes TEXT,
    summary TEXT,
    baseline_route_json JSONB,
    target_route_json JSONB,
    current_route_json JSONB,
    route_diff_json JSONB,
    timeline_json JSONB,
    sections_json JSONB,
    bundle_hash VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
