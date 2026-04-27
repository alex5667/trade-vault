CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_route_rca_results (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(255) NOT NULL UNIQUE,
    analysis TEXT,
    output_hash VARCHAR(255),
    quality_score FLOAT,
    usefulness_score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_route_rca_quality (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(255) NOT NULL,
    output_hash VARCHAR(255),
    overall_quality_score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_route_rca_feedback (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(255) NOT NULL,
    usefulness_score FLOAT,
    base_usefulness VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
