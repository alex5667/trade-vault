-- Phase 3: Local Fallback Plane Persistence
CREATE TABLE IF NOT EXISTS llm_local_fallback_results (
    id SERIAL PRIMARY KEY,
    request_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    content TEXT,
    ts_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_llm_local_fallback_results_request_id ON llm_local_fallback_results(request_id);
CREATE INDEX IF NOT EXISTS idx_llm_local_fallback_results_ts_ms ON llm_local_fallback_results(ts_ms);

CREATE TABLE IF NOT EXISTS llm_local_fallback_rejections (
    id SERIAL PRIMARY KEY,
    request_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    ts_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_llm_local_fallback_rejections_request_id ON llm_local_fallback_rejections(request_id);
