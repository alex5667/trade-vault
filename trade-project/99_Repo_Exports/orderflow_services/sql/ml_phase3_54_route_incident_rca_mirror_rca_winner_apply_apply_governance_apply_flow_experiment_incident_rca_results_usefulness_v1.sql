BEGIN;

CREATE TABLE IF NOT EXISTS llm_p354_rca_use_results (
    id            bigserial PRIMARY KEY,
    request_id    text NOT NULL,
    bundle_id     text NOT NULL,
    ts_ms         bigint NOT NULL,
    severity      text NOT NULL,
    trigger_type  text NOT NULL,
    provider_mode text NOT NULL,
    result_json   jsonb NOT NULL,
    request_json  jsonb NOT NULL,
    bundle_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_p354_rca_use_results_ts
    ON llm_p354_rca_use_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_p354_rca_use_rollups (
    id                    bigserial PRIMARY KEY,
    ts_ms                 bigint NOT NULL,
    window_min            integer NOT NULL,
    vertex_n              integer NOT NULL,
    vertex_avg_quality    double precision NOT NULL,
    vertex_avg_usefulness double precision NOT NULL,
    vertex_accepted_rate  double precision NOT NULL,
    local_n               integer NOT NULL,
    local_avg_quality     double precision NOT NULL,
    local_avg_usefulness  double precision NOT NULL,
    local_accepted_rate   double precision NOT NULL,
    rollup_json           jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_p354_rca_use_rollups_ts
    ON llm_p354_rca_use_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_p354_rca_use_decs (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    current_bridge_mode text NOT NULL,
    target_bridge_mode text NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    decision_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_p354_rca_use_decs_ts
    ON llm_p354_rca_use_decs(ts_ms DESC);

COMMIT;
