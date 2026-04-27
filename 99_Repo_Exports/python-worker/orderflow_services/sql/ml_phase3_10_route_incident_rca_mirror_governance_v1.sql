BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_slo_rollups (
    id                     bigserial PRIMARY KEY,
    ts_ms                  bigint NOT NULL,
    window_min             integer NOT NULL,
    requested_promotions   integer NOT NULL,
    applied_promotions     integer NOT NULL,
    requested_rollbacks    integer NOT NULL,
    applied_rollbacks      integer NOT NULL,
    promotion_apply_rate   double precision NOT NULL,
    rollback_apply_rate    double precision NOT NULL,
    rollback_mttr_p50_sec  double precision NOT NULL,
    rollback_mttr_p95_sec  double precision NOT NULL,
    rollback_mttr_samples  integer NOT NULL,
    reason_codes_json      jsonb NOT NULL,
    created_at             timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_slo_rollups_ts
    ON llm_route_incident_rca_mirror_slo_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_retry_results (
    id          bigserial PRIMARY KEY,
    ts_ms       bigint NOT NULL,
    event_key   text NOT NULL,
    decision    text NOT NULL,
    reason_code text NOT NULL,
    attempts    integer NOT NULL,
    target_mode text NOT NULL,
    result_json jsonb NOT NULL,
    created_at  timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_retry_results_ts
    ON llm_route_incident_rca_mirror_retry_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_escalations (
    id           bigserial PRIMARY KEY,
    ts_ms        bigint NOT NULL,
    severity     text NOT NULL,
    summary_json jsonb NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_escalations_ts
    ON llm_route_incident_rca_mirror_escalations(ts_ms DESC);

COMMIT;
