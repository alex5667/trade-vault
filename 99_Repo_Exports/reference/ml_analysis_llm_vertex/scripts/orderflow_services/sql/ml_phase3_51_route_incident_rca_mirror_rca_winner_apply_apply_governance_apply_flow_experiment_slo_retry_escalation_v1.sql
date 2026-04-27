BEGIN;

CREATE TABLE IF NOT EXISTS ml_route_rca_experiment_slo_rollups_v51 (
    id                     bigserial PRIMARY KEY,
    ts_ms                  bigint NOT NULL,
    window_min             integer NOT NULL,
    verification_n         integer NOT NULL,
    verified_n             integer NOT NULL,
    rollback_planned_n     integer NOT NULL,
    rollback_applied_n     integer NOT NULL,
    retry_n                integer NOT NULL,
    escalation_n           integer NOT NULL,
    verify_keep_rate       double precision NOT NULL,
    rollback_plan_rate     double precision NOT NULL,
    rollback_applied_rate  double precision NOT NULL,
    rollback_mttr_p95_sec  double precision NOT NULL,
    escalation_rate        double precision NOT NULL,
    rollup_json            jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ml_route_rca_experiment_slo_rollups_v51_ts
    ON ml_route_rca_experiment_slo_rollups_v51(ts_ms DESC);

CREATE TABLE IF NOT EXISTS ml_route_rca_experiment_retry_results_v51 (
    id                    bigserial PRIMARY KEY,
    ts_ms                 bigint NOT NULL,
    decision              text NOT NULL,
    reason_code           text NOT NULL,
    target_profile        text NOT NULL,
    target_incumbent_arm  text NOT NULL,
    target_weights_json   jsonb NOT NULL,
    attempts              integer NOT NULL,
    applied               integer NOT NULL,
    result_json           jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ml_route_rca_experiment_retry_results_v51_ts
    ON ml_route_rca_experiment_retry_results_v51(ts_ms DESC);

CREATE TABLE IF NOT EXISTS ml_route_rca_experiment_escalations_v51 (
    id                    bigserial PRIMARY KEY,
    ts_ms                 bigint NOT NULL,
    severity              text NOT NULL,
    reason_code           text NOT NULL,
    target_profile        text NOT NULL,
    target_incumbent_arm  text NOT NULL,
    escalation_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ml_route_rca_experiment_escalations_v51_ts
    ON ml_route_rca_experiment_escalations_v51(ts_ms DESC);

COMMIT;
