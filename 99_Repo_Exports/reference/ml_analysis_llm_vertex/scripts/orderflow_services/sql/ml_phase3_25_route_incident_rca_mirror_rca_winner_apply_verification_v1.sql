BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_verification_results (
    id                      bigserial PRIMARY KEY,
    apply_id                varchar(255) NOT NULL,
    decision                varchar(100) NOT NULL,
    primary_match_rate      float8 NOT NULL,
    unexpected_primary_rate float8 NOT NULL,
    shadow_rate             float8 NOT NULL,
    target_mode             varchar(50) NOT NULL,
    target_primary_arm      varchar(100) NOT NULL,
    ts_ms                   bigint NOT NULL,
    created_at              timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_vr_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_verification_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal (
    id                          bigserial PRIMARY KEY,
    apply_id                    varchar(255) NOT NULL,
    reason_code                 varchar(100) NOT NULL,
    harness_state_restored_json jsonb NOT NULL,
    ts_ms                       bigint NOT NULL,
    created_at                  timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_rj_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal(ts_ms DESC);

COMMIT;
