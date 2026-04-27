BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_controller_decisions (
    id                 bigserial PRIMARY KEY,
    apply_id           varchar(255) NOT NULL,
    decision           varchar(100) NOT NULL,
    winner_arm         varchar(100) NOT NULL,
    strategy           varchar(50) NOT NULL,
    harness_state_json jsonb NOT NULL,
    ts_ms              bigint NOT NULL,
    created_at         timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_ctrl_dec_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_controller_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_controller_journal (
    id          bigserial PRIMARY KEY,
    apply_id    varchar(255) NOT NULL,
    log_type    varchar(50) NOT NULL,
    message     text NOT NULL,
    ts_ms       bigint NOT NULL,
    created_at  timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_ctrl_jrn_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_controller_journal(ts_ms DESC);

COMMIT;
