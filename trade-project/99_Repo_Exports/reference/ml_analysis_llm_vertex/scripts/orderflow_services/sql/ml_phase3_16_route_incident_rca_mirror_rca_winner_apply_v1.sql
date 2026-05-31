BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_decisions (
    id              bigserial PRIMARY KEY,
    decision        text NOT NULL,
    recommendation  text NOT NULL,
    reason          text NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_journal (
    id              bigserial PRIMARY KEY,
    winner          text NOT NULL,
    strategy        text NOT NULL,
    executor_mode   text NOT NULL,
    new_config_json jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_journal_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_journal(ts_ms DESC);

COMMIT;
