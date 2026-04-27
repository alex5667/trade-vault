BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rollout_decisions (
    id                    bigserial PRIMARY KEY,
    ts_ms                 bigint NOT NULL,
    source                text NOT NULL,
    event_decision        text NOT NULL,
    event_reason_code     text NOT NULL,
    controller_decision   text NOT NULL,
    controller_reason_code text NOT NULL,
    current_mode          text NOT NULL,
    target_mode           text NOT NULL,
    current_rollout_state text NOT NULL,
    target_rollout_state  text NOT NULL,
    advisory_only         integer NOT NULL,
    executor_mode         text NOT NULL,
    snapshot_json         jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rollout_decisions_ts
    ON llm_route_incident_rca_mirror_rollout_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rollout_journal (
    id              bigserial PRIMARY KEY,
    ts_ms           bigint NOT NULL,
    transition_type text NOT NULL,
    source          text NOT NULL,
    reason_code     text NOT NULL,
    mode_before     text NOT NULL,
    mode_after      text NOT NULL,
    state_before    text NOT NULL,
    state_after     text NOT NULL,
    snapshot_json   jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rollout_journal_ts
    ON llm_route_incident_rca_mirror_rollout_journal(ts_ms DESC);

COMMIT;
