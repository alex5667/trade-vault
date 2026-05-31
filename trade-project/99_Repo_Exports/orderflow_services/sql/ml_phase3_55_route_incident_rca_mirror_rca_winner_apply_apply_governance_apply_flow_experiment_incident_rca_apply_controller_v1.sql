BEGIN;

CREATE TABLE IF NOT EXISTS llm_p355_rca_ctrl_decs (
    id                   bigserial PRIMARY KEY,
    ts_ms                bigint NOT NULL,
    usefulness_decision  text NOT NULL,
    usefulness_reason_code text NOT NULL,
    decision             text NOT NULL,
    reason_code          text NOT NULL,
    current_bridge_mode  text NOT NULL,
    target_bridge_mode   text NOT NULL,
    applied              integer NOT NULL,
    decision_json        jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_p355_rca_ctrl_decs_ts
    ON llm_p355_rca_ctrl_decs(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_p355_rca_ctrl_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    current_bridge_mode text NOT NULL,
    target_bridge_mode text NOT NULL,
    rollback_ready_json jsonb NOT NULL,
    applied            integer NOT NULL,
    journal_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_p355_rca_ctrl_journal_ts
    ON llm_p355_rca_ctrl_journal(ts_ms DESC);

COMMIT;
