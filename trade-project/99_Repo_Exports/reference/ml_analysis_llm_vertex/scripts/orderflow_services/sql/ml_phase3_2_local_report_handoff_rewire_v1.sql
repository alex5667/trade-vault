-- Phase 3.2: Local Report Handoff Rewire Decisions
-- Stores adapter routing decisions for audit and debugging.

BEGIN;

CREATE TABLE IF NOT EXISTS llm_local_report_handoff_rewire_decisions (
    id                    bigserial PRIMARY KEY,
    request_id            text NOT NULL,
    ts_ms                 bigint NOT NULL,
    decision              text NOT NULL,
    reason_code           text NOT NULL,
    source_stream         text NOT NULL,
    output_stream         text NOT NULL,
    handoff_payload_json  jsonb NOT NULL,
    original_payload_json jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_local_report_handoff_rewire_decisions_ts
    ON llm_local_report_handoff_rewire_decisions(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_local_report_handoff_rewire_decisions_request_id
    ON llm_local_report_handoff_rewire_decisions(request_id);

COMMIT;
