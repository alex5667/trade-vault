CREATE TABLE IF NOT EXISTS atr_policy_confirm_requests (
  token text PRIMARY KEY,
  actor text NOT NULL,
  action text NOT NULL,                  -- APPROVE | REVOKE | REJECT
  target_kind text NOT NULL,             -- proposal | active
  target_id text NOT NULL,               -- proposal_id or active_ref
  proposal_id text,
  payload_json jsonb NOT NULL,
  status text NOT NULL,                  -- PENDING | CONSUMED | EXPIRED | EXPIRED_ON_BOOT | CANCELLED
  created_at_ms bigint NOT NULL,
  expires_at_ms bigint NOT NULL,
  consumed_at_ms bigint,
  note text NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_confirm_requests_status
  ON atr_policy_confirm_requests (status, expires_at_ms);

CREATE INDEX IF NOT EXISTS idx_atr_policy_confirm_requests_proposal
  ON atr_policy_confirm_requests (proposal_id, created_at_ms DESC);
