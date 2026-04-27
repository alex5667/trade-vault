
-- Phase 1.3: recommendation review/apply bus tables and indexes
-- Safe to apply multiple times.

ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS review_status text NOT NULL DEFAULT 'PENDING';

ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS replay_required boolean NOT NULL DEFAULT false;

ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS replay_status text NOT NULL DEFAULT 'UNKNOWN';

ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS approved_count integer NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS rejected_count integer NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS last_review_ts_ms bigint;

CREATE TABLE IF NOT EXISTS llm_recommendation_reviews (
  recommendation_id       text NOT NULL,
  reviewer                text NOT NULL,
  ts_ms                   bigint NOT NULL,
  decision                text NOT NULL,
  replay_status           text NOT NULL DEFAULT 'UNKNOWN',
  source                  text NOT NULL DEFAULT 'stream',
  notes                   text,
  payload_json            jsonb,
  PRIMARY KEY (recommendation_id, reviewer, ts_ms)
);

CREATE TABLE IF NOT EXISTS llm_recommendation_audit (
  audit_id                text PRIMARY KEY,
  recommendation_id       text NOT NULL,
  ts_ms                   bigint NOT NULL,
  event_type              text NOT NULL,
  actor                   text NOT NULL,
  payload_json            jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_recommendation_reviews_rec_ts
  ON llm_recommendation_reviews (recommendation_id, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendation_audit_rec_ts
  ON llm_recommendation_audit (recommendation_id, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendations_review_status
  ON llm_recommendations (review_status, apply_status, ts_ms DESC);
