-- Phase 8: Retention policies for telemetry and audit tables
-- Addresses P2.5 to prevent unbounded growth of Graph/Policy evaluation tables

-- 1. atr_release_equivalence_checks
ALTER TABLE atr_release_equivalence_checks DROP CONSTRAINT IF EXISTS atr_release_equivalence_checks_pkey CASCADE;
ALTER TABLE atr_release_equivalence_checks ADD PRIMARY KEY (check_id, created_at);
SELECT create_hypertable('atr_release_equivalence_checks', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_release_equivalence_checks', INTERVAL '30 days', if_not_exists => TRUE);

-- 2. atr_release_drifts
ALTER TABLE atr_release_drifts DROP CONSTRAINT IF EXISTS atr_release_drifts_pkey CASCADE;
ALTER TABLE atr_release_drifts ADD PRIMARY KEY (drift_id, created_at);
SELECT create_hypertable('atr_release_drifts', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_release_drifts', INTERVAL '30 days', if_not_exists => TRUE);

-- 3. atr_release_cutover_readiness
ALTER TABLE atr_release_cutover_readiness DROP CONSTRAINT IF EXISTS atr_release_cutover_readiness_pkey CASCADE;
ALTER TABLE atr_release_cutover_readiness ADD PRIMARY KEY (readiness_id, created_at);
SELECT create_hypertable('atr_release_cutover_readiness', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_release_cutover_readiness', INTERVAL '30 days', if_not_exists => TRUE);

-- 4. atr_effective_state_equivalence_checks
ALTER TABLE atr_effective_state_equivalence_checks DROP CONSTRAINT IF EXISTS atr_effective_state_equivalence_checks_pkey CASCADE;
ALTER TABLE atr_effective_state_equivalence_checks ADD PRIMARY KEY (check_id, created_at);
SELECT create_hypertable('atr_effective_state_equivalence_checks', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_effective_state_equivalence_checks', INTERVAL '30 days', if_not_exists => TRUE);

-- 5. atr_effective_state_drifts
ALTER TABLE atr_effective_state_drifts DROP CONSTRAINT IF EXISTS atr_effective_state_drifts_pkey CASCADE;
ALTER TABLE atr_effective_state_drifts ADD PRIMARY KEY (drift_id, created_at);
SELECT create_hypertable('atr_effective_state_drifts', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_effective_state_drifts', INTERVAL '30 days', if_not_exists => TRUE);

-- 6. atr_effective_state_cutover_readiness
ALTER TABLE atr_effective_state_cutover_readiness DROP CONSTRAINT IF EXISTS atr_effective_state_cutover_readiness_pkey CASCADE;
ALTER TABLE atr_effective_state_cutover_readiness ADD PRIMARY KEY (readiness_id, created_at);
SELECT create_hypertable('atr_effective_state_cutover_readiness', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_effective_state_cutover_readiness', INTERVAL '30 days', if_not_exists => TRUE);

-- 7. atr_graph_reconciliation_drifts
ALTER TABLE atr_graph_reconciliation_drifts DROP CONSTRAINT IF EXISTS atr_graph_reconciliation_drifts_pkey CASCADE;
ALTER TABLE atr_graph_reconciliation_drifts ADD PRIMARY KEY (drift_id, created_at);
SELECT create_hypertable('atr_graph_reconciliation_drifts', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_graph_reconciliation_drifts', INTERVAL '30 days', if_not_exists => TRUE);

-- 8. atr_graph_primary_cutover
ALTER TABLE atr_graph_primary_cutover DROP CONSTRAINT IF EXISTS atr_graph_primary_cutover_pkey CASCADE;
ALTER TABLE atr_graph_primary_cutover ADD PRIMARY KEY (cutover_id, created_at);
SELECT create_hypertable('atr_graph_primary_cutover', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_graph_primary_cutover', INTERVAL '30 days', if_not_exists => TRUE);

-- 9. atr_graph_primary_authority_violations
ALTER TABLE atr_graph_primary_authority_violations DROP CONSTRAINT IF EXISTS atr_graph_primary_authority_violations_pkey CASCADE;
ALTER TABLE atr_graph_primary_authority_violations ADD PRIMARY KEY (violation_id, created_at);
SELECT create_hypertable('atr_graph_primary_authority_violations', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('atr_graph_primary_authority_violations', INTERVAL '30 days', if_not_exists => TRUE);
