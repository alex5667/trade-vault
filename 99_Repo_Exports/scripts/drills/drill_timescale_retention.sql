-- Drill: TimescaleDB Retention & Compression Stall
-- ---------------------------------------------------------
-- Goal: Test system resilience when background jobs for 
-- compression and retention fail or are paused. Verify that
-- database I/O degradation does not block the trading core.
-- ---------------------------------------------------------

\echo '=== Starting Failure Drill: TimescaleDB Retention Stall ==='

\echo '[Step 1] Disabling TimescaleDB background jobs for compression and retention...'

BEGIN;
SELECT alter_job(job_id, scheduled => false) 
FROM timescaledb_information.jobs 
WHERE proc_name IN ('policy_compression', 'policy_retention');
COMMIT;

\echo '>> Background jobs are now disabled.'
\echo ''
\echo '[Step 2] Observation Period'
\echo 'Wait 12-24 hours to observe I/O degradation and storage growth.'
\echo 'Operator checklist during observation:'
\echo '  1. Verify SRE Alerts fire: TimescaleDBCompressionJobsFailing, TimescaleDBDiskSpaceRunningOut'
\echo '  2. Verify trading metrics: P4/P4.1 latency MUST remain unaffected.'
\echo '  3. Verify Python services (TCA writer) do not crash from DB backpressure.'
\echo ''

\echo '[Step 3] Rollback Instructions'
\echo 'To restore normal operation, execute the following block manually:'
\echo ''
\echo 'BEGIN;'
\echo 'SELECT alter_job(job_id, scheduled => true)'
\echo 'FROM timescaledb_information.jobs'
\echo 'WHERE proc_name IN (''policy_compression'', ''policy_retention'');'
\echo 'COMMIT;'
\echo ''
\echo '=== Drill Setup Completed ==='
