# Детальная карта метрик: Prometheus & Grafana

В этом документе собраны **абсолютно все** метрики, используемые в коде, дашбордах и алертах проекта.

## 1. Метрики в исходном коде (Go & Python)

### Файл: `go-gateway/internal/metrics/metrics.go`
- **`deals_loss_total`**: Total number of losing deals
- **`deals_win_total`**: Total number of winning deals
- **`gateway_up`**: 1 if gateway is up, 0 otherwise
- **`orders_enqueued_total`**: Total number of orders enqueued
- **`orders_pushed_total`**: Total number of orders pushed to MT5
- **`signals_total`**: Total number of signals received
- **`strategy_avg_pnl_usd`**: Average P/L in USD (last window) from Analytics v2.0
- **`strategy_last_auc`**: AUC from ROC tuner (Analytics v2.0)
- **`strategy_last_threshold`**: Current tuned threshold from Analytics v2.0
- **`strategy_winrate`**: Winrate 0..1 (last window) from Analytics v2.0

### Файл: `orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`bad_streak`**: consecutive degradation streak counter
- **`conf_cal_live_degrade`**: degrade decision (1/0)
- **`conf_cal_live_degrade_events_total`**: degrade decisions observed
- **`conf_cal_live_exporter_read_ok`**: exporter read succeeded (1/0)
- **`conf_cal_live_exporter_up`**: exporter loop running (1/0)
- **`conf_cal_live_last_rollback_ts_ms`**: last rollback timestamp
- **`conf_cal_live_ok`**: status ok flag (1/0)
- **`conf_cal_live_rows`**: rows in live eval window (raw)
- **`conf_cal_live_rows_cal`**: rows with calibrated values
- **`conf_cal_live_skip`**: skip_reason present (1/0)
- **`conf_cal_live_status_age_sec`**: age (now - status.ts_ms) seconds
- **`conf_cal_live_status_ts_ms`**: status ts_ms from live loop
- **`conf_cal_proof_age_sec`**: age (now - proof.ts) seconds
- **`conf_cal_proof_canary_share`**: canary share from proof controller (0..1)
- **`conf_cal_proof_evidence_age_sec`**: age (now - proof.evidence_ts) seconds
- **`conf_cal_proof_evidence_ts_sec`**: evidence ts used for freshness (seconds)
- **`conf_cal_proof_parse_errors_total`**: proof state parse/shape errors
- **`conf_cal_proof_read_errors_total`**: proof state read errors
- **`conf_cal_proof_read_ok`**: proof state read succeeded (1/0)
- **`conf_cal_proof_status_age_sec`**: status age seconds reported in proof.source
- **`conf_cal_proof_ts_sec`**: proof controller update ts (seconds)
- **`conf_cal_proof_valid`**: proof valid flag (1/0)
- **`live_brier_cal`**: live Brier on calibrated confidence
- **`live_brier_raw`**: live Brier on raw confidence
- **`live_ece_cal`**: live ECE on calibrated confidence
- **`live_ece_raw`**: live ECE on raw confidence
- **`rollback_total`**: total rollbacks observed by exporter (persisted)

### Файл: `orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `orderflow_services/conf_score_weight_tuning_exporter_v1.py`
- **`conf_score_tuning_last_age_seconds`**: Age (seconds) since last tuning run
- **`conf_score_tuning_last_exit_code`**: Exit code of last tuning run
- **`conf_score_tuning_last_joined_rows`**: Joined rows used for tuning
- **`conf_score_tuning_last_ok`**: 1 if last tuning job run succeeded
- **`conf_score_tuning_last_pos_rate`**: Positive label rate in last dataset
- **`conf_score_tuning_last_published`**: 1 if last run published tuning to Redis

### Файл: `orderflow_services/decision_coverage_exporter_v1.py`
- **`decision_last_age_seconds`**: Age of last decision in seconds (freshness probe)
- **`decision_last_ts_ms`**: Last decision timestamp (ms)
- **`decision_n_24h`**: Number of decisions in rolling 24h window
- **`decision_regime_n_24h`**: Decisions per regime in rolling 24h window
- **`decision_regime_share_24h`**: Regime share of rolling 24h decisions [0..1]

### Файл: `orderflow_services/edge_stack_shadow_status_exporter_v1.py`
- **`edge_stack_shadow_brier`**: Brier score
- **`edge_stack_shadow_champion_brier`**: Champion brier (cal, no labels)
- **`edge_stack_shadow_ece`**: ECE
- **`edge_stack_shadow_eval_age_seconds`**: Age of last status file in seconds
- **`edge_stack_shadow_eval_rows`**: Number of rows evaluated
- **`edge_stack_shadow_expectancy_r_top5pct`**: Expectancy R@top5%
- **`edge_stack_shadow_last_success`**: 1 if last shadow eval succeeded
- **`edge_stack_shadow_last_updated_ts_ms`**: Last shadow eval updated_ts_ms
- **`edge_stack_shadow_precision_top5pct`**: Precision@top5%
- **`edge_stack_shadow_promote_applied`**: 1 if this run applied promotion
- **`edge_stack_shadow_promote_recommended`**: 1 if guard recommends promotion
- **`edge_stack_shadow_status_up`**: 1 if status file is readable and not stale

### Файл: `orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `orderflow_services/exec_health_slo_autoguard_exporter_v1.py`
- **`exec_health_slo_autoguard_exporter_up`**: 1 if exporter can read autoguard state
- **`exec_health_slo_autoguard_freeze_active`**: Autoguard freeze active (1/0)

### Файл: `orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `orderflow_services/of_gate_archiver_exporter_v1.py`
- **`of_gate_timescale_expect`**: 1 if timescaledb expected
- **`of_gate_timescale_policies_disabled`**: count of disabled required policies
- **`of_gate_timescale_policies_missing`**: count of missing required policies
- **`of_gate_timescale_policy_disabled`**: policy disabled (1/0)
- **`of_gate_timescale_policy_present`**: policy present (1/0)
- **`of_gate_timescale_present`**: 1 if timescaledb extension present

### Файл: `orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `orderflow_services/orchestration_composite_preflight_exporter_v1.py`
- **`orchestration_composite_preflight_block_total`**: Number of purposes currently blocked
- **`orchestration_composite_preflight_decision_status`**: One-hot composite decision status
- **`orchestration_composite_preflight_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`orchestration_composite_preflight_exporter_up`**: 1 if composite preflight exporter loop is alive
- **`orchestration_composite_preflight_invalid_total`**: Number of purposes currently invalid
- **`orchestration_composite_preflight_ok_total`**: Number of purposes currently OK
- **`orchestration_composite_preflight_present_total`**: Number of purposes with persisted state
- **`orchestration_composite_preflight_purposes_total`**: Configured orchestration purposes covered by exporter
- **`orchestration_composite_preflight_selected_priority_rank`**: Priority rank of the selected composite reason
- **`orchestration_composite_preflight_selected_reason_code`**: One-hot normalized selected reason code
- **`orchestration_composite_preflight_selected_source`**: One-hot selected source dominating the composite decision
- **`orchestration_composite_preflight_state_age_seconds`**: Age of latest persisted composite preflight decision
- **`orchestration_composite_preflight_state_present`**: 1 if persisted composite preflight state exists for the purpose

### Файл: `orderflow_services/policy_mode_exporter_p66_v1.py`
- **`policy_mode_last_age_seconds`**: Age of last policy mode decision (seconds)
- **`policy_mode_last_ts_ms`**: Last policy mode decision timestamp (ms)
- **`policy_mode_n_24h_total`**: Total decisions observed (24h)

### Файл: `orderflow_services/signal_quality_regime_exporter_p66_v1.py`
- **`signal_quality_last_age_seconds`**: Age of last signal-quality calc (seconds)
- **`signal_quality_last_ts_ms`**: Timestamp of last signal-quality calc (ms)

### Файл: `python-worker/ml_analysis/tools/edge_stack_train_exporter_v1.py`
- **`edge_stack_train_exporter_up`**: 1 if exporter can read Redis metrics
- **`edge_stack_train_last_age_seconds`**: Age of metrics record in seconds
- **`edge_stack_train_last_joined`**: joined count from last bundle
- **`edge_stack_train_last_oof_meta_brier`**: OOF meta brier from last bundle
- **`edge_stack_train_last_oof_meta_ece`**: OOF meta ECE from last bundle
- **`edge_stack_train_last_pos_rate`**: pos_rate from last bundle
- **`edge_stack_train_last_promote_applied`**: 1 if promotion applied in last bundle
- **`edge_stack_train_last_success`**: 1 if last bundle status is ok
- **`edge_stack_train_last_train_ok`**: 1 if train validation passed in last bundle
- **`edge_stack_train_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `python-worker/mnt/data/p96_after/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length

### Файл: `python-worker/mnt/data/p96_after/tick_flow_full/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length

### Файл: `python-worker/orderflow_services/calibration_extended_exporter_v1.py`
- **`conf_cal_extended_degrade_review`**: degrade-review requested by promotion manager
- **`conf_cal_extended_delta`**: challenger - champion delta for extended calibration metrics
- **`conf_cal_extended_exporter_up`**: extended calibration exporter loop up
- **`conf_cal_extended_metric`**: extended calibration metric by arm
- **`conf_cal_extended_parse_errors_total`**: proof/status parse/shape errors
- **`conf_cal_extended_promoted_last_run`**: promotion manager promoted on last run
- **`conf_cal_extended_proof_age_sec`**: proof json age in seconds
- **`conf_cal_extended_read_errors_total`**: proof/status read errors
- **`conf_cal_extended_read_ok`**: proof/status read ok (1/0)
- **`conf_cal_extended_status_age_sec`**: status json age in seconds

### Файл: `python-worker/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`bad_streak`**: consecutive degradation streak counter
- **`conf_cal_live_degrade`**: degrade decision (1/0)
- **`conf_cal_live_degrade_events_total`**: degrade decisions observed
- **`conf_cal_live_exporter_read_ok`**: exporter read succeeded (1/0)
- **`conf_cal_live_exporter_up`**: exporter loop running (1/0)
- **`conf_cal_live_last_rollback_ts_ms`**: last rollback timestamp
- **`conf_cal_live_ok`**: status ok flag (1/0)
- **`conf_cal_live_rows`**: rows in live eval window (raw)
- **`conf_cal_live_rows_cal`**: rows with calibrated values
- **`conf_cal_live_skip`**: skip_reason present (1/0)
- **`conf_cal_live_status_age_sec`**: age (now - status.ts_ms) seconds
- **`conf_cal_live_status_ts_ms`**: status ts_ms from live loop
- **`conf_cal_proof_age_sec`**: age (now - proof.ts) seconds
- **`conf_cal_proof_canary_share`**: canary share from proof controller (0..1)
- **`conf_cal_proof_evidence_age_sec`**: age (now - proof.evidence_ts) seconds
- **`conf_cal_proof_evidence_ts_sec`**: evidence ts used for freshness (seconds)
- **`conf_cal_proof_parse_errors_total`**: proof state parse/shape errors
- **`conf_cal_proof_read_errors_total`**: proof state read errors
- **`conf_cal_proof_read_ok`**: proof state read succeeded (1/0)
- **`conf_cal_proof_status_age_sec`**: status age seconds reported in proof.source
- **`conf_cal_proof_ts_sec`**: proof controller update ts (seconds)
- **`conf_cal_proof_valid`**: proof valid flag (1/0)
- **`live_brier_cal`**: live Brier on calibrated confidence
- **`live_brier_raw`**: live Brier on raw confidence
- **`live_ece_cal`**: live ECE on calibrated confidence
- **`live_ece_raw`**: live ECE on raw confidence
- **`rollback_total`**: total rollbacks observed by exporter (persisted)

### Файл: `python-worker/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `python-worker/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `python-worker/orderflow_services/conf_score_weight_tuning_exporter_v1.py`
- **`conf_score_tuning_last_age_seconds`**: Age (seconds) since last tuning run
- **`conf_score_tuning_last_exit_code`**: Exit code of last tuning run
- **`conf_score_tuning_last_joined_rows`**: Joined rows used for tuning
- **`conf_score_tuning_last_ok`**: 1 if last tuning job run succeeded
- **`conf_score_tuning_last_pos_rate`**: Positive label rate in last dataset
- **`conf_score_tuning_last_published`**: 1 if last run published tuning to Redis

### Файл: `python-worker/orderflow_services/decision_coverage_exporter_v1.py`
- **`decision_last_age_seconds`**: Age of last decision in seconds (freshness probe)
- **`decision_last_ts_ms`**: Last decision timestamp (ms)
- **`decision_n_24h`**: Number of decisions in rolling 24h window
- **`decision_regime_n_24h`**: Decisions per regime in rolling 24h window
- **`decision_regime_share_24h`**: Regime share of rolling 24h decisions [0..1]

### Файл: `python-worker/orderflow_services/edge_stack_shadow_status_exporter_v1.py`
- **`edge_stack_shadow_brier`**: Brier score
- **`edge_stack_shadow_champion_brier`**: Champion brier (cal, no labels)
- **`edge_stack_shadow_ece`**: ECE
- **`edge_stack_shadow_eval_age_seconds`**: Age of last status file in seconds
- **`edge_stack_shadow_eval_rows`**: Number of rows evaluated
- **`edge_stack_shadow_expectancy_r_top5pct`**: Expectancy R@top5%
- **`edge_stack_shadow_last_success`**: 1 if last shadow eval succeeded
- **`edge_stack_shadow_last_updated_ts_ms`**: Last shadow eval updated_ts_ms
- **`edge_stack_shadow_precision_top5pct`**: Precision@top5%
- **`edge_stack_shadow_promote_applied`**: 1 if this run applied promotion
- **`edge_stack_shadow_promote_recommended`**: 1 if guard recommends promotion
- **`edge_stack_shadow_status_up`**: 1 if status file is readable and not stale

### Файл: `python-worker/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `python-worker/orderflow_services/exec_health_freeze_acl_audit_exporter_v1.py`
- **`exec_health_freeze_acl_audit_last_event_ts_ms`**: Last matching ACL violation timestamp in epoch ms
- **`exec_health_freeze_acl_audit_state_age_seconds`**: Age of ACL audit exporter state in seconds
- **`exec_health_freeze_acl_audit_up`**: 1 if ExecHealth ACL audit exporter loop is healthy
- **`exec_health_freeze_acl_violation_active`**: One-hot recent ACL violation by command
- **`exec_health_freeze_acl_violation_total`**: Redis ACL violations for ExecHealth freeze-control surfaces

### Файл: `python-worker/orderflow_services/exec_health_freeze_client_name_audit_exporter_v1.py`
- **`exec_health_freeze_client_name_active_connections`**: Current CLIENT LIST connections per trusted ExecHealth client name
- **`exec_health_freeze_client_name_distinct_addrs`**: Distinct addr count per trusted ExecHealth client name
- **`exec_health_freeze_client_name_last_recovery_ts_ms`**: Last successful client identity recovery timestamp
- **`exec_health_freeze_client_name_match`**: 1 if trusted client-name/lib-name field matches the ExecHealth contract
- **`exec_health_freeze_client_name_policy_last_check_ts_ms`**: Last client-name policy check timestamp in epoch ms
- **`exec_health_freeze_client_name_policy_state_age_seconds`**: Age of ExecHealth client-name policy exporter state in seconds
- **`exec_health_freeze_client_name_policy_up`**: 1 if Redis-side ExecHealth client-name policy exporter loop is healthy
- **`exec_health_freeze_client_name_recovery_total`**: Self-healing recoveries after Redis reconnect identity drift
- **`exec_health_freeze_client_name_repair_failed_total`**: Failed self-healing attempts after Redis reconnect identity drift
- **`exec_health_freeze_client_name_violation`**: One-hot Redis-side client-name policy violation

### Файл: `python-worker/orderflow_services/exec_health_freeze_control_exporter_v1.py`
- **`exec_health_freeze_control_effective_active`**: Effective ExecHealth freeze state (1/0)
- **`exec_health_freeze_control_exporter_up`**: 1 if exporter can read freeze control state
- **`exec_health_freeze_control_manual_ack_age_seconds`**: Age of the last manual ack in seconds
- **`exec_health_freeze_control_manual_ack_required`**: Whether manual ack is required before thaw
- **`exec_health_freeze_control_manual_freeze_total`**: Total manual freeze overrides
- **`exec_health_freeze_control_manual_override_active`**: Whether a manual operator override is active
- **`exec_health_freeze_control_source`**: One-hot current freeze source
- **`exec_health_freeze_control_state_age_seconds`**: Age of freeze control state in seconds
- **`exec_health_freeze_control_thaw_total`**: Total manual thaw acknowledgements
- **`exec_health_freeze_control_trigger_total`**: Total autoguard latches recorded in control state

### Файл: `python-worker/orderflow_services/exec_health_freeze_dual_control_exporter_v1.py`
- **`exec_health_freeze_dual_control_exporter_up`**: 1 if exporter can read dual-control state
- **`exec_health_freeze_dual_control_pending_request`**: 1 if thaw request is pending
- **`exec_health_freeze_dual_control_ready`**: 1 if request has valid prepare+approve by distinct operators
- **`exec_health_freeze_dual_control_request_age_seconds`**: Age of current thaw request in seconds
- **`exec_health_freeze_dual_control_same_operator_violation`**: 1 if preparer and approver are identical
- **`exec_health_freeze_dual_control_status`**: One-hot dual-control request status
- **`exec_health_freeze_dual_control_valid_commit_event_present`**: 1 if a valid signed commit event exists
- **`exec_health_freeze_dual_control_violation`**: One-hot dual-control violations

### Файл: `python-worker/orderflow_services/exec_health_freeze_integrity_exporter_v1.py`
- **`exec_health_freeze_integrity_control_present`**: 1 if freeze control hash is present
- **`exec_health_freeze_integrity_exporter_up`**: 1 if exporter can read freeze control integrity state
- **`exec_health_freeze_integrity_invalid_ack_event_present`**: 1 if an invalid ack event was observed
- **`exec_health_freeze_integrity_last_trigger_ts_ms`**: Latest trigger ts referenced by control/state or stream
- **`exec_health_freeze_integrity_pending_ack`**: 1 if a pending signed manual ack is still required
- **`exec_health_freeze_integrity_request_log_valid_sequence`**: 1 if append-only request log has a valid prepare/approve/commit sequence
- **`exec_health_freeze_integrity_state_age_seconds`**: Max age of control/state hashes
- **`exec_health_freeze_integrity_state_present`**: 1 if autoguard state hash is present
- **`exec_health_freeze_integrity_valid_ack_event_present`**: 1 if a valid signed ack event exists for the current nonce
- **`exec_health_freeze_integrity_violation`**: One-hot freeze integrity violations

### Файл: `python-worker/orderflow_services/exec_health_freeze_service_identity_exporter_v1.py`
- **`exec_health_freeze_service_identity_active_connections`**: Current CLIENT LIST connections for expected ExecHealth service
- **`exec_health_freeze_service_identity_last_check_ts_ms`**: Last service identity check timestamp in epoch ms
- **`exec_health_freeze_service_identity_match`**: 1 if live CLIENT LIST matches expected identity field
- **`exec_health_freeze_service_identity_state_age_seconds`**: Age of service identity exporter state in seconds
- **`exec_health_freeze_service_identity_up`**: 1 if service identity exporter loop is healthy
- **`exec_health_freeze_service_identity_violation`**: One-hot service identity violation

### Файл: `python-worker/orderflow_services/exec_health_freeze_tamper_guard_v1.py`
- **`exec_health_freeze_tamper_guard_last_refreeze_ts_ms`**: Last automatic tamper refreeze timestamp
- **`exec_health_freeze_tamper_guard_refreeze_total`**: Automatic re-freeze actions due to tamper
- **`exec_health_freeze_tamper_guard_state_age_seconds`**: Age of tamper guard state hash in seconds
- **`exec_health_freeze_tamper_guard_tamper_active`**: 1 if current integrity evaluation indicates tamper requiring refreeze
- **`exec_health_freeze_tamper_guard_up`**: 1 if tamper guard loop is healthy
- **`exec_health_freeze_tamper_guard_violation`**: One-hot tamper violations seen by guard

### Файл: `python-worker/orderflow_services/exec_health_slo_autoguard_exporter_v1.py`
- **`exec_health_slo_autoguard_exporter_up`**: 1 if exporter can read autoguard state
- **`exec_health_slo_autoguard_freeze_active`**: Autoguard freeze active (1/0)

### Файл: `python-worker/orderflow_services/exec_health_slo_exporter_v1.py`
- **`exec_health_slo_active_instances`**: Active ExecHealth instances by scope
- **`exec_health_slo_cross_scope_mode_distinct`**: Distinct modal modes across scopes
- **`exec_health_slo_cross_scope_threshold_distinct`**: Distinct modal thresholds across scopes
- **`exec_health_slo_exporter_up`**: 1 if exporter can read Redis summary
- **`exec_health_slo_last_age_seconds`**: Age of last SLO summary in seconds
- **`exec_health_slo_last_updated_ts_ms`**: Last SLO summary updated_ts_ms
- **`exec_health_slo_rollout_drift_instances`**: Instances with rollout drift by scope
- **`exec_health_slo_rollout_drift_instances_total`**: Total instances with rollout drift
- **`exec_health_slo_scope_deploy_distinct`**: Distinct deploy ids by scope
- **`exec_health_slo_scope_mode_distinct`**: Distinct effective modes by scope
- **`exec_health_slo_scope_threshold_distinct`**: Distinct threshold values by scope/metric
- **`exec_health_slo_share`**: ExecHealth share by scope/outcome
- **`exec_health_slo_stale_instances`**: Stale ExecHealth instances by scope
- **`exec_health_slo_stale_instances_total`**: Total stale instances

### Файл: `python-worker/orderflow_services/feature_drift_batch_exporter_v1.py`
- **`feature_drift_batch_crit_n`**: Crit-level feature drift count
- **`feature_drift_batch_denylist_suggest_n`**: Features suggested for denylist AB
- **`feature_drift_batch_exporter_up`**: 1 if exporter can read Redis summary
- **`feature_drift_batch_feature_delta`**: Per-feature missing/zero/clip deltas
- **`feature_drift_batch_feature_flag`**: Per-feature drift flags
- **`feature_drift_batch_feature_ks_pvalue`**: Per-feature KS p-value
- **`feature_drift_batch_feature_ks_stat`**: Per-feature KS statistic
- **`feature_drift_batch_feature_psi`**: Per-feature PSI
- **`feature_drift_batch_features_evaluated`**: Features evaluated
- **`feature_drift_batch_features_total`**: Total features considered
- **`feature_drift_batch_last_age_seconds`**: Age of latest drift-batch summary
- **`feature_drift_batch_last_success`**: 1 if latest drift batch status is ok
- **`feature_drift_batch_last_updated_ts_ms`**: updated_ts_ms from Redis hash
- **`feature_drift_batch_shadow_disable_suggest_n`**: Features suggested for shadow disable
- **`feature_drift_batch_warn_n`**: Warn-level feature drift count
- **`feature_drift_batch_worst_ks_stat`**: Worst KS stat in latest report
- **`feature_drift_batch_worst_psi`**: Worst PSI in latest report

### Файл: `python-worker/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `python-worker/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `python-worker/orderflow_services/latency_contract_deploy_lint_exporter_v1.py`
- **`latency_contract_deploy_lint_errors_total`**: latest deploy lint errors count
- **`latency_contract_deploy_lint_exporter_read_ok`**: latency deploy lint exporter redis read ok
- **`latency_contract_deploy_lint_exporter_up`**: latency deploy lint exporter loop running
- **`latency_contract_deploy_lint_fail_age_seconds`**: age of current deploy lint failure streak
- **`latency_contract_deploy_lint_gate_active`**: persistent deploy lint gate active
- **`latency_contract_deploy_lint_last_checked_age_seconds`**: age of last deploy lint check
- **`latency_contract_deploy_lint_notifier_active`**: deploy lint notifier sees active persistent drift
- **`latency_contract_deploy_lint_notifier_last_run_age_seconds`**: age of deploy lint notifier last run
- **`latency_contract_deploy_lint_notifier_silenced`**: deploy lint notifier currently suppressed by silence workflow
- **`latency_contract_deploy_lint_notifier_silenced_purposes_total`**: count of currently silenced purposes in notifier state
- **`latency_contract_deploy_lint_notifier_state_present`**: deploy lint notifier state present
- **`latency_contract_deploy_lint_ok`**: latest deploy lint result ok
- **`latency_contract_deploy_lint_silence_active`**: deploy lint notifier silence active
- **`latency_contract_deploy_lint_silence_approval_age_seconds`**: age of latest override approval request
- **`latency_contract_deploy_lint_silence_approval_binding_match`**: latest override approval still matches current deploy-lint drift binding
- **`latency_contract_deploy_lint_silence_approval_binding_schema_version`**: binding schema version used by latest override approval request
- **`latency_contract_deploy_lint_silence_approval_cancelled`**: latest override approval request auto-cancelled after approval freshness elapsed
- **`latency_contract_deploy_lint_silence_approval_details_fingerprint_match`**: latest override approval still matches current deploy-lint semantic details_json fingerprint
- **`latency_contract_deploy_lint_silence_approval_expired`**: latest override approval request auto-expired before approval
- **`latency_contract_deploy_lint_silence_approval_freshness_remaining_seconds`**: remaining freshness time for latest override approval request
- **`latency_contract_deploy_lint_silence_approval_invalidated`**: latest override approval request was invalidated because deploy-lint drift changed before final ack
- **`latency_contract_deploy_lint_silence_approval_notifier_route_class_match`**: latest override approval request notifier route class still matches current state
- **`latency_contract_deploy_lint_silence_approval_pending`**: latest override approval request is prepared and awaiting second approver
- **`latency_contract_deploy_lint_silence_approval_ready`**: latest override approval request is approved and ready for requester ack
- **`latency_contract_deploy_lint_silence_approval_warning_policy_match`**: latest override approval request warning severity policy still matches current state
- **`latency_contract_deploy_lint_silence_dual_control_denied_total`**: times silence ack was denied because dual-control approval was missing or invalid
- **`latency_contract_deploy_lint_silence_dual_control_override_active`**: current notifier silence is active under an approved dual-control exception
- **`latency_contract_deploy_lint_silence_dual_control_required`**: current silence requires dual-control exception approval metadata
- **`latency_contract_deploy_lint_silence_policy_denied_total`**: times silence ack was denied by policy for this purpose
- **`latency_contract_deploy_lint_silence_policy_limit_hit_total`**: times ack policy limits were hit for this purpose
- **`latency_contract_deploy_lint_silence_policy_override_active`**: current notifier silence is using escalation-ticket override
- **`latency_contract_deploy_lint_silence_policy_window_ack_count`**: ack count used in current silence policy window
- **`latency_contract_deploy_lint_silence_policy_window_budget_minutes_used`**: budget minutes used in current silence policy window
- **`latency_contract_deploy_lint_silence_remaining_seconds`**: remaining notifier silence time
- **`latency_contract_deploy_lint_silence_state_present`**: deploy lint silence state present
- **`latency_contract_deploy_lint_silence_ttl_expired`**: last silence window for this purpose expired and escalation should remain active until fixed/re-acked
- **`latency_contract_deploy_lint_silence_ttl_expired_age_seconds`**: age since notifier observed silence TTL expiry
- **`latency_contract_deploy_lint_state_present`**: deploy lint state present
- **`latency_contract_deploy_lint_summary_dual_control_binding_mismatch_total`**: number of latest override approvals whose bound deploy-lint drift no longer matches the current drift snapshot
- **`latency_contract_deploy_lint_summary_dual_control_cancelled_gate_active_total`**: number of active gate purposes whose latest approved override request auto-cancelled before ack consumption
- **`latency_contract_deploy_lint_summary_dual_control_expired_gate_active_total`**: number of active gate purposes whose latest prepared override request auto-expired
- **`latency_contract_deploy_lint_summary_dual_control_invalidated_gate_active_total`**: number of active gate purposes whose latest override approval was invalidated because deploy-lint drift changed before final ack
- **`latency_contract_deploy_lint_summary_dual_control_override_gate_active_total`**: number of active gate purposes currently silenced under an approved dual-control exception
- **`latency_contract_deploy_lint_summary_dual_control_pending_total`**: number of purposes with pending dual-control override approval requests
- **`latency_contract_deploy_lint_summary_dual_control_ready_total`**: number of purposes with approved dual-control override requests waiting to be consumed
- **`latency_contract_deploy_lint_summary_dual_control_route_binding_mismatch_total`**: number of active gate purposes whose warning policy or notifier route class changed
- **`latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total`**: number of latest override approvals whose semantic drift binding no longer matches current gate_reason_code/errors_count/details fingerprint
- **`latency_contract_deploy_lint_summary_expired_gate_active_total`**: number of purposes with persistent deploy lint gate active after silence TTL expiry
- **`latency_contract_deploy_lint_summary_fail_total`**: number of purposes currently failing deploy lint
- **`latency_contract_deploy_lint_summary_gate_active_total`**: number of purposes with persistent deploy lint gate active
- **`latency_contract_deploy_lint_summary_policy_blocked_gate_active_total`**: number of active gate purposes where latest ack attempt was blocked by silence policy
- **`latency_contract_deploy_lint_summary_policy_override_gate_active_total`**: number of active gate purposes currently silenced via escalation-ticket override
- **`latency_contract_deploy_lint_summary_silenced_gate_active_total`**: number of purposes with persistent deploy lint gate active but silenced in notifier
- **`latency_contract_deploy_lint_summary_unsilenced_gate_active_total`**: number of purposes with persistent deploy lint gate active and not silenced in notifier
- **`latency_contract_deploy_lint_warnings_total`**: latest deploy lint warnings count

### Файл: `python-worker/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `python-worker/orderflow_services/of_gate_archiver_exporter_v1.py`
- **`of_gate_timescale_expect`**: 1 if timescaledb expected
- **`of_gate_timescale_policies_disabled`**: count of disabled required policies
- **`of_gate_timescale_policies_missing`**: count of missing required policies
- **`of_gate_timescale_policy_disabled`**: policy disabled (1/0)
- **`of_gate_timescale_policy_present`**: policy present (1/0)
- **`of_gate_timescale_present`**: 1 if timescaledb extension present

### Файл: `python-worker/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `python-worker/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `python-worker/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `python-worker/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `python-worker/orderflow_services/orchestration_composite_preflight_exporter_v1.py`
- **`orchestration_composite_preflight_block_total`**: Number of purposes currently blocked
- **`orchestration_composite_preflight_decision_status`**: One-hot composite decision status
- **`orchestration_composite_preflight_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`orchestration_composite_preflight_exporter_up`**: 1 if composite preflight exporter loop is alive
- **`orchestration_composite_preflight_invalid_total`**: Number of purposes currently invalid
- **`orchestration_composite_preflight_ok_total`**: Number of purposes currently OK
- **`orchestration_composite_preflight_present_total`**: Number of purposes with persisted state
- **`orchestration_composite_preflight_purposes_total`**: Configured orchestration purposes covered by exporter
- **`orchestration_composite_preflight_selected_priority_rank`**: Priority rank of the selected composite reason
- **`orchestration_composite_preflight_selected_reason_code`**: One-hot normalized selected reason code
- **`orchestration_composite_preflight_selected_source`**: One-hot selected source dominating the composite decision
- **`orchestration_composite_preflight_soft_total`**: Number of purposes currently soft-blocked
- **`orchestration_composite_preflight_source_status`**: Per-source orchestration preflight status
- **`orchestration_composite_preflight_state_age_seconds`**: Age of latest persisted composite preflight decision
- **`orchestration_composite_preflight_state_present`**: 1 if persisted composite preflight state exists for the purpose

### Файл: `python-worker/orderflow_services/policy_mode_exporter_p66_v1.py`
- **`policy_mode_last_age_seconds`**: Age of last policy mode decision (seconds)
- **`policy_mode_last_ts_ms`**: Last policy mode decision timestamp (ms)
- **`policy_mode_n_24h_total`**: Total decisions observed (24h)

### Файл: `python-worker/orderflow_services/signal_quality_regime_exporter_p66_v1.py`
- **`signal_quality_last_age_seconds`**: Age of last signal-quality calc (seconds)
- **`signal_quality_last_ts_ms`**: Timestamp of last signal-quality calc (ms)

### Файл: `python-worker/orderflow_services/strategy_research_guard_state_exporter_v1.py`
- **`strategy_research_guard_blocker_active`**: 1 if promotion/apply blocker is active
- **`strategy_research_guard_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_guard_blocker_reason`**: One-hot blocker reason kind
- **`strategy_research_guard_chosen_variant_unique`**: 1 if latest best variant is uniquely identified
- **`strategy_research_guard_cscv_splits`**: CSCV split count used in latest report
- **`strategy_research_guard_downside_adjusted_return`**: Downside-adjusted return
- **`strategy_research_guard_dsr`**: Deflated Sharpe Ratio or conservative proxy
- **`strategy_research_guard_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`strategy_research_guard_exporter_up`**: 1 if exporter loop is alive
- **`strategy_research_guard_hit_rate_conditioned_on_cost`**: Hit-rate conditioned on cost
- **`strategy_research_guard_last_success`**: 1 if latest research guard job succeeded
- **`strategy_research_guard_last_updated_ts_ms`**: updated_ts_ms from latest research report
- **`strategy_research_guard_mean_r`**: Mean R of research sample
- **`strategy_research_guard_net_expectancy`**: Research net expectancy
- **`strategy_research_guard_pbo`**: Probability of Backtest Overfitting
- **`strategy_research_guard_precision_at_top_x`**: Precision at selected top-X bucket
- **`strategy_research_guard_primary_metric_value`**: Primary evaluator metric value
- **`strategy_research_guard_psr`**: Probabilistic Sharpe Ratio or equivalent normalized score
- **`strategy_research_guard_report_age_seconds`**: Age of latest research report in seconds
- **`strategy_research_guard_report_only`**: 1 if blocker is in report-only mode
- **`strategy_research_guard_summary_present`**: 1 if summary hash exists

### Файл: `python-worker/orderflow_services/strategy_research_stats_alert_policy_exporter_v1.py`
- **`strategy_research_stats_alert_policy_active_suppressions_total`**: Number of active TTL-backed suppress overrides by family
- **`strategy_research_stats_alert_policy_defaults_present`**: 1 if defaults hash exists
- **`strategy_research_stats_alert_policy_delta_vs_7d`**: Required 24h-vs-7d delta for purpose/family alerts
- **`strategy_research_stats_alert_policy_enabled`**: 1 if family alerting is enabled for purpose
- **`strategy_research_stats_alert_policy_exporter_up`**: 1 if alert policy exporter loop is running
- **`strategy_research_stats_alert_policy_hash_present`**: 1 if explicit purpose policy hash exists
- **`strategy_research_stats_alert_policy_min_events_24h`**: Minimum 24h events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_min_events_7d`**: Minimum 7d events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_override_active`**: 1 if a TTL-backed suppress override is active for purpose/family
- **`strategy_research_stats_alert_policy_override_budget_remaining_seconds`**: Remaining suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_budget_used_seconds`**: Cumulative suppression budget used by purpose/family override chain
- **`strategy_research_stats_alert_policy_override_created_unixtime`**: Creation time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approval_age_seconds`**: Age of approved dual-control approval for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approver_present`**: 1 if a second approver is recorded for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_freshness_remaining_seconds`**: Remaining freshness window for approved dual-control approval for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_required`**: 1 if limit-hit renewal for purpose/family requires dual-control approval
- **`strategy_research_stats_alert_policy_override_dual_control_state`**: Dual-control approval state for purpose/family
- **`strategy_research_stats_alert_policy_override_escalation_present`**: 1 if a renewal acknowledgement contains escalation fields for purpose/family
- **`strategy_research_stats_alert_policy_override_expire_unixtime`**: Expiry time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_expired_recently`**: 1 if suppress override expired recently for purpose/family
- **`strategy_research_stats_alert_policy_override_expiring_soon`**: 1 if active suppress override is within reminder window for purpose/family
- **`strategy_research_stats_alert_policy_override_last_expired_unixtime`**: Unix time of the most recent observed override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_last_reminder_unixtime`**: Unix time of the most recent expiry reminder for purpose/family
- **`strategy_research_stats_alert_policy_override_lifecycle_state`**: Lifecycle state of suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit`**: 1 if the latest suppression workflow hit a policy limit kind for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit_age_seconds`**: Age of the latest policy limit hit for purpose/family
- **`strategy_research_stats_alert_policy_override_max_budget_seconds`**: Configured max suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_max_renew_count`**: Configured max renew count for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_operator_present`**: 1 if active suppress override contains an operator for purpose/family
- **`strategy_research_stats_alert_policy_override_present`**: 1 if an override hash is present and still active for purpose/family
- **`strategy_research_stats_alert_policy_override_reason_present`**: 1 if active suppress override contains a reason for purpose/family
- **`strategy_research_stats_alert_policy_override_remaining_seconds`**: Seconds until suppress override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_age_seconds`**: Age of the current renewal acknowledgement for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_present`**: 1 if a renewal acknowledgement is currently stored for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_required`**: 1 if reminder/expiry requires explicit acknowledgement before renew for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_count`**: How many times a suppress override has been renewed for purpose/family
- **`strategy_research_stats_alert_policy_override_requires_escalation`**: 1 if policy requires escalation once limit is exceeded for purpose/family
- **`strategy_research_stats_alert_policy_override_state_present`**: 1 if persistent lifecycle state exists for purpose/family
- **`strategy_research_stats_alert_policy_override_ticket_present`**: 1 if active suppress override contains a ticket for purpose/family
- **`strategy_research_stats_alert_policy_redis_read_ok`**: 1 if alert policy exporter can read Redis
- **`strategy_research_stats_alert_policy_share_threshold_24h`**: 24h share threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_static_suppress_active`**: 1 if family alerting is statically suppressed by policy hash for purpose
- **`strategy_research_stats_alert_policy_suppress_active`**: 1 if family alerting is suppressed for purpose after TTL-aware overrides are applied

### Файл: `python-worker/orderflow_services/strategy_research_stats_exporter_v1.py`
- **`strategy_research_stats_blocker_active`**: 1 if hard blocker is active
- **`strategy_research_stats_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_stats_downside_adjusted_return`**: Downside adjusted return
- **`strategy_research_stats_dsr`**: Deflated Sharpe ratio proxy
- **`strategy_research_stats_exporter_redis_read_ok`**: 1 if exporter read redis successfully
- **`strategy_research_stats_exporter_up`**: 1 if exporter loop is running
- **`strategy_research_stats_gate_mode`**: One-hot gate mode
- **`strategy_research_stats_gate_status`**: One-hot gate status
- **`strategy_research_stats_hit_rate_conditioned_on_cost`**: Hit rate conditioned on cost
- **`strategy_research_stats_invalid_state`**: 1 if gate state is invalid
- **`strategy_research_stats_mean_r`**: Mean R
- **`strategy_research_stats_net_expectancy`**: Net expectancy
- **`strategy_research_stats_pbo`**: Probability of backtest overfitting
- **`strategy_research_stats_period_count`**: Period count used in latest report
- **`strategy_research_stats_precision_at_top_x`**: Precision@topX
- **`strategy_research_stats_primary_metric_value`**: Primary strategy research metric
- **`strategy_research_stats_psr`**: Probabilistic Sharpe ratio proxy
- **`strategy_research_stats_reason`**: One-hot blocker reason
- **`strategy_research_stats_report_age_seconds`**: Age of latest strategy research stats report
- **`strategy_research_stats_rows`**: Row count used in latest report
- **`strategy_research_stats_soft_block_active`**: 1 if soft blocker is active
- **`strategy_research_stats_summary_present`**: 1 if summary hash exists
- **`strategy_research_stats_variant_count`**: Variant count used in latest report

### Файл: `python-worker/reference/ml_analysis/tools/edge_stack_train_exporter_v1.py`
- **`edge_stack_train_exporter_up`**: 1 if exporter can read Redis metrics
- **`edge_stack_train_last_age_seconds`**: Age of metrics record in seconds
- **`edge_stack_train_last_joined`**: joined count from last bundle
- **`edge_stack_train_last_oof_meta_brier`**: OOF meta brier from last bundle
- **`edge_stack_train_last_oof_meta_ece`**: OOF meta ECE from last bundle
- **`edge_stack_train_last_pos_rate`**: pos_rate from last bundle
- **`edge_stack_train_last_promote_applied`**: 1 if promotion applied in last bundle
- **`edge_stack_train_last_success`**: 1 if last bundle status is ok
- **`edge_stack_train_last_train_ok`**: 1 if train validation passed in last bundle
- **`edge_stack_train_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `python-worker/reference/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`bad_streak`**: consecutive degradation streak counter
- **`conf_cal_live_degrade`**: degrade decision (1/0)
- **`conf_cal_live_degrade_events_total`**: degrade decisions observed
- **`conf_cal_live_exporter_read_ok`**: exporter read succeeded (1/0)
- **`conf_cal_live_exporter_up`**: exporter loop running (1/0)
- **`conf_cal_live_last_rollback_ts_ms`**: last rollback timestamp
- **`conf_cal_live_ok`**: status ok flag (1/0)
- **`conf_cal_live_rows`**: rows in live eval window (raw)
- **`conf_cal_live_rows_cal`**: rows with calibrated values
- **`conf_cal_live_skip`**: skip_reason present (1/0)
- **`conf_cal_live_status_age_sec`**: age (now - status.ts_ms) seconds
- **`conf_cal_live_status_ts_ms`**: status ts_ms from live loop
- **`conf_cal_proof_age_sec`**: age (now - proof.ts) seconds
- **`conf_cal_proof_canary_share`**: canary share from proof controller (0..1)
- **`conf_cal_proof_evidence_age_sec`**: age (now - proof.evidence_ts) seconds
- **`conf_cal_proof_evidence_ts_sec`**: evidence ts used for freshness (seconds)
- **`conf_cal_proof_parse_errors_total`**: proof state parse/shape errors
- **`conf_cal_proof_read_errors_total`**: proof state read errors
- **`conf_cal_proof_read_ok`**: proof state read succeeded (1/0)
- **`conf_cal_proof_status_age_sec`**: status age seconds reported in proof.source
- **`conf_cal_proof_ts_sec`**: proof controller update ts (seconds)
- **`conf_cal_proof_valid`**: proof valid flag (1/0)
- **`live_brier_cal`**: live Brier on calibrated confidence
- **`live_brier_raw`**: live Brier on raw confidence
- **`live_ece_cal`**: live ECE on calibrated confidence
- **`live_ece_raw`**: live ECE on raw confidence
- **`rollback_total`**: total rollbacks observed by exporter (persisted)

### Файл: `python-worker/reference/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `python-worker/reference/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `python-worker/reference/orderflow_services/conf_score_weight_tuning_exporter_v1.py`
- **`conf_score_tuning_last_age_seconds`**: Age (seconds) since last tuning run
- **`conf_score_tuning_last_exit_code`**: Exit code of last tuning run
- **`conf_score_tuning_last_joined_rows`**: Joined rows used for tuning
- **`conf_score_tuning_last_ok`**: 1 if last tuning job run succeeded
- **`conf_score_tuning_last_pos_rate`**: Positive label rate in last dataset
- **`conf_score_tuning_last_published`**: 1 if last run published tuning to Redis

### Файл: `python-worker/reference/orderflow_services/decision_coverage_exporter_v1.py`
- **`decision_last_age_seconds`**: Age of last decision in seconds (freshness probe)
- **`decision_last_ts_ms`**: Last decision timestamp (ms)
- **`decision_n_24h`**: Number of decisions in rolling 24h window
- **`decision_regime_n_24h`**: Decisions per regime in rolling 24h window
- **`decision_regime_share_24h`**: Regime share of rolling 24h decisions [0..1]

### Файл: `python-worker/reference/orderflow_services/edge_stack_shadow_status_exporter_v1.py`
- **`edge_stack_shadow_brier`**: Brier score
- **`edge_stack_shadow_champion_brier`**: Champion brier (cal, no labels)
- **`edge_stack_shadow_ece`**: ECE
- **`edge_stack_shadow_eval_age_seconds`**: Age of last status file in seconds
- **`edge_stack_shadow_eval_rows`**: Number of rows evaluated
- **`edge_stack_shadow_expectancy_r_top5pct`**: Expectancy R@top5%
- **`edge_stack_shadow_last_success`**: 1 if last shadow eval succeeded
- **`edge_stack_shadow_last_updated_ts_ms`**: Last shadow eval updated_ts_ms
- **`edge_stack_shadow_precision_top5pct`**: Precision@top5%
- **`edge_stack_shadow_promote_applied`**: 1 if this run applied promotion
- **`edge_stack_shadow_promote_recommended`**: 1 if guard recommends promotion
- **`edge_stack_shadow_status_up`**: 1 if status file is readable and not stale

### Файл: `python-worker/reference/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `python-worker/reference/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `python-worker/reference/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `python-worker/reference/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `python-worker/reference/orderflow_services/of_gate_archiver_exporter_v1.py`
- **`of_gate_timescale_expect`**: 1 if timescaledb expected
- **`of_gate_timescale_policies_disabled`**: count of disabled required policies
- **`of_gate_timescale_policies_missing`**: count of missing required policies
- **`of_gate_timescale_policy_disabled`**: policy disabled (1/0)
- **`of_gate_timescale_policy_present`**: policy present (1/0)
- **`of_gate_timescale_present`**: 1 if timescaledb extension present

### Файл: `python-worker/reference/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `python-worker/reference/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `python-worker/reference/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `python-worker/reference/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `python-worker/reference/orderflow_services/policy_mode_exporter_p66_v1.py`
- **`policy_mode_last_age_seconds`**: Age of last policy mode decision (seconds)
- **`policy_mode_last_ts_ms`**: Last policy mode decision timestamp (ms)
- **`policy_mode_n_24h_total`**: Total decisions observed (24h)

### Файл: `python-worker/reference/orderflow_services/signal_quality_regime_exporter_p66_v1.py`
- **`signal_quality_last_age_seconds`**: Age of last signal-quality calc (seconds)
- **`signal_quality_last_ts_ms`**: Timestamp of last signal-quality calc (ms)

### Файл: `python-worker/reference/services/ab_winner_apply_runner.py`
- **`ab_apply_applied_total`**: Applied suggestions total
- **`ab_apply_backlog_gauge`**: Backlog estimate (seen in cycle)
- **`ab_apply_considered_total`**: Considered sids total
- **`ab_apply_errors_total`**: Apply runner errors total
- **`ab_apply_last_success_ts_ms`**: Last success ts_ms
- **`ab_apply_skipped_total`**: Skipped suggestions total

### Файл: `python-worker/reference/services/async_signal_publisher.py`
- **`signals_publish_busy_total`**: BusyLoading Redis errors
- **`signals_publish_dropped_total`**: Signals dropped after max retries or overflow
- **`signals_publish_errors_total`**: Failed signal publishes
- **`signals_publish_ok_total`**: Successful signal publishes
- **`signals_publish_retries_enqueued_total`**: Signals queued for retry
- **`signals_publish_retries_success_total`**: Successful retries

### Файл: `python-worker/reference/services/binance_account_reporter.py`
- **`binance_account_available_balance_usdt`**: Available balance
- **`binance_account_initial_margin_usdt`**: Initial margin
- **`binance_account_maint_margin_usdt`**: Maintenance margin
- **`binance_account_margin_balance_usdt`**: Margin balance
- **`binance_account_open_notional_usdt`**: Total absolute notional exposure
- **`binance_account_open_orders`**: Number of open orders
- **`binance_account_open_positions`**: Number of open positions
- **`binance_account_report_last_err_ts_seconds`**: Last failed report time
- **`binance_account_report_last_ok_ts_seconds`**: Last successful report time
- **`binance_account_snapshot_age_ms`**: Age of last stored snapshot
- **`binance_account_unrealized_pnl_usdt`**: Unrealized PnL
- **`binance_account_wallet_balance_usdt`**: Wallet balance

### Файл: `python-worker/reference/services/execution_gate_service.py`
- **`exec_gate_confirmations_received_total`**: Total confirmations received
- **`exec_gate_orders_published_total`**: Total verified orders published
- **`exec_gate_pending_proposals`**: Current number of pending proposals
- **`exec_gate_proposals_received_total`**: Total signal proposals received
- **`exec_gate_telegram_notifications_total`**: Total Telegram notifications sent

### Файл: `python-worker/reference/services/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `python-worker/reference/services/of_confirm_service.py`
- **`of_confirm_events_received_total`**: Total events received
- **`of_confirm_signals_out_total`**: Total confirmed signals published
- **`of_confirm_signals_processed_total`**: Total signals processed

### Файл: `python-worker/reference/services/orderflow/meta_ab_v2_report_exporter_v1.py`
- **`meta_ab_v2_action`**: one-hot action
- **`meta_ab_v2_delta_exp_r_per_candidate`**: delta exp_r_per_candidate (chall - champ)
- **`meta_ab_v2_delta_tail_rate_per_candidate`**: delta tail_rate_per_candidate (chall - champ)
- **`meta_ab_v2_last_ts_ms`**: report ts_ms
- **`meta_ab_v2_n_eligible`**: eligible rows
- **`meta_ab_v2_n_total`**: dataset rows total
- **`meta_ab_v2_p_min`**: p_min threshold
- **`meta_ab_v2_report_parse_errors_total`**: JSON parse/read errors
- **`meta_ab_v2_report_present`**: 1 if report file parsed OK
- **`meta_ab_v2_share_current`**: current challenger share
- **`meta_ab_v2_share_next`**: recommended challenger share
- **`meta_ab_v2_winner`**: one-hot winner

### Файл: `python-worker/reference/services/orderflow/signal_quality_exporter_v1.py`
- **`signal_quality_ece_24h`**: Expected Calibration Error
- **`signal_quality_expectancy_r_24h`**: Expectancy (Mean R)
- **`signal_quality_last_ts_ms`**: Timestamp of last calculation
- **`signal_quality_n_24h`**: Number of trades in calculation
- **`signal_quality_precision_top5p_24h`**: Precision at Top 5%

### Файл: `python-worker/reference/services/orderflow/tools/signal_quality_exporter_v3.py`
- **`policy_effectiveness_baseline_ok_present`**: Whether OK baseline was present (1/0)
- **`policy_effectiveness_ece_delta_24h`**: ECE delta vs OK baseline in last 24h (positive = worse calibration)
- **`policy_effectiveness_expectancy_r_delta_24h`**: Expectancy(R) delta vs OK baseline in last 24h
- **`policy_effectiveness_input_age_seconds`**: Age of last input timestamp used by report (seconds)
- **`policy_effectiveness_input_last_ts_ms`**: Last input timestamp used by report (epoch ms)
- **`policy_effectiveness_last_age_seconds`**: Age of policy effectiveness report (seconds)
- **`policy_effectiveness_last_ts_ms`**: Last policy effectiveness report timestamp (epoch ms)
- **`policy_effectiveness_precision_top5p_delta_24h`**: Precision@top5% delta vs OK baseline in last 24h
- **`policy_effectiveness_share_24h`**: Share of effective_mode in last 24h
- **`policy_effectiveness_total_n_24h`**: Total decisions in last 24h used for policy effectiveness report
- **`signal_quality_ece_24h`**: ECE over 24h
- **`signal_quality_ece_24h_by_bucket`**: ECE by cov_bucket,applied
- **`signal_quality_ece_24h_by_mode`**: ECE by drift_mode,dq_state
- **`signal_quality_expectancy_r_24h`**: Expectancy R over 24h
- **`signal_quality_expectancy_r_24h_by_bucket`**: Expectancy R by cov_bucket,applied
- **`signal_quality_expectancy_r_24h_by_mode`**: Expectancy R by drift_mode,dq_state
- **`signal_quality_last_ts_ms`**: Last close ts used in KPIs (ms)
- **`signal_quality_n_24h`**: N closed trades over 24h
- **`signal_quality_n_24h_by_bucket`**: N by cov_bucket,applied
- **`signal_quality_n_24h_by_mode`**: N by drift_mode,dq_state
- **`signal_quality_precision_top5p_24h`**: Precision@top5% over 24h
- **`signal_quality_precision_top5p_24h_by_bucket`**: Precision@top5% by cov_bucket,applied
- **`signal_quality_precision_top5p_24h_by_mode`**: Precision@top5% by drift_mode,dq_state
- **`signal_quality_staleness_sec`**: Staleness of KPIs (sec)

### Файл: `python-worker/reference/services/orderflow/tools/signal_quality_kpi_worker_v3.py`
- **`signal_quality_kpi_v3_runs_total`**: KPI v3 runs

### Файл: `python-worker/reference/services/orderflow/tools/trade_close_joiner_worker_v5.py`
- **`trade_close_joiner_close_wait_total`**: Close events sent to wait stream
- **`trade_close_joiner_events_total`**: Events processed
- **`trade_close_joiner_last_ok_ts_ms`**: Last successful join timestamp (ms)
- **`trade_close_joiner_runs_total`**: Joiner loop runs
- **`trade_close_joiner_trades_closed_dedup_total`**: Dedup drops
- **`trade_close_joiner_trades_closed_written_total`**: Closed trades written

### Файл: `python-worker/reference/services/tb_labeler_worker_v10_1.py`
- **`tb_label_input_lag_ms`**: Lag between now and input ts_ms
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written

### Файл: `python-worker/reference/services/tb_labeler_worker_v10_2.py`
- **`tb_label_input_lookup_total`**: OF input lookup mode
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written
- **`tb_of_inputs_claim_total`**: Claimed pending OF inputs
- **`tb_of_inputs_group_lag_ms`**: Approx lag between stream head and group last-delivered (ms)
- **`tb_of_inputs_group_pending`**: OF inputs consumer group pending

### Файл: `python-worker/reference/services/telegram_notifier_worker_v2.py`
- **`notify_last_err_ts_seconds`**: Timestamp of last failed send
- **`notify_last_ok_ts_seconds`**: Timestamp of last successful send
- **`notify_pending_n`**: Number of pending messages
- **`notify_queue_lag_ms`**: Time lag of messages in queue
- **`notify_receipt_latency_ms`**: Time to receive receipt
- **`notify_send_latency_ms`**: Time to send notification
- **`notify_send_total`**: Total notifications sent

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `python-worker/reference/tick_flow_full/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `python-worker/reference/utilities/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `python-worker/reference/utilities/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `python-worker/reference/utilities/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `python-worker/services/ab_winner_apply_runner.py`
- **`ab_apply_applied_total`**: Applied suggestions total
- **`ab_apply_backlog_gauge`**: Backlog estimate (seen in cycle)
- **`ab_apply_considered_total`**: Considered sids total
- **`ab_apply_errors_total`**: Apply runner errors total
- **`ab_apply_last_success_ts_ms`**: Last success ts_ms
- **`ab_apply_skipped_total`**: Skipped suggestions total

### Файл: `python-worker/services/async_signal_publisher.py`
- **`signals_publish_busy_total`**: BusyLoading Redis errors
- **`signals_publish_dropped_total`**: Signals dropped after max retries or overflow
- **`signals_publish_errors_total`**: Failed signal publishes
- **`signals_publish_ok_total`**: Successful signal publishes
- **`signals_publish_retries_enqueued_total`**: Signals queued for retry
- **`signals_publish_retries_success_total`**: Successful retries

### Файл: `python-worker/services/binance_account_reporter.py`
- **`binance_account_available_balance_usdt`**: Available balance
- **`binance_account_initial_margin_usdt`**: Initial margin
- **`binance_account_maint_margin_usdt`**: Maintenance margin
- **`binance_account_margin_balance_usdt`**: Margin balance
- **`binance_account_open_notional_usdt`**: Total absolute notional exposure
- **`binance_account_open_orders`**: Number of open orders
- **`binance_account_open_positions`**: Number of open positions
- **`binance_account_report_last_err_ts_seconds`**: Last failed report time
- **`binance_account_report_last_ok_ts_seconds`**: Last successful report time
- **`binance_account_snapshot_age_ms`**: Age of last stored snapshot
- **`binance_account_unrealized_pnl_usdt`**: Unrealized PnL
- **`binance_account_wallet_balance_usdt`**: Wallet balance

### Файл: `python-worker/services/execution_gate_service.py`
- **`exec_gate_confirmations_received_total`**: Total confirmations received
- **`exec_gate_orders_published_total`**: Total verified orders published
- **`exec_gate_pending_proposals`**: Current number of pending proposals
- **`exec_gate_proposals_received_total`**: Total signal proposals received
- **`exec_gate_telegram_notifications_total`**: Total Telegram notifications sent

### Файл: `python-worker/services/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `python-worker/services/of_confirm_service.py`
- **`of_confirm_events_received_total`**: Total events received
- **`of_confirm_signals_out_total`**: Total confirmed signals published
- **`of_confirm_signals_processed_total`**: Total signals processed

### Файл: `python-worker/services/orderflow/meta_ab_v2_report_exporter_v1.py`
- **`meta_ab_v2_action`**: one-hot action
- **`meta_ab_v2_delta_exp_r_per_candidate`**: delta exp_r_per_candidate (chall - champ)
- **`meta_ab_v2_delta_tail_rate_per_candidate`**: delta tail_rate_per_candidate (chall - champ)
- **`meta_ab_v2_last_ts_ms`**: report ts_ms
- **`meta_ab_v2_n_eligible`**: eligible rows
- **`meta_ab_v2_n_total`**: dataset rows total
- **`meta_ab_v2_p_min`**: p_min threshold
- **`meta_ab_v2_report_parse_errors_total`**: JSON parse/read errors
- **`meta_ab_v2_report_present`**: 1 if report file parsed OK
- **`meta_ab_v2_share_current`**: current challenger share
- **`meta_ab_v2_share_next`**: recommended challenger share
- **`meta_ab_v2_winner`**: one-hot winner

### Файл: `python-worker/services/orderflow/signal_quality_exporter_v1.py`
- **`signal_quality_ece_24h`**: Expected Calibration Error
- **`signal_quality_expectancy_r_24h`**: Expectancy (Mean R)
- **`signal_quality_last_ts_ms`**: Timestamp of last calculation
- **`signal_quality_n_24h`**: Number of trades in calculation
- **`signal_quality_precision_top5p_24h`**: Precision at Top 5%

### Файл: `python-worker/services/orderflow/tools/signal_quality_exporter_v3.py`
- **`policy_effectiveness_baseline_ok_present`**: Whether OK baseline was present (1/0)
- **`policy_effectiveness_ece_delta_24h`**: ECE delta vs OK baseline in last 24h (positive = worse calibration)
- **`policy_effectiveness_expectancy_r_delta_24h`**: Expectancy(R) delta vs OK baseline in last 24h
- **`policy_effectiveness_input_age_seconds`**: Age of last input timestamp used by report (seconds)
- **`policy_effectiveness_input_last_ts_ms`**: Last input timestamp used by report (epoch ms)
- **`policy_effectiveness_last_age_seconds`**: Age of policy effectiveness report (seconds)
- **`policy_effectiveness_last_ts_ms`**: Last policy effectiveness report timestamp (epoch ms)
- **`policy_effectiveness_precision_top5p_delta_24h`**: Precision@top5% delta vs OK baseline in last 24h
- **`policy_effectiveness_share_24h`**: Share of effective_mode in last 24h
- **`policy_effectiveness_total_n_24h`**: Total decisions in last 24h used for policy effectiveness report
- **`signal_quality_ece_24h`**: ECE over 24h
- **`signal_quality_ece_24h_by_bucket`**: ECE by cov_bucket,applied
- **`signal_quality_ece_24h_by_mode`**: ECE by drift_mode,dq_state
- **`signal_quality_expectancy_r_24h`**: Expectancy R over 24h
- **`signal_quality_expectancy_r_24h_by_bucket`**: Expectancy R by cov_bucket,applied
- **`signal_quality_expectancy_r_24h_by_mode`**: Expectancy R by drift_mode,dq_state
- **`signal_quality_last_ts_ms`**: Last close ts used in KPIs (ms)
- **`signal_quality_n_24h`**: N closed trades over 24h
- **`signal_quality_n_24h_by_bucket`**: N by cov_bucket,applied
- **`signal_quality_n_24h_by_mode`**: N by drift_mode,dq_state
- **`signal_quality_precision_top5p_24h`**: Precision@top5% over 24h
- **`signal_quality_precision_top5p_24h_by_bucket`**: Precision@top5% by cov_bucket,applied
- **`signal_quality_precision_top5p_24h_by_mode`**: Precision@top5% by drift_mode,dq_state
- **`signal_quality_staleness_sec`**: Staleness of KPIs (sec)

### Файл: `python-worker/services/orderflow/tools/signal_quality_kpi_worker_v3.py`
- **`signal_quality_kpi_v3_runs_total`**: KPI v3 runs

### Файл: `python-worker/services/orderflow/tools/trade_close_joiner_worker_v5.py`
- **`trade_close_joiner_close_wait_total`**: Close events sent to wait stream
- **`trade_close_joiner_events_total`**: Events processed
- **`trade_close_joiner_last_ok_ts_ms`**: Last successful join timestamp (ms)
- **`trade_close_joiner_runs_total`**: Joiner loop runs
- **`trade_close_joiner_trades_closed_dedup_total`**: Dedup drops
- **`trade_close_joiner_trades_closed_written_total`**: Closed trades written

### Файл: `python-worker/services/tb_labeler_worker_v10_1.py`
- **`tb_label_input_lag_ms`**: Lag between now and input ts_ms
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written

### Файл: `python-worker/services/tb_labeler_worker_v10_2.py`
- **`tb_label_input_lookup_total`**: OF input lookup mode
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written
- **`tb_of_inputs_claim_total`**: Claimed pending OF inputs
- **`tb_of_inputs_group_lag_ms`**: Approx lag between stream head and group last-delivered (ms)
- **`tb_of_inputs_group_pending`**: OF inputs consumer group pending

### Файл: `python-worker/services/telegram_notifier_worker_v2.py`
- **`notify_last_err_ts_seconds`**: Timestamp of last failed send
- **`notify_last_ok_ts_seconds`**: Timestamp of last successful send
- **`notify_pending_n`**: Number of pending messages
- **`notify_queue_lag_ms`**: Time lag of messages in queue
- **`notify_receipt_latency_ms`**: Time to receive receipt
- **`notify_send_latency_ms`**: Time to send notification
- **`notify_send_total`**: Total notifications sent

### Файл: `python-worker/tick_flow_full/orderflow_services/calibration_extended_exporter_v1.py`
- **`conf_cal_extended_degrade_review`**: degrade-review requested by promotion manager
- **`conf_cal_extended_delta`**: challenger - champion delta for extended calibration metrics
- **`conf_cal_extended_exporter_up`**: extended calibration exporter loop up
- **`conf_cal_extended_metric`**: extended calibration metric by arm
- **`conf_cal_extended_parse_errors_total`**: proof/status parse/shape errors
- **`conf_cal_extended_promoted_last_run`**: promotion manager promoted on last run
- **`conf_cal_extended_proof_age_sec`**: proof json age in seconds
- **`conf_cal_extended_read_errors_total`**: proof/status read errors
- **`conf_cal_extended_read_ok`**: proof/status read ok (1/0)
- **`conf_cal_extended_status_age_sec`**: status json age in seconds

### Файл: `python-worker/tick_flow_full/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `python-worker/tick_flow_full/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `python-worker/tick_flow_full/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `python-worker/tick_flow_full/orderflow_services/derivatives_context_exporter_v1.py`
- **`deriv_ctx_exporter_basis_bps`**: Basis bps
- **`deriv_ctx_exporter_errors_total`**: Exporter errors
- **`deriv_ctx_exporter_flag`**: Derivatives context flags
- **`deriv_ctx_exporter_funding_rate_z`**: Funding rate robust z-score
- **`deriv_ctx_exporter_last_snapshot_ts_ms`**: Last derivatives context snapshot ts_ms
- **`deriv_ctx_exporter_oi_notional_usd`**: OI notional USD
- **`deriv_ctx_exporter_snapshot_age_ms`**: Age of derivatives context snapshot in ms
- **`deriv_ctx_exporter_up`**: Derivatives context exporter up

### Файл: `python-worker/tick_flow_full/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `python-worker/tick_flow_full/orderflow_services/exec_health_freeze_control_exporter_v1.py`
- **`exec_health_freeze_control_effective_active`**: Effective ExecHealth freeze state (1/0)
- **`exec_health_freeze_control_exporter_up`**: 1 if exporter can read freeze control state
- **`exec_health_freeze_control_manual_ack_age_seconds`**: Age of the last manual ack in seconds
- **`exec_health_freeze_control_manual_ack_required`**: Whether manual ack is required before thaw
- **`exec_health_freeze_control_manual_freeze_total`**: Total manual freeze overrides
- **`exec_health_freeze_control_manual_override_active`**: Whether a manual operator override is active
- **`exec_health_freeze_control_source`**: One-hot current freeze source
- **`exec_health_freeze_control_state_age_seconds`**: Age of freeze control state in seconds
- **`exec_health_freeze_control_thaw_total`**: Total manual thaw acknowledgements
- **`exec_health_freeze_control_trigger_total`**: Total autoguard latches recorded in control state

### Файл: `python-worker/tick_flow_full/orderflow_services/exec_health_freeze_dual_control_exporter_v1.py`
- **`exec_health_freeze_dual_control_exporter_up`**: 1 if exporter can read dual-control state
- **`exec_health_freeze_dual_control_pending_request`**: 1 if thaw request is pending
- **`exec_health_freeze_dual_control_ready`**: 1 if request has valid prepare+approve by distinct operators
- **`exec_health_freeze_dual_control_request_age_seconds`**: Age of current thaw request in seconds
- **`exec_health_freeze_dual_control_same_operator_violation`**: 1 if preparer and approver are identical
- **`exec_health_freeze_dual_control_status`**: One-hot dual-control request status
- **`exec_health_freeze_dual_control_valid_commit_event_present`**: 1 if a valid signed commit event exists
- **`exec_health_freeze_dual_control_violation`**: One-hot dual-control violations

### Файл: `python-worker/tick_flow_full/orderflow_services/exec_health_freeze_integrity_exporter_v1.py`
- **`exec_health_freeze_integrity_control_present`**: 1 if freeze control hash is present
- **`exec_health_freeze_integrity_exporter_up`**: 1 if exporter can read freeze control integrity state
- **`exec_health_freeze_integrity_invalid_ack_event_present`**: 1 if an invalid ack event was observed
- **`exec_health_freeze_integrity_last_trigger_ts_ms`**: Latest trigger ts referenced by control/state or stream
- **`exec_health_freeze_integrity_pending_ack`**: 1 if a pending signed manual ack is still required
- **`exec_health_freeze_integrity_state_age_seconds`**: Max age of control/state hashes
- **`exec_health_freeze_integrity_state_present`**: 1 if autoguard state hash is present
- **`exec_health_freeze_integrity_valid_ack_event_present`**: 1 if a valid signed ack event exists for the current nonce
- **`exec_health_freeze_integrity_violation`**: One-hot freeze integrity violations

### Файл: `python-worker/tick_flow_full/orderflow_services/exec_health_freeze_service_identity_exporter_v1.py`
- **`exec_health_freeze_service_identity_active_connections`**: Current CLIENT LIST connections for expected ExecHealth service
- **`exec_health_freeze_service_identity_last_check_ts_ms`**: Last service identity check timestamp in epoch ms
- **`exec_health_freeze_service_identity_match`**: 1 if live CLIENT LIST matches expected identity field
- **`exec_health_freeze_service_identity_state_age_seconds`**: Age of service identity exporter state in seconds
- **`exec_health_freeze_service_identity_up`**: 1 if service identity exporter loop is healthy
- **`exec_health_freeze_service_identity_violation`**: One-hot service identity violation

### Файл: `python-worker/tick_flow_full/orderflow_services/exec_health_slo_exporter_v1.py`
- **`exec_health_slo_active_instances`**: Active ExecHealth instances by scope
- **`exec_health_slo_cross_scope_mode_distinct`**: Distinct modal modes across scopes
- **`exec_health_slo_cross_scope_threshold_distinct`**: Distinct modal thresholds across scopes
- **`exec_health_slo_exporter_up`**: 1 if exporter can read Redis summary
- **`exec_health_slo_last_age_seconds`**: Age of last SLO summary in seconds
- **`exec_health_slo_last_updated_ts_ms`**: Last SLO summary updated_ts_ms
- **`exec_health_slo_rollout_drift_instances`**: Instances with rollout drift by scope
- **`exec_health_slo_rollout_drift_instances_total`**: Total instances with rollout drift
- **`exec_health_slo_scope_deploy_distinct`**: Distinct deploy ids by scope
- **`exec_health_slo_scope_mode_distinct`**: Distinct effective modes by scope
- **`exec_health_slo_scope_threshold_distinct`**: Distinct threshold values by scope/metric
- **`exec_health_slo_share`**: ExecHealth share by scope/outcome
- **`exec_health_slo_stale_instances`**: Stale ExecHealth instances by scope
- **`exec_health_slo_stale_instances_total`**: Total stale instances

### Файл: `python-worker/tick_flow_full/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `python-worker/tick_flow_full/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `python-worker/tick_flow_full/orderflow_services/latency_contract_deploy_lint_exporter_v1.py`
- **`latency_contract_deploy_lint_errors_total`**: latest deploy lint errors count
- **`latency_contract_deploy_lint_exporter_read_ok`**: latency deploy lint exporter redis read ok
- **`latency_contract_deploy_lint_exporter_up`**: latency deploy lint exporter loop running
- **`latency_contract_deploy_lint_fail_age_seconds`**: age of current deploy lint failure streak
- **`latency_contract_deploy_lint_gate_active`**: persistent deploy lint gate active
- **`latency_contract_deploy_lint_last_checked_age_seconds`**: age of last deploy lint check
- **`latency_contract_deploy_lint_notifier_active`**: deploy lint notifier sees active persistent drift
- **`latency_contract_deploy_lint_notifier_last_run_age_seconds`**: age of deploy lint notifier last run
- **`latency_contract_deploy_lint_notifier_silenced`**: deploy lint notifier currently suppressed by silence workflow
- **`latency_contract_deploy_lint_notifier_silenced_purposes_total`**: count of currently silenced purposes in notifier state
- **`latency_contract_deploy_lint_notifier_state_present`**: deploy lint notifier state present
- **`latency_contract_deploy_lint_ok`**: latest deploy lint result ok
- **`latency_contract_deploy_lint_silence_active`**: deploy lint notifier silence active
- **`latency_contract_deploy_lint_silence_policy_denied_total`**: times silence ack was denied by policy for this purpose
- **`latency_contract_deploy_lint_silence_policy_limit_hit_total`**: times ack policy limits were hit for this purpose
- **`latency_contract_deploy_lint_silence_policy_override_active`**: current notifier silence is using escalation-ticket override
- **`latency_contract_deploy_lint_silence_policy_window_ack_count`**: ack count used in current silence policy window
- **`latency_contract_deploy_lint_silence_policy_window_budget_minutes_used`**: budget minutes used in current silence policy window
- **`latency_contract_deploy_lint_silence_remaining_seconds`**: remaining notifier silence time
- **`latency_contract_deploy_lint_silence_state_present`**: deploy lint silence state present
- **`latency_contract_deploy_lint_silence_ttl_expired`**: last silence window for this purpose expired and escalation should remain active until fixed/re-acked
- **`latency_contract_deploy_lint_silence_ttl_expired_age_seconds`**: age since notifier observed silence TTL expiry
- **`latency_contract_deploy_lint_state_present`**: deploy lint state present
- **`latency_contract_deploy_lint_summary_expired_gate_active_total`**: number of purposes with persistent deploy lint gate active after silence TTL expiry
- **`latency_contract_deploy_lint_summary_fail_total`**: number of purposes currently failing deploy lint
- **`latency_contract_deploy_lint_summary_gate_active_total`**: number of purposes with persistent deploy lint gate active
- **`latency_contract_deploy_lint_summary_policy_blocked_gate_active_total`**: number of active gate purposes where latest ack attempt was blocked by silence policy
- **`latency_contract_deploy_lint_summary_policy_override_gate_active_total`**: number of active gate purposes currently silenced via escalation-ticket override
- **`latency_contract_deploy_lint_summary_silenced_gate_active_total`**: number of purposes with persistent deploy lint gate active but silenced in notifier
- **`latency_contract_deploy_lint_summary_unsilenced_gate_active_total`**: number of purposes with persistent deploy lint gate active and not silenced in notifier
- **`latency_contract_deploy_lint_warnings_total`**: latest deploy lint warnings count

### Файл: `python-worker/tick_flow_full/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `python-worker/tick_flow_full/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `python-worker/tick_flow_full/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `python-worker/tick_flow_full/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `python-worker/tick_flow_full/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `python-worker/tick_flow_full/orderflow_services/orchestration_composite_preflight_exporter_v1.py`
- **`orchestration_composite_preflight_block_total`**: Number of purposes currently blocked
- **`orchestration_composite_preflight_decision_status`**: One-hot composite decision status
- **`orchestration_composite_preflight_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`orchestration_composite_preflight_exporter_up`**: 1 if composite preflight exporter loop is alive
- **`orchestration_composite_preflight_invalid_total`**: Number of purposes currently invalid
- **`orchestration_composite_preflight_ok_total`**: Number of purposes currently OK
- **`orchestration_composite_preflight_present_total`**: Number of purposes with persisted state
- **`orchestration_composite_preflight_purposes_total`**: Configured orchestration purposes covered by exporter
- **`orchestration_composite_preflight_selected_priority_rank`**: Priority rank of the selected composite reason
- **`orchestration_composite_preflight_selected_reason_code`**: One-hot normalized selected reason code
- **`orchestration_composite_preflight_selected_source`**: One-hot selected source dominating the composite decision
- **`orchestration_composite_preflight_soft_total`**: Number of purposes currently soft-blocked
- **`orchestration_composite_preflight_source_status`**: Per-source orchestration preflight status
- **`orchestration_composite_preflight_state_age_seconds`**: Age of latest persisted composite preflight decision
- **`orchestration_composite_preflight_state_present`**: 1 if persisted composite preflight state exists for the purpose

### Файл: `python-worker/tick_flow_full/orderflow_services/strategy_research_guard_state_exporter_v1.py`
- **`strategy_research_guard_blocker_active`**: 1 if promotion/apply blocker is active
- **`strategy_research_guard_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_guard_blocker_reason`**: One-hot blocker reason kind
- **`strategy_research_guard_chosen_variant_unique`**: 1 if latest best variant is uniquely identified
- **`strategy_research_guard_cscv_splits`**: CSCV split count used in latest report
- **`strategy_research_guard_downside_adjusted_return`**: Downside-adjusted return
- **`strategy_research_guard_dsr`**: Deflated Sharpe Ratio or conservative proxy
- **`strategy_research_guard_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`strategy_research_guard_exporter_up`**: 1 if exporter loop is alive
- **`strategy_research_guard_hit_rate_conditioned_on_cost`**: Hit-rate conditioned on cost
- **`strategy_research_guard_last_success`**: 1 if latest research guard job succeeded
- **`strategy_research_guard_last_updated_ts_ms`**: updated_ts_ms from latest research report
- **`strategy_research_guard_mean_r`**: Mean R of research sample
- **`strategy_research_guard_net_expectancy`**: Research net expectancy
- **`strategy_research_guard_pbo`**: Probability of Backtest Overfitting
- **`strategy_research_guard_precision_at_top_x`**: Precision at selected top-X bucket
- **`strategy_research_guard_primary_metric_value`**: Primary evaluator metric value
- **`strategy_research_guard_psr`**: Probabilistic Sharpe Ratio or equivalent normalized score
- **`strategy_research_guard_report_age_seconds`**: Age of latest research report in seconds
- **`strategy_research_guard_report_only`**: 1 if blocker is in report-only mode
- **`strategy_research_guard_summary_present`**: 1 if summary hash exists

### Файл: `python-worker/tick_flow_full/orderflow_services/strategy_research_stats_alert_policy_exporter_v1.py`
- **`strategy_research_stats_alert_policy_active_suppressions_total`**: Number of active TTL-backed suppress overrides by family
- **`strategy_research_stats_alert_policy_defaults_present`**: 1 if defaults hash exists
- **`strategy_research_stats_alert_policy_delta_vs_7d`**: Required 24h-vs-7d delta for purpose/family alerts
- **`strategy_research_stats_alert_policy_enabled`**: 1 if family alerting is enabled for purpose
- **`strategy_research_stats_alert_policy_exporter_up`**: 1 if alert policy exporter loop is running
- **`strategy_research_stats_alert_policy_hash_present`**: 1 if explicit purpose policy hash exists
- **`strategy_research_stats_alert_policy_min_events_24h`**: Minimum 24h events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_min_events_7d`**: Minimum 7d events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_override_active`**: 1 if a TTL-backed suppress override is active for purpose/family
- **`strategy_research_stats_alert_policy_override_budget_remaining_seconds`**: Remaining suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_budget_used_seconds`**: Cumulative suppression budget used by purpose/family override chain
- **`strategy_research_stats_alert_policy_override_created_unixtime`**: Creation time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approval_age_seconds`**: Age of approved dual-control approval for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approver_present`**: 1 if a second approver is recorded for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_freshness_remaining_seconds`**: Remaining freshness window for approved dual-control approval for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_required`**: 1 if limit-hit renewal for purpose/family requires dual-control approval
- **`strategy_research_stats_alert_policy_override_dual_control_state`**: Dual-control approval state for purpose/family
- **`strategy_research_stats_alert_policy_override_escalation_present`**: 1 if a renewal acknowledgement contains escalation fields for purpose/family
- **`strategy_research_stats_alert_policy_override_expire_unixtime`**: Expiry time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_expired_recently`**: 1 if suppress override expired recently for purpose/family
- **`strategy_research_stats_alert_policy_override_expiring_soon`**: 1 if active suppress override is within reminder window for purpose/family
- **`strategy_research_stats_alert_policy_override_last_expired_unixtime`**: Unix time of the most recent observed override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_last_reminder_unixtime`**: Unix time of the most recent expiry reminder for purpose/family
- **`strategy_research_stats_alert_policy_override_lifecycle_state`**: Lifecycle state of suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit`**: 1 if the latest suppression workflow hit a policy limit kind for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit_age_seconds`**: Age of the latest policy limit hit for purpose/family
- **`strategy_research_stats_alert_policy_override_max_budget_seconds`**: Configured max suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_max_renew_count`**: Configured max renew count for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_operator_present`**: 1 if active suppress override contains an operator for purpose/family
- **`strategy_research_stats_alert_policy_override_present`**: 1 if an override hash is present and still active for purpose/family
- **`strategy_research_stats_alert_policy_override_reason_present`**: 1 if active suppress override contains a reason for purpose/family
- **`strategy_research_stats_alert_policy_override_remaining_seconds`**: Seconds until suppress override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_age_seconds`**: Age of the current renewal acknowledgement for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_present`**: 1 if a renewal acknowledgement is currently stored for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_required`**: 1 if reminder/expiry requires explicit acknowledgement before renew for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_count`**: How many times a suppress override has been renewed for purpose/family
- **`strategy_research_stats_alert_policy_override_requires_escalation`**: 1 if policy requires escalation once limit is exceeded for purpose/family
- **`strategy_research_stats_alert_policy_override_state_present`**: 1 if persistent lifecycle state exists for purpose/family
- **`strategy_research_stats_alert_policy_override_ticket_present`**: 1 if active suppress override contains a ticket for purpose/family
- **`strategy_research_stats_alert_policy_redis_read_ok`**: 1 if alert policy exporter can read Redis
- **`strategy_research_stats_alert_policy_share_threshold_24h`**: 24h share threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_static_suppress_active`**: 1 if family alerting is statically suppressed by policy hash for purpose
- **`strategy_research_stats_alert_policy_suppress_active`**: 1 if family alerting is suppressed for purpose after TTL-aware overrides are applied

### Файл: `python-worker/tick_flow_full/orderflow_services/strategy_research_stats_exporter_v1.py`
- **`strategy_research_stats_blocker_active`**: 1 if hard blocker is active
- **`strategy_research_stats_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_stats_downside_adjusted_return`**: Downside adjusted return
- **`strategy_research_stats_dsr`**: Deflated Sharpe ratio proxy
- **`strategy_research_stats_exporter_redis_read_ok`**: 1 if exporter read redis successfully
- **`strategy_research_stats_exporter_up`**: 1 if exporter loop is running
- **`strategy_research_stats_gate_mode`**: One-hot gate mode
- **`strategy_research_stats_gate_status`**: One-hot gate status
- **`strategy_research_stats_hit_rate_conditioned_on_cost`**: Hit rate conditioned on cost
- **`strategy_research_stats_invalid_state`**: 1 if gate state is invalid
- **`strategy_research_stats_mean_r`**: Mean R
- **`strategy_research_stats_net_expectancy`**: Net expectancy
- **`strategy_research_stats_pbo`**: Probability of backtest overfitting
- **`strategy_research_stats_period_count`**: Period count used in latest report
- **`strategy_research_stats_precision_at_top_x`**: Precision@topX
- **`strategy_research_stats_primary_metric_value`**: Primary strategy research metric
- **`strategy_research_stats_psr`**: Probabilistic Sharpe ratio proxy
- **`strategy_research_stats_reason`**: One-hot blocker reason
- **`strategy_research_stats_report_age_seconds`**: Age of latest strategy research stats report
- **`strategy_research_stats_rows`**: Row count used in latest report
- **`strategy_research_stats_soft_block_active`**: 1 if soft blocker is active
- **`strategy_research_stats_summary_present`**: 1 if summary hash exists
- **`strategy_research_stats_variant_count`**: Variant count used in latest report

### Файл: `python-worker/tools/auto_apply_guard_prom_exporter_v1.py`
- **`auto_apply_guard_blocked`**: Blocked runs count
- **`auto_apply_guard_blocked_ratio`**: Ratio of blocked to total runs
- **`auto_apply_guard_blocked_reason`**: Blocked reasons count
- **`auto_apply_guard_exec_err_ratio`**: Ratio of execution errors to attempts
- **`auto_apply_guard_run_err`**: Failed runs count
- **`auto_apply_guard_run_ok`**: Successful runs count
- **`auto_apply_guard_run_total`**: Total runs count

### Файл: `python-worker/tools/auto_apply_tick_gate_blocker.py`
- **`auto_apply_tick_gate_blocked`**: 1 if auto-apply blocked by tick gate, else 0
- **`auto_apply_tick_gate_events_total`**: Gate evaluations by status
- **`auto_apply_tick_gate_fail_reasons_total`**: Fail reasons (limited labels)
- **`auto_apply_tick_gate_last_rc`**: Last tick gate return code
- **`auto_apply_tick_gate_last_run_ts_seconds`**: Last tick gate evaluation time

### Файл: `python-worker/tools/auto_apply_tick_gate_exporter.py`
- **`auto_apply_tick_gate_block_meta_age_seconds`**: Age of last block decision meta (seconds)
- **`auto_apply_tick_gate_blocked`**: Auto-apply blocked by tick gate (0/1)
- **`auto_apply_tick_gate_exporter_errors_total`**: Exporter errors total
- **`auto_apply_tick_gate_exporter_last_scrape_ts_seconds`**: Last successful scrape timestamp

### Файл: `python-worker/tools/close_backfill_replay_exporter_v1.py`
- **`close_backfill_already_joined_total`**: Already joined skipped.
- **`close_backfill_bad_payload_total`**: Bad payloads.
- **`close_backfill_close_events_total`**: Backfill found POSITION_CLOSED.
- **`close_backfill_direct_joined_total`**: Backfill direct joined into trades:closed.
- **`close_backfill_last_run_ts_ms`**: Last run timestamp (ms).
- **`close_backfill_no_sid_total`**: Close events missing sid.
- **`close_backfill_processed_total`**: Backfill scanned events.
- **`close_backfill_pushed_to_close_wait_total`**: Backfill pushed into trades:close_wait.
- **`close_backfill_seen_dedup_skipped_total`**: Seen-event dedup skipped.
- **`close_backfill_staleness_sec`**: Seconds since last run.

### Файл: `python-worker/tools/close_wait_drainer_exporter_v1.py`
- **`close_wait_dead_letter_total`**: Dead-lettered messages.
- **`close_wait_dedup_race_skipped_total`**: Dedup race skipped.
- **`close_wait_dedup_skipped_total`**: Dedup skipped.
- **`close_wait_error_total`**: Errors.
- **`close_wait_joined_total`**: Close-wait messages successfully joined.
- **`close_wait_last_run_ts_ms`**: Last run timestamp (ms).
- **`close_wait_lock_contended_total`**: Lock contended.
- **`close_wait_missing_decision_total`**: Close-wait messages missing decision.
- **`close_wait_pending_count`**: Pending entries in group.
- **`close_wait_seen_total`**: Close-wait messages seen (counter-like gauge).
- **`close_wait_staleness_sec`**: Seconds since last run.

### Файл: `python-worker/tools/confirmations_coverage_report_exporter_v1.py`
- **`confirmations_coverage_conf_bad_all_zero`**: 1 if all conf_* are zero
- **`confirmations_coverage_conf_min_nonzero_rate`**: min nonzero rate across conf_*
- **`confirmations_coverage_feat_mean`**: mean
- **`confirmations_coverage_feat_nonnull_rate`**: nonnull rate
- **`confirmations_coverage_feat_nonzero_rate`**: nonzero rate
- **`confirmations_coverage_feat_present`**: 1 if feature column present
- **`confirmations_coverage_n_rows`**: rows in dataset
- **`confirmations_coverage_reason`**: one-hot coverage reason code
- **`confirmations_coverage_report_age_sec`**: age of report in seconds
- **`confirmations_coverage_report_parsed_ok`**: 1 if report parsed
- **`confirmations_coverage_report_present`**: 1 if report file exists
- **`confirmations_coverage_report_stale`**: 1 if report stale
- **`confirmations_coverage_report_ts_ms`**: report ts_ms

### Файл: `python-worker/tools/decision_coverage_exporter_v1.py`
- **`decision_allow_rate_24h`**: Fraction of allowed decisions in last 24h
- **`decision_last_ts_ms`**: Timestamp of last processed decision
- **`decision_n_24h`**: Total count of decisions in last 24h
- **`decision_policy_mode_n_24h`**: Count of decisions by policy mode
- **`decision_policy_mode_share_24h`**: Share of decisions by policy mode
- **`decision_veto_rate_24h`**: Fraction of vetoed decisions in last 24h

### Файл: `python-worker/tools/edge_stack_shadow_exporter_v1.py`
- **`edge_stack_shadow_last_n`**: Number of samples in last eval
- **`edge_stack_shadow_last_success`**: 1 if last eval was status=ok
- **`edge_stack_shadow_last_updated_ts_ms`**: Timestamp of last update
- **`edge_stack_shadow_promoted`**: 1 if last run triggered promotion

### Файл: `python-worker/tools/edge_stack_train_exporter_v1.py`
- **`edge_stack_train_last_joined`**: Joined records in last dataset
- **`edge_stack_train_last_oof_meta_brier`**: OOF meta brier score
- **`edge_stack_train_last_oof_meta_ece`**: OOF meta ECE
- **`edge_stack_train_last_oof_meta_precision_top5pct`**: OOF meta precision@top5%
- **`edge_stack_train_last_pos_rate`**: Positive rate in last dataset
- **`edge_stack_train_last_success`**: Last edge_stack train status (1=ok)
- **`edge_stack_train_last_updated_ts_ms`**: Last edge_stack train metrics update time (ms)

### Файл: `python-worker/tools/feature_drift_exporter_v1.py`
- **`drift_n_cur_24h`**: Sample count in current window
- **`drift_n_ref_24h`**: Sample count in reference window
- **`drift_staleness_sec`**: Seconds since last drift calculation
- **`drift_state_24h`**: Drift State: 0=OK, 1=WARN, 2=BLOCK, 3=UNKNOWN
- **`feature_drift_max_z_24h`**: Max Robust Z-score across features (24h)
- **`psi_max_24h`**: Max PSI across features (24h)

### Файл: `python-worker/tools/meta_ab_v2_report_exporter_v1.py`
- **`meta_ab_v2_action`**: one-hot action
- **`meta_ab_v2_action_raw`**: one-hot raw action before policy
- **`meta_ab_v2_delta_exp_r_per_candidate`**: delta exp_r_per_candidate (chall - champ)
- **`meta_ab_v2_delta_tail_rate_per_candidate`**: delta tail_rate_per_candidate (chall - champ)
- **`meta_ab_v2_last_ts_ms`**: report ts_ms
- **`meta_ab_v2_n_eligible`**: eligible rows
- **`meta_ab_v2_n_total`**: dataset rows total
- **`meta_ab_v2_p_min`**: p_min used for eligibility
- **`meta_ab_v2_policy_allow_apply`**: 1 if policy allows apply
- **`meta_ab_v2_policy_blocked`**: 1 if policy blocked ramp/apply
- **`meta_ab_v2_policy_blocked_reason`**: one-hot policy blocked reason
- **`meta_ab_v2_report_age_sec`**: seconds since report ts_ms
- **`meta_ab_v2_report_error`**: 1 if last read had an error
- **`meta_ab_v2_report_parsed_ok`**: 1 if report file parsed OK
- **`meta_ab_v2_report_stale`**: 1 if report_age_sec > stale_after_h
- **`meta_ab_v2_run_ok`**: 1 if report does NOT contain reason
- **`meta_ab_v2_share_current`**: current challenger share
- **`meta_ab_v2_share_next`**: recommended challenger share
- **`meta_ab_v2_share_next_raw`**: raw recommended challenger share before policy
- **`meta_ab_v2_winner`**: one-hot winner

### Файл: `python-worker/tools/meta_enforce_guard_exporter_v1.py`
- **`meta_enforce_guard_freeze`**: 1 if meta enforce guardrail is active (frozen)

### Файл: `python-worker/tools/meta_promote_dir_check_v1.py`
- **`meta_promote_dir_exists`**: 1 if promote dir exists
- **`meta_promote_dir_free_bytes`**: Free bytes in promote dir filesystem
- **`meta_promote_dir_free_pct`**: Free percentage in promote dir filesystem
- **`meta_promote_dir_ok`**: 1 if promote dir is healthy and writable with space
- **`meta_promote_dir_writable`**: 1 if promote dir is writable

### Файл: `python-worker/tools/replay_inputs_archiver_exporter_v1.py`
- **`replay_inputs_archiver_archived_total`**: Total archived replay inputs.
- **`replay_inputs_archiver_bad_payload_total`**: Bad payloads (counter-like).
- **`replay_inputs_archiver_error_total`**: Archiver errors (counter-like).
- **`replay_inputs_archiver_last_run_ts_ms`**: Last run timestamp (ms).
- **`replay_inputs_archiver_no_sid_total`**: Missing sid (counter-like).
- **`replay_inputs_archiver_seen_dedup_skipped_total`**: Seen-id dedup skips (counter-like).
- **`replay_inputs_archiver_staleness_sec`**: Seconds since last run.

### Файл: `python-worker/tools/tick_gate_metrics_aggregator.py`
- **`tick_gate_events_total`**: Total tick gate events by status
- **`tick_gate_fail_reasons_total`**: Fail reasons (label-limited)
- **`tick_gate_last_run_ts_seconds`**: Unix timestamp (epoch s) of last gate run
- **`tick_gate_last_status`**: One-hot encoding of last status
- **`tick_gate_stream_lag_ms`**: Consumer lag: stream head - latest consumed (ms)

### Файл: `python-worker/utilities/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `python-worker/utilities/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `python-worker/utilities/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `reference/binance_execution/binance_account_reporter.py`
- **`binance_account_available_balance_usdt`**: Available balance
- **`binance_account_initial_margin_usdt`**: Initial margin
- **`binance_account_maint_margin_usdt`**: Maintenance margin
- **`binance_account_margin_balance_usdt`**: Margin balance
- **`binance_account_open_notional_usdt`**: Total absolute notional exposure
- **`binance_account_open_orders`**: Number of open orders
- **`binance_account_open_positions`**: Number of open positions
- **`binance_account_report_last_err_ts_seconds`**: Last failed report time
- **`binance_account_report_last_ok_ts_seconds`**: Last successful report time
- **`binance_account_snapshot_age_ms`**: Age of last stored snapshot
- **`binance_account_unrealized_pnl_usdt`**: Unrealized PnL
- **`binance_account_wallet_balance_usdt`**: Wallet balance

### Файл: `reference/liquidation_map/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `reference/liquidation_map/tick_flow_full/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `reference/ml_analysis/tools/edge_stack_train_exporter_v1.py`
- **`edge_stack_train_exporter_up`**: 1 if exporter can read Redis metrics
- **`edge_stack_train_last_age_seconds`**: Age of metrics record in seconds
- **`edge_stack_train_last_joined`**: joined count from last bundle
- **`edge_stack_train_last_oof_meta_brier`**: OOF meta brier from last bundle
- **`edge_stack_train_last_oof_meta_ece`**: OOF meta ECE from last bundle
- **`edge_stack_train_last_pos_rate`**: pos_rate from last bundle
- **`edge_stack_train_last_promote_applied`**: 1 if promotion applied in last bundle
- **`edge_stack_train_last_success`**: 1 if last bundle status is ok
- **`edge_stack_train_last_train_ok`**: 1 if train validation passed in last bundle
- **`edge_stack_train_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `reference/orderflow_services/calibration_extended_exporter_v1.py`
- **`conf_cal_extended_degrade_review`**: degrade-review requested by promotion manager
- **`conf_cal_extended_delta`**: challenger - champion delta for extended calibration metrics
- **`conf_cal_extended_exporter_up`**: extended calibration exporter loop up
- **`conf_cal_extended_metric`**: extended calibration metric by arm
- **`conf_cal_extended_parse_errors_total`**: proof/status parse/shape errors
- **`conf_cal_extended_promoted_last_run`**: promotion manager promoted on last run
- **`conf_cal_extended_proof_age_sec`**: proof json age in seconds
- **`conf_cal_extended_read_errors_total`**: proof/status read errors
- **`conf_cal_extended_read_ok`**: proof/status read ok (1/0)
- **`conf_cal_extended_status_age_sec`**: status json age in seconds

### Файл: `reference/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`bad_streak`**: consecutive degradation streak counter
- **`conf_cal_live_degrade`**: degrade decision (1/0)
- **`conf_cal_live_degrade_events_total`**: degrade decisions observed
- **`conf_cal_live_exporter_read_ok`**: exporter read succeeded (1/0)
- **`conf_cal_live_exporter_up`**: exporter loop running (1/0)
- **`conf_cal_live_last_rollback_ts_ms`**: last rollback timestamp
- **`conf_cal_live_ok`**: status ok flag (1/0)
- **`conf_cal_live_rows`**: rows in live eval window (raw)
- **`conf_cal_live_rows_cal`**: rows with calibrated values
- **`conf_cal_live_skip`**: skip_reason present (1/0)
- **`conf_cal_live_status_age_sec`**: age (now - status.ts_ms) seconds
- **`conf_cal_live_status_ts_ms`**: status ts_ms from live loop
- **`conf_cal_proof_age_sec`**: age (now - proof.ts) seconds
- **`conf_cal_proof_canary_share`**: canary share from proof controller (0..1)
- **`conf_cal_proof_evidence_age_sec`**: age (now - proof.evidence_ts) seconds
- **`conf_cal_proof_evidence_ts_sec`**: evidence ts used for freshness (seconds)
- **`conf_cal_proof_parse_errors_total`**: proof state parse/shape errors
- **`conf_cal_proof_read_errors_total`**: proof state read errors
- **`conf_cal_proof_read_ok`**: proof state read succeeded (1/0)
- **`conf_cal_proof_status_age_sec`**: status age seconds reported in proof.source
- **`conf_cal_proof_ts_sec`**: proof controller update ts (seconds)
- **`conf_cal_proof_valid`**: proof valid flag (1/0)
- **`live_brier_cal`**: live Brier on calibrated confidence
- **`live_brier_raw`**: live Brier on raw confidence
- **`live_ece_cal`**: live ECE on calibrated confidence
- **`live_ece_raw`**: live ECE on raw confidence
- **`rollback_total`**: total rollbacks observed by exporter (persisted)

### Файл: `reference/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `reference/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `reference/orderflow_services/conf_score_weight_tuning_exporter_v1.py`
- **`conf_score_tuning_last_age_seconds`**: Age (seconds) since last tuning run
- **`conf_score_tuning_last_exit_code`**: Exit code of last tuning run
- **`conf_score_tuning_last_joined_rows`**: Joined rows used for tuning
- **`conf_score_tuning_last_ok`**: 1 if last tuning job run succeeded
- **`conf_score_tuning_last_pos_rate`**: Positive label rate in last dataset
- **`conf_score_tuning_last_published`**: 1 if last run published tuning to Redis

### Файл: `reference/orderflow_services/decision_coverage_exporter_v1.py`
- **`decision_last_age_seconds`**: Age of last decision in seconds (freshness probe)
- **`decision_last_ts_ms`**: Last decision timestamp (ms)
- **`decision_n_24h`**: Number of decisions in rolling 24h window
- **`decision_regime_n_24h`**: Decisions per regime in rolling 24h window
- **`decision_regime_share_24h`**: Regime share of rolling 24h decisions [0..1]

### Файл: `reference/orderflow_services/edge_stack_shadow_status_exporter_v1.py`
- **`edge_stack_shadow_brier`**: Brier score
- **`edge_stack_shadow_champion_brier`**: Champion brier (cal, no labels)
- **`edge_stack_shadow_ece`**: ECE
- **`edge_stack_shadow_eval_age_seconds`**: Age of last status file in seconds
- **`edge_stack_shadow_eval_rows`**: Number of rows evaluated
- **`edge_stack_shadow_expectancy_r_top5pct`**: Expectancy R@top5%
- **`edge_stack_shadow_last_success`**: 1 if last shadow eval succeeded
- **`edge_stack_shadow_last_updated_ts_ms`**: Last shadow eval updated_ts_ms
- **`edge_stack_shadow_precision_top5pct`**: Precision@top5%
- **`edge_stack_shadow_promote_applied`**: 1 if this run applied promotion
- **`edge_stack_shadow_promote_recommended`**: 1 if guard recommends promotion
- **`edge_stack_shadow_status_up`**: 1 if status file is readable and not stale

### Файл: `reference/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `reference/orderflow_services/exec_health_freeze_acl_audit_exporter_v1.py`
- **`exec_health_freeze_acl_audit_last_event_ts_ms`**: Last matching ACL violation timestamp in epoch ms
- **`exec_health_freeze_acl_audit_state_age_seconds`**: Age of ACL audit exporter state in seconds
- **`exec_health_freeze_acl_audit_up`**: 1 if ExecHealth ACL audit exporter loop is healthy
- **`exec_health_freeze_acl_violation_active`**: One-hot recent ACL violation by command
- **`exec_health_freeze_acl_violation_total`**: Redis ACL violations for ExecHealth freeze-control surfaces

### Файл: `reference/orderflow_services/exec_health_freeze_client_name_audit_exporter_v1.py`
- **`exec_health_freeze_client_name_active_connections`**: Current CLIENT LIST connections per trusted ExecHealth client name
- **`exec_health_freeze_client_name_distinct_addrs`**: Distinct addr count per trusted ExecHealth client name
- **`exec_health_freeze_client_name_last_recovery_ts_ms`**: Last successful client identity recovery timestamp
- **`exec_health_freeze_client_name_match`**: 1 if trusted client-name/lib-name field matches the ExecHealth contract
- **`exec_health_freeze_client_name_policy_last_check_ts_ms`**: Last client-name policy check timestamp in epoch ms
- **`exec_health_freeze_client_name_policy_state_age_seconds`**: Age of ExecHealth client-name policy exporter state in seconds
- **`exec_health_freeze_client_name_policy_up`**: 1 if Redis-side ExecHealth client-name policy exporter loop is healthy
- **`exec_health_freeze_client_name_recovery_total`**: Self-healing recoveries after Redis reconnect identity drift
- **`exec_health_freeze_client_name_repair_failed_total`**: Failed self-healing attempts after Redis reconnect identity drift
- **`exec_health_freeze_client_name_violation`**: One-hot Redis-side client-name policy violation

### Файл: `reference/orderflow_services/exec_health_freeze_control_exporter_v1.py`
- **`exec_health_freeze_control_effective_active`**: Effective ExecHealth freeze state (1/0)
- **`exec_health_freeze_control_exporter_up`**: 1 if exporter can read freeze control state
- **`exec_health_freeze_control_manual_ack_age_seconds`**: Age of the last manual ack in seconds
- **`exec_health_freeze_control_manual_ack_required`**: Whether manual ack is required before thaw
- **`exec_health_freeze_control_manual_freeze_total`**: Total manual freeze overrides
- **`exec_health_freeze_control_manual_override_active`**: Whether a manual operator override is active
- **`exec_health_freeze_control_source`**: One-hot current freeze source
- **`exec_health_freeze_control_state_age_seconds`**: Age of freeze control state in seconds
- **`exec_health_freeze_control_thaw_total`**: Total manual thaw acknowledgements
- **`exec_health_freeze_control_trigger_total`**: Total autoguard latches recorded in control state

### Файл: `reference/orderflow_services/exec_health_freeze_dual_control_exporter_v1.py`
- **`exec_health_freeze_dual_control_exporter_up`**: 1 if exporter can read dual-control state
- **`exec_health_freeze_dual_control_pending_request`**: 1 if thaw request is pending
- **`exec_health_freeze_dual_control_ready`**: 1 if request has valid prepare+approve by distinct operators
- **`exec_health_freeze_dual_control_request_age_seconds`**: Age of current thaw request in seconds
- **`exec_health_freeze_dual_control_same_operator_violation`**: 1 if preparer and approver are identical
- **`exec_health_freeze_dual_control_status`**: One-hot dual-control request status
- **`exec_health_freeze_dual_control_valid_commit_event_present`**: 1 if a valid signed commit event exists
- **`exec_health_freeze_dual_control_violation`**: One-hot dual-control violations

### Файл: `reference/orderflow_services/exec_health_freeze_integrity_exporter_v1.py`
- **`exec_health_freeze_integrity_control_present`**: 1 if freeze control hash is present
- **`exec_health_freeze_integrity_exporter_up`**: 1 if exporter can read freeze control integrity state
- **`exec_health_freeze_integrity_invalid_ack_event_present`**: 1 if an invalid ack event was observed
- **`exec_health_freeze_integrity_last_trigger_ts_ms`**: Latest trigger ts referenced by control/state or stream
- **`exec_health_freeze_integrity_pending_ack`**: 1 if a pending signed manual ack is still required
- **`exec_health_freeze_integrity_request_log_valid_sequence`**: 1 if append-only request log has a valid prepare/approve/commit sequence
- **`exec_health_freeze_integrity_state_age_seconds`**: Max age of control/state hashes
- **`exec_health_freeze_integrity_state_present`**: 1 if autoguard state hash is present
- **`exec_health_freeze_integrity_valid_ack_event_present`**: 1 if a valid signed ack event exists for the current nonce
- **`exec_health_freeze_integrity_violation`**: One-hot freeze integrity violations

### Файл: `reference/orderflow_services/exec_health_freeze_service_identity_exporter_v1.py`
- **`exec_health_freeze_service_identity_active_connections`**: Current CLIENT LIST connections for expected ExecHealth service
- **`exec_health_freeze_service_identity_last_check_ts_ms`**: Last service identity check timestamp in epoch ms
- **`exec_health_freeze_service_identity_match`**: 1 if live CLIENT LIST matches expected identity field
- **`exec_health_freeze_service_identity_state_age_seconds`**: Age of service identity exporter state in seconds
- **`exec_health_freeze_service_identity_up`**: 1 if service identity exporter loop is healthy
- **`exec_health_freeze_service_identity_violation`**: One-hot service identity violation

### Файл: `reference/orderflow_services/exec_health_freeze_tamper_guard_v1.py`
- **`exec_health_freeze_tamper_guard_last_refreeze_ts_ms`**: Last automatic tamper refreeze timestamp
- **`exec_health_freeze_tamper_guard_refreeze_total`**: Automatic re-freeze actions due to tamper
- **`exec_health_freeze_tamper_guard_state_age_seconds`**: Age of tamper guard state hash in seconds
- **`exec_health_freeze_tamper_guard_tamper_active`**: 1 if current integrity evaluation indicates tamper requiring refreeze
- **`exec_health_freeze_tamper_guard_up`**: 1 if tamper guard loop is healthy
- **`exec_health_freeze_tamper_guard_violation`**: One-hot tamper violations seen by guard

### Файл: `reference/orderflow_services/exec_health_slo_autoguard_exporter_v1.py`
- **`exec_health_slo_autoguard_exporter_up`**: 1 if exporter can read autoguard state
- **`exec_health_slo_autoguard_freeze_active`**: Autoguard freeze active (1/0)

### Файл: `reference/orderflow_services/exec_health_slo_exporter_v1.py`
- **`exec_health_slo_active_instances`**: Active ExecHealth instances by scope
- **`exec_health_slo_cross_scope_mode_distinct`**: Distinct modal modes across scopes
- **`exec_health_slo_cross_scope_threshold_distinct`**: Distinct modal thresholds across scopes
- **`exec_health_slo_exporter_up`**: 1 if exporter can read Redis summary
- **`exec_health_slo_last_age_seconds`**: Age of last SLO summary in seconds
- **`exec_health_slo_last_updated_ts_ms`**: Last SLO summary updated_ts_ms
- **`exec_health_slo_rollout_drift_instances`**: Instances with rollout drift by scope
- **`exec_health_slo_rollout_drift_instances_total`**: Total instances with rollout drift
- **`exec_health_slo_scope_deploy_distinct`**: Distinct deploy ids by scope
- **`exec_health_slo_scope_mode_distinct`**: Distinct effective modes by scope
- **`exec_health_slo_scope_threshold_distinct`**: Distinct threshold values by scope/metric
- **`exec_health_slo_share`**: ExecHealth share by scope/outcome
- **`exec_health_slo_stale_instances`**: Stale ExecHealth instances by scope
- **`exec_health_slo_stale_instances_total`**: Total stale instances

### Файл: `reference/orderflow_services/feature_drift_batch_exporter_v1.py`
- **`feature_drift_batch_crit_n`**: Crit-level feature drift count
- **`feature_drift_batch_denylist_suggest_n`**: Features suggested for denylist AB
- **`feature_drift_batch_exporter_up`**: 1 if exporter can read Redis summary
- **`feature_drift_batch_feature_delta`**: Per-feature missing/zero/clip deltas
- **`feature_drift_batch_feature_flag`**: Per-feature drift flags
- **`feature_drift_batch_feature_ks_pvalue`**: Per-feature KS p-value
- **`feature_drift_batch_feature_ks_stat`**: Per-feature KS statistic
- **`feature_drift_batch_feature_psi`**: Per-feature PSI
- **`feature_drift_batch_features_evaluated`**: Features evaluated
- **`feature_drift_batch_features_total`**: Total features considered
- **`feature_drift_batch_last_age_seconds`**: Age of latest drift-batch summary
- **`feature_drift_batch_last_success`**: 1 if latest drift batch status is ok
- **`feature_drift_batch_last_updated_ts_ms`**: updated_ts_ms from Redis hash
- **`feature_drift_batch_shadow_disable_suggest_n`**: Features suggested for shadow disable
- **`feature_drift_batch_warn_n`**: Warn-level feature drift count
- **`feature_drift_batch_worst_ks_stat`**: Worst KS stat in latest report
- **`feature_drift_batch_worst_psi`**: Worst PSI in latest report

### Файл: `reference/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `reference/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `reference/orderflow_services/latency_contract_deploy_lint_exporter_v1.py`
- **`latency_contract_deploy_lint_errors_total`**: latest deploy lint errors count
- **`latency_contract_deploy_lint_exporter_read_ok`**: latency deploy lint exporter redis read ok
- **`latency_contract_deploy_lint_exporter_up`**: latency deploy lint exporter loop running
- **`latency_contract_deploy_lint_fail_age_seconds`**: age of current deploy lint failure streak
- **`latency_contract_deploy_lint_gate_active`**: persistent deploy lint gate active
- **`latency_contract_deploy_lint_last_checked_age_seconds`**: age of last deploy lint check
- **`latency_contract_deploy_lint_notifier_active`**: deploy lint notifier sees active persistent drift
- **`latency_contract_deploy_lint_notifier_last_run_age_seconds`**: age of deploy lint notifier last run
- **`latency_contract_deploy_lint_notifier_silenced`**: deploy lint notifier currently suppressed by silence workflow
- **`latency_contract_deploy_lint_notifier_silenced_purposes_total`**: count of currently silenced purposes in notifier state
- **`latency_contract_deploy_lint_notifier_state_present`**: deploy lint notifier state present
- **`latency_contract_deploy_lint_ok`**: latest deploy lint result ok
- **`latency_contract_deploy_lint_silence_active`**: deploy lint notifier silence active
- **`latency_contract_deploy_lint_silence_approval_age_seconds`**: age of latest override approval request
- **`latency_contract_deploy_lint_silence_approval_binding_match`**: latest override approval still matches current deploy-lint drift binding
- **`latency_contract_deploy_lint_silence_approval_binding_schema_version`**: binding schema version used by latest override approval request
- **`latency_contract_deploy_lint_silence_approval_cancelled`**: latest override approval request auto-cancelled after approval freshness elapsed
- **`latency_contract_deploy_lint_silence_approval_details_fingerprint_match`**: latest override approval still matches current deploy-lint semantic details_json fingerprint
- **`latency_contract_deploy_lint_silence_approval_expired`**: latest override approval request auto-expired before approval
- **`latency_contract_deploy_lint_silence_approval_freshness_remaining_seconds`**: remaining freshness time for latest override approval request
- **`latency_contract_deploy_lint_silence_approval_invalidated`**: latest override approval request was invalidated because deploy-lint drift changed before final ack
- **`latency_contract_deploy_lint_silence_approval_notifier_route_class_match`**: latest override approval request notifier route class still matches current state
- **`latency_contract_deploy_lint_silence_approval_pending`**: latest override approval request is prepared and awaiting second approver
- **`latency_contract_deploy_lint_silence_approval_ready`**: latest override approval request is approved and ready for requester ack
- **`latency_contract_deploy_lint_silence_approval_warning_policy_match`**: latest override approval request warning severity policy still matches current state
- **`latency_contract_deploy_lint_silence_dual_control_denied_total`**: times silence ack was denied because dual-control approval was missing or invalid
- **`latency_contract_deploy_lint_silence_dual_control_override_active`**: current notifier silence is active under an approved dual-control exception
- **`latency_contract_deploy_lint_silence_dual_control_required`**: current silence requires dual-control exception approval metadata
- **`latency_contract_deploy_lint_silence_policy_denied_total`**: times silence ack was denied by policy for this purpose
- **`latency_contract_deploy_lint_silence_policy_limit_hit_total`**: times ack policy limits were hit for this purpose
- **`latency_contract_deploy_lint_silence_policy_override_active`**: current notifier silence is using escalation-ticket override
- **`latency_contract_deploy_lint_silence_policy_window_ack_count`**: ack count used in current silence policy window
- **`latency_contract_deploy_lint_silence_policy_window_budget_minutes_used`**: budget minutes used in current silence policy window
- **`latency_contract_deploy_lint_silence_remaining_seconds`**: remaining notifier silence time
- **`latency_contract_deploy_lint_silence_state_present`**: deploy lint silence state present
- **`latency_contract_deploy_lint_silence_ttl_expired`**: last silence window for this purpose expired and escalation should remain active until fixed/re-acked
- **`latency_contract_deploy_lint_silence_ttl_expired_age_seconds`**: age since notifier observed silence TTL expiry
- **`latency_contract_deploy_lint_state_present`**: deploy lint state present
- **`latency_contract_deploy_lint_summary_dual_control_binding_mismatch_total`**: number of latest override approvals whose bound deploy-lint drift no longer matches the current drift snapshot
- **`latency_contract_deploy_lint_summary_dual_control_cancelled_gate_active_total`**: number of active gate purposes whose latest approved override request auto-cancelled before ack consumption
- **`latency_contract_deploy_lint_summary_dual_control_expired_gate_active_total`**: number of active gate purposes whose latest prepared override request auto-expired
- **`latency_contract_deploy_lint_summary_dual_control_invalidated_gate_active_total`**: number of active gate purposes whose latest override approval was invalidated because deploy-lint drift changed before final ack
- **`latency_contract_deploy_lint_summary_dual_control_override_gate_active_total`**: number of active gate purposes currently silenced under an approved dual-control exception
- **`latency_contract_deploy_lint_summary_dual_control_pending_total`**: number of purposes with pending dual-control override approval requests
- **`latency_contract_deploy_lint_summary_dual_control_ready_total`**: number of purposes with approved dual-control override requests waiting to be consumed
- **`latency_contract_deploy_lint_summary_dual_control_route_binding_mismatch_total`**: number of active gate purposes whose warning policy or notifier route class changed
- **`latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total`**: number of latest override approvals whose semantic drift binding no longer matches current gate_reason_code/errors_count/details fingerprint
- **`latency_contract_deploy_lint_summary_expired_gate_active_total`**: number of purposes with persistent deploy lint gate active after silence TTL expiry
- **`latency_contract_deploy_lint_summary_fail_total`**: number of purposes currently failing deploy lint
- **`latency_contract_deploy_lint_summary_gate_active_total`**: number of purposes with persistent deploy lint gate active
- **`latency_contract_deploy_lint_summary_policy_blocked_gate_active_total`**: number of active gate purposes where latest ack attempt was blocked by silence policy
- **`latency_contract_deploy_lint_summary_policy_override_gate_active_total`**: number of active gate purposes currently silenced via escalation-ticket override
- **`latency_contract_deploy_lint_summary_silenced_gate_active_total`**: number of purposes with persistent deploy lint gate active but silenced in notifier
- **`latency_contract_deploy_lint_summary_unsilenced_gate_active_total`**: number of purposes with persistent deploy lint gate active and not silenced in notifier
- **`latency_contract_deploy_lint_warnings_total`**: latest deploy lint warnings count

### Файл: `reference/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `reference/orderflow_services/of_gate_archiver_exporter_v1.py`
- **`of_gate_timescale_expect`**: 1 if timescaledb expected
- **`of_gate_timescale_policies_disabled`**: count of disabled required policies
- **`of_gate_timescale_policies_missing`**: count of missing required policies
- **`of_gate_timescale_policy_disabled`**: policy disabled (1/0)
- **`of_gate_timescale_policy_present`**: policy present (1/0)
- **`of_gate_timescale_present`**: 1 if timescaledb extension present

### Файл: `reference/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `reference/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `reference/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `reference/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `reference/orderflow_services/orchestration_composite_preflight_exporter_v1.py`
- **`orchestration_composite_preflight_block_total`**: Number of purposes currently blocked
- **`orchestration_composite_preflight_decision_status`**: One-hot composite decision status
- **`orchestration_composite_preflight_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`orchestration_composite_preflight_exporter_up`**: 1 if composite preflight exporter loop is alive
- **`orchestration_composite_preflight_invalid_total`**: Number of purposes currently invalid
- **`orchestration_composite_preflight_ok_total`**: Number of purposes currently OK
- **`orchestration_composite_preflight_present_total`**: Number of purposes with persisted state
- **`orchestration_composite_preflight_purposes_total`**: Configured orchestration purposes covered by exporter
- **`orchestration_composite_preflight_selected_priority_rank`**: Priority rank of the selected composite reason
- **`orchestration_composite_preflight_selected_reason_code`**: One-hot normalized selected reason code
- **`orchestration_composite_preflight_selected_source`**: One-hot selected source dominating the composite decision
- **`orchestration_composite_preflight_soft_total`**: Number of purposes currently soft-blocked
- **`orchestration_composite_preflight_source_status`**: Per-source orchestration preflight status
- **`orchestration_composite_preflight_state_age_seconds`**: Age of latest persisted composite preflight decision
- **`orchestration_composite_preflight_state_present`**: 1 if persisted composite preflight state exists for the purpose

### Файл: `reference/orderflow_services/policy_mode_exporter_p66_v1.py`
- **`policy_mode_last_age_seconds`**: Age of last policy mode decision (seconds)
- **`policy_mode_last_ts_ms`**: Last policy mode decision timestamp (ms)
- **`policy_mode_n_24h_total`**: Total decisions observed (24h)

### Файл: `reference/orderflow_services/signal_quality_regime_exporter_p66_v1.py`
- **`signal_quality_last_age_seconds`**: Age of last signal-quality calc (seconds)
- **`signal_quality_last_ts_ms`**: Timestamp of last signal-quality calc (ms)

### Файл: `reference/orderflow_services/strategy_research_guard_state_exporter_v1.py`
- **`strategy_research_guard_blocker_active`**: 1 if promotion/apply blocker is active
- **`strategy_research_guard_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_guard_blocker_reason`**: One-hot blocker reason kind
- **`strategy_research_guard_chosen_variant_unique`**: 1 if latest best variant is uniquely identified
- **`strategy_research_guard_cscv_splits`**: CSCV split count used in latest report
- **`strategy_research_guard_downside_adjusted_return`**: Downside-adjusted return
- **`strategy_research_guard_dsr`**: Deflated Sharpe Ratio or conservative proxy
- **`strategy_research_guard_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`strategy_research_guard_exporter_up`**: 1 if exporter loop is alive
- **`strategy_research_guard_hit_rate_conditioned_on_cost`**: Hit-rate conditioned on cost
- **`strategy_research_guard_last_success`**: 1 if latest research guard job succeeded
- **`strategy_research_guard_last_updated_ts_ms`**: updated_ts_ms from latest research report
- **`strategy_research_guard_mean_r`**: Mean R of research sample
- **`strategy_research_guard_net_expectancy`**: Research net expectancy
- **`strategy_research_guard_pbo`**: Probability of Backtest Overfitting
- **`strategy_research_guard_precision_at_top_x`**: Precision at selected top-X bucket
- **`strategy_research_guard_primary_metric_value`**: Primary evaluator metric value
- **`strategy_research_guard_psr`**: Probabilistic Sharpe Ratio or equivalent normalized score
- **`strategy_research_guard_report_age_seconds`**: Age of latest research report in seconds
- **`strategy_research_guard_report_only`**: 1 if blocker is in report-only mode
- **`strategy_research_guard_summary_present`**: 1 if summary hash exists

### Файл: `reference/orderflow_services/strategy_research_stats_alert_policy_exporter_v1.py`
- **`strategy_research_stats_alert_policy_active_suppressions_total`**: Number of active TTL-backed suppress overrides by family
- **`strategy_research_stats_alert_policy_defaults_present`**: 1 if defaults hash exists
- **`strategy_research_stats_alert_policy_delta_vs_7d`**: Required 24h-vs-7d delta for purpose/family alerts
- **`strategy_research_stats_alert_policy_enabled`**: 1 if family alerting is enabled for purpose
- **`strategy_research_stats_alert_policy_exporter_up`**: 1 if alert policy exporter loop is running
- **`strategy_research_stats_alert_policy_hash_present`**: 1 if explicit purpose policy hash exists
- **`strategy_research_stats_alert_policy_min_events_24h`**: Minimum 24h events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_min_events_7d`**: Minimum 7d events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_override_active`**: 1 if a TTL-backed suppress override is active for purpose/family
- **`strategy_research_stats_alert_policy_override_budget_remaining_seconds`**: Remaining suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_budget_used_seconds`**: Cumulative suppression budget used by purpose/family override chain
- **`strategy_research_stats_alert_policy_override_created_unixtime`**: Creation time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approval_age_seconds`**: Age of approved dual-control approval for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approver_present`**: 1 if a second approver is recorded for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_required`**: 1 if limit-hit renewal for purpose/family requires dual-control approval
- **`strategy_research_stats_alert_policy_override_dual_control_state`**: Dual-control approval state for purpose/family
- **`strategy_research_stats_alert_policy_override_escalation_present`**: 1 if a renewal acknowledgement contains escalation fields for purpose/family
- **`strategy_research_stats_alert_policy_override_expire_unixtime`**: Expiry time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_expired_recently`**: 1 if suppress override expired recently for purpose/family
- **`strategy_research_stats_alert_policy_override_expiring_soon`**: 1 if active suppress override is within reminder window for purpose/family
- **`strategy_research_stats_alert_policy_override_last_expired_unixtime`**: Unix time of the most recent observed override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_last_reminder_unixtime`**: Unix time of the most recent expiry reminder for purpose/family
- **`strategy_research_stats_alert_policy_override_lifecycle_state`**: Lifecycle state of suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit`**: 1 if the latest suppression workflow hit a policy limit kind for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit_age_seconds`**: Age of the latest policy limit hit for purpose/family
- **`strategy_research_stats_alert_policy_override_max_budget_seconds`**: Configured max suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_max_renew_count`**: Configured max renew count for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_operator_present`**: 1 if active suppress override contains an operator for purpose/family
- **`strategy_research_stats_alert_policy_override_present`**: 1 if an override hash is present and still active for purpose/family
- **`strategy_research_stats_alert_policy_override_reason_present`**: 1 if active suppress override contains a reason for purpose/family
- **`strategy_research_stats_alert_policy_override_remaining_seconds`**: Seconds until suppress override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_age_seconds`**: Age of the current renewal acknowledgement for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_present`**: 1 if a renewal acknowledgement is currently stored for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_required`**: 1 if reminder/expiry requires explicit acknowledgement before renew for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_count`**: How many times a suppress override has been renewed for purpose/family
- **`strategy_research_stats_alert_policy_override_requires_escalation`**: 1 if policy requires escalation once limit is exceeded for purpose/family
- **`strategy_research_stats_alert_policy_override_state_present`**: 1 if persistent lifecycle state exists for purpose/family
- **`strategy_research_stats_alert_policy_override_ticket_present`**: 1 if active suppress override contains a ticket for purpose/family
- **`strategy_research_stats_alert_policy_redis_read_ok`**: 1 if alert policy exporter can read Redis
- **`strategy_research_stats_alert_policy_share_threshold_24h`**: 24h share threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_static_suppress_active`**: 1 if family alerting is statically suppressed by policy hash for purpose
- **`strategy_research_stats_alert_policy_suppress_active`**: 1 if family alerting is suppressed for purpose after TTL-aware overrides are applied

### Файл: `reference/orderflow_services/strategy_research_stats_exporter_v1.py`
- **`strategy_research_stats_blocker_active`**: 1 if hard blocker is active
- **`strategy_research_stats_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_stats_downside_adjusted_return`**: Downside adjusted return
- **`strategy_research_stats_dsr`**: Deflated Sharpe ratio proxy
- **`strategy_research_stats_exporter_redis_read_ok`**: 1 if exporter read redis successfully
- **`strategy_research_stats_exporter_up`**: 1 if exporter loop is running
- **`strategy_research_stats_gate_mode`**: One-hot gate mode
- **`strategy_research_stats_gate_status`**: One-hot gate status
- **`strategy_research_stats_hit_rate_conditioned_on_cost`**: Hit rate conditioned on cost
- **`strategy_research_stats_invalid_state`**: 1 if gate state is invalid
- **`strategy_research_stats_mean_r`**: Mean R
- **`strategy_research_stats_net_expectancy`**: Net expectancy
- **`strategy_research_stats_pbo`**: Probability of backtest overfitting
- **`strategy_research_stats_period_count`**: Period count used in latest report
- **`strategy_research_stats_precision_at_top_x`**: Precision@topX
- **`strategy_research_stats_primary_metric_value`**: Primary strategy research metric
- **`strategy_research_stats_psr`**: Probabilistic Sharpe ratio proxy
- **`strategy_research_stats_reason`**: One-hot blocker reason
- **`strategy_research_stats_report_age_seconds`**: Age of latest strategy research stats report
- **`strategy_research_stats_rows`**: Row count used in latest report
- **`strategy_research_stats_soft_block_active`**: 1 if soft blocker is active
- **`strategy_research_stats_summary_present`**: 1 if summary hash exists
- **`strategy_research_stats_variant_count`**: Variant count used in latest report

### Файл: `reference/services/ab_winner_apply_runner.py`
- **`ab_apply_applied_total`**: Applied suggestions total
- **`ab_apply_backlog_gauge`**: Backlog estimate (seen in cycle)
- **`ab_apply_considered_total`**: Considered sids total
- **`ab_apply_errors_total`**: Apply runner errors total
- **`ab_apply_last_success_ts_ms`**: Last success ts_ms
- **`ab_apply_skipped_total`**: Skipped suggestions total

### Файл: `reference/services/async_signal_publisher.py`
- **`signals_publish_busy_total`**: BusyLoading Redis errors
- **`signals_publish_dropped_total`**: Signals dropped after max retries or overflow
- **`signals_publish_errors_total`**: Failed signal publishes
- **`signals_publish_ok_total`**: Successful signal publishes
- **`signals_publish_retries_enqueued_total`**: Signals queued for retry
- **`signals_publish_retries_success_total`**: Successful retries

### Файл: `reference/services/binance_account_reporter.py`
- **`binance_account_available_balance_usdt`**: Available balance
- **`binance_account_initial_margin_usdt`**: Initial margin
- **`binance_account_maint_margin_usdt`**: Maintenance margin
- **`binance_account_margin_balance_usdt`**: Margin balance
- **`binance_account_open_notional_usdt`**: Total absolute notional exposure
- **`binance_account_open_orders`**: Number of open orders
- **`binance_account_open_positions`**: Number of open positions
- **`binance_account_report_last_err_ts_seconds`**: Last failed report time
- **`binance_account_report_last_ok_ts_seconds`**: Last successful report time
- **`binance_account_snapshot_age_ms`**: Age of last stored snapshot
- **`binance_account_unrealized_pnl_usdt`**: Unrealized PnL
- **`binance_account_wallet_balance_usdt`**: Wallet balance

### Файл: `reference/services/execution_gate_service.py`
- **`exec_gate_confirmations_received_total`**: Total confirmations received
- **`exec_gate_orders_published_total`**: Total verified orders published
- **`exec_gate_pending_proposals`**: Current number of pending proposals
- **`exec_gate_proposals_received_total`**: Total signal proposals received
- **`exec_gate_telegram_notifications_total`**: Total Telegram notifications sent

### Файл: `reference/services/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `reference/services/of_confirm_service.py`
- **`of_confirm_events_received_total`**: Total events received
- **`of_confirm_signals_out_total`**: Total confirmed signals published
- **`of_confirm_signals_processed_total`**: Total signals processed

### Файл: `reference/services/orderflow/meta_ab_v2_report_exporter_v1.py`
- **`meta_ab_v2_action`**: one-hot action
- **`meta_ab_v2_delta_exp_r_per_candidate`**: delta exp_r_per_candidate (chall - champ)
- **`meta_ab_v2_delta_tail_rate_per_candidate`**: delta tail_rate_per_candidate (chall - champ)
- **`meta_ab_v2_last_ts_ms`**: report ts_ms
- **`meta_ab_v2_n_eligible`**: eligible rows
- **`meta_ab_v2_n_total`**: dataset rows total
- **`meta_ab_v2_p_min`**: p_min threshold
- **`meta_ab_v2_report_parse_errors_total`**: JSON parse/read errors
- **`meta_ab_v2_report_present`**: 1 if report file parsed OK
- **`meta_ab_v2_share_current`**: current challenger share
- **`meta_ab_v2_share_next`**: recommended challenger share
- **`meta_ab_v2_winner`**: one-hot winner

### Файл: `reference/services/orderflow/signal_quality_exporter_v1.py`
- **`signal_quality_ece_24h`**: Expected Calibration Error
- **`signal_quality_expectancy_r_24h`**: Expectancy (Mean R)
- **`signal_quality_last_ts_ms`**: Timestamp of last calculation
- **`signal_quality_n_24h`**: Number of trades in calculation
- **`signal_quality_precision_top5p_24h`**: Precision at Top 5%

### Файл: `reference/services/orderflow/tools/signal_quality_exporter_v3.py`
- **`policy_effectiveness_baseline_ok_present`**: Whether OK baseline was present (1/0)
- **`policy_effectiveness_ece_delta_24h`**: ECE delta vs OK baseline in last 24h (positive = worse calibration)
- **`policy_effectiveness_expectancy_r_delta_24h`**: Expectancy(R) delta vs OK baseline in last 24h
- **`policy_effectiveness_input_age_seconds`**: Age of last input timestamp used by report (seconds)
- **`policy_effectiveness_input_last_ts_ms`**: Last input timestamp used by report (epoch ms)
- **`policy_effectiveness_last_age_seconds`**: Age of policy effectiveness report (seconds)
- **`policy_effectiveness_last_ts_ms`**: Last policy effectiveness report timestamp (epoch ms)
- **`policy_effectiveness_precision_top5p_delta_24h`**: Precision@top5% delta vs OK baseline in last 24h
- **`policy_effectiveness_share_24h`**: Share of effective_mode in last 24h
- **`policy_effectiveness_total_n_24h`**: Total decisions in last 24h used for policy effectiveness report
- **`signal_quality_ece_24h`**: ECE over 24h
- **`signal_quality_ece_24h_by_bucket`**: ECE by cov_bucket,applied
- **`signal_quality_ece_24h_by_mode`**: ECE by drift_mode,dq_state
- **`signal_quality_expectancy_r_24h`**: Expectancy R over 24h
- **`signal_quality_expectancy_r_24h_by_bucket`**: Expectancy R by cov_bucket,applied
- **`signal_quality_expectancy_r_24h_by_mode`**: Expectancy R by drift_mode,dq_state
- **`signal_quality_last_ts_ms`**: Last close ts used in KPIs (ms)
- **`signal_quality_n_24h`**: N closed trades over 24h
- **`signal_quality_n_24h_by_bucket`**: N by cov_bucket,applied
- **`signal_quality_n_24h_by_mode`**: N by drift_mode,dq_state
- **`signal_quality_precision_top5p_24h`**: Precision@top5% over 24h
- **`signal_quality_precision_top5p_24h_by_bucket`**: Precision@top5% by cov_bucket,applied
- **`signal_quality_precision_top5p_24h_by_mode`**: Precision@top5% by drift_mode,dq_state
- **`signal_quality_staleness_sec`**: Staleness of KPIs (sec)

### Файл: `reference/services/orderflow/tools/signal_quality_kpi_worker_v3.py`
- **`signal_quality_kpi_v3_runs_total`**: KPI v3 runs

### Файл: `reference/services/orderflow/tools/trade_close_joiner_worker_v5.py`
- **`trade_close_joiner_close_wait_total`**: Close events sent to wait stream
- **`trade_close_joiner_events_total`**: Events processed
- **`trade_close_joiner_last_ok_ts_ms`**: Last successful join timestamp (ms)
- **`trade_close_joiner_runs_total`**: Joiner loop runs
- **`trade_close_joiner_trades_closed_dedup_total`**: Dedup drops
- **`trade_close_joiner_trades_closed_written_total`**: Closed trades written

### Файл: `reference/services/tb_labeler_worker_v10_1.py`
- **`tb_label_input_lag_ms`**: Lag between now and input ts_ms
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written

### Файл: `reference/services/tb_labeler_worker_v10_2.py`
- **`tb_label_input_lookup_total`**: OF input lookup mode
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written
- **`tb_of_inputs_claim_total`**: Claimed pending OF inputs
- **`tb_of_inputs_group_lag_ms`**: Approx lag between stream head and group last-delivered (ms)
- **`tb_of_inputs_group_pending`**: OF inputs consumer group pending

### Файл: `reference/services/telegram_notifier_worker_v2.py`
- **`notify_last_err_ts_seconds`**: Timestamp of last failed send
- **`notify_last_ok_ts_seconds`**: Timestamp of last successful send
- **`notify_pending_n`**: Number of pending messages
- **`notify_queue_lag_ms`**: Time lag of messages in queue
- **`notify_receipt_latency_ms`**: Time to receive receipt
- **`notify_send_latency_ms`**: Time to send notification
- **`notify_send_total`**: Total notifications sent

### Файл: `reference/tick_flow_full/orderflow_services/calibration_extended_exporter_v1.py`
- **`conf_cal_extended_degrade_review`**: degrade-review requested by promotion manager
- **`conf_cal_extended_delta`**: challenger - champion delta for extended calibration metrics
- **`conf_cal_extended_exporter_up`**: extended calibration exporter loop up
- **`conf_cal_extended_metric`**: extended calibration metric by arm
- **`conf_cal_extended_parse_errors_total`**: proof/status parse/shape errors
- **`conf_cal_extended_promoted_last_run`**: promotion manager promoted on last run
- **`conf_cal_extended_proof_age_sec`**: proof json age in seconds
- **`conf_cal_extended_read_errors_total`**: proof/status read errors
- **`conf_cal_extended_read_ok`**: proof/status read ok (1/0)
- **`conf_cal_extended_status_age_sec`**: status json age in seconds

### Файл: `reference/tick_flow_full/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `reference/tick_flow_full/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `reference/tick_flow_full/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `reference/tick_flow_full/orderflow_services/derivatives_context_exporter_v1.py`
- **`deriv_ctx_exporter_basis_bps`**: Basis bps
- **`deriv_ctx_exporter_errors_total`**: Exporter errors
- **`deriv_ctx_exporter_flag`**: Derivatives context flags
- **`deriv_ctx_exporter_funding_rate_z`**: Funding rate robust z-score
- **`deriv_ctx_exporter_last_snapshot_ts_ms`**: Last derivatives context snapshot ts_ms
- **`deriv_ctx_exporter_oi_notional_usd`**: OI notional USD
- **`deriv_ctx_exporter_snapshot_age_ms`**: Age of derivatives context snapshot in ms
- **`deriv_ctx_exporter_up`**: Derivatives context exporter up

### Файл: `reference/tick_flow_full/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `reference/tick_flow_full/orderflow_services/exec_health_freeze_control_exporter_v1.py`
- **`exec_health_freeze_control_effective_active`**: Effective ExecHealth freeze state (1/0)
- **`exec_health_freeze_control_exporter_up`**: 1 if exporter can read freeze control state
- **`exec_health_freeze_control_manual_ack_age_seconds`**: Age of the last manual ack in seconds
- **`exec_health_freeze_control_manual_ack_required`**: Whether manual ack is required before thaw
- **`exec_health_freeze_control_manual_freeze_total`**: Total manual freeze overrides
- **`exec_health_freeze_control_manual_override_active`**: Whether a manual operator override is active
- **`exec_health_freeze_control_source`**: One-hot current freeze source
- **`exec_health_freeze_control_state_age_seconds`**: Age of freeze control state in seconds
- **`exec_health_freeze_control_thaw_total`**: Total manual thaw acknowledgements
- **`exec_health_freeze_control_trigger_total`**: Total autoguard latches recorded in control state

### Файл: `reference/tick_flow_full/orderflow_services/exec_health_freeze_dual_control_exporter_v1.py`
- **`exec_health_freeze_dual_control_exporter_up`**: 1 if exporter can read dual-control state
- **`exec_health_freeze_dual_control_pending_request`**: 1 if thaw request is pending
- **`exec_health_freeze_dual_control_ready`**: 1 if request has valid prepare+approve by distinct operators
- **`exec_health_freeze_dual_control_request_age_seconds`**: Age of current thaw request in seconds
- **`exec_health_freeze_dual_control_same_operator_violation`**: 1 if preparer and approver are identical
- **`exec_health_freeze_dual_control_status`**: One-hot dual-control request status
- **`exec_health_freeze_dual_control_valid_commit_event_present`**: 1 if a valid signed commit event exists
- **`exec_health_freeze_dual_control_violation`**: One-hot dual-control violations

### Файл: `reference/tick_flow_full/orderflow_services/exec_health_freeze_integrity_exporter_v1.py`
- **`exec_health_freeze_integrity_control_present`**: 1 if freeze control hash is present
- **`exec_health_freeze_integrity_exporter_up`**: 1 if exporter can read freeze control integrity state
- **`exec_health_freeze_integrity_invalid_ack_event_present`**: 1 if an invalid ack event was observed
- **`exec_health_freeze_integrity_last_trigger_ts_ms`**: Latest trigger ts referenced by control/state or stream
- **`exec_health_freeze_integrity_pending_ack`**: 1 if a pending signed manual ack is still required
- **`exec_health_freeze_integrity_state_age_seconds`**: Max age of control/state hashes
- **`exec_health_freeze_integrity_state_present`**: 1 if autoguard state hash is present
- **`exec_health_freeze_integrity_valid_ack_event_present`**: 1 if a valid signed ack event exists for the current nonce
- **`exec_health_freeze_integrity_violation`**: One-hot freeze integrity violations

### Файл: `reference/tick_flow_full/orderflow_services/exec_health_freeze_service_identity_exporter_v1.py`
- **`exec_health_freeze_service_identity_active_connections`**: Current CLIENT LIST connections for expected ExecHealth service
- **`exec_health_freeze_service_identity_last_check_ts_ms`**: Last service identity check timestamp in epoch ms
- **`exec_health_freeze_service_identity_match`**: 1 if live CLIENT LIST matches expected identity field
- **`exec_health_freeze_service_identity_state_age_seconds`**: Age of service identity exporter state in seconds
- **`exec_health_freeze_service_identity_up`**: 1 if service identity exporter loop is healthy
- **`exec_health_freeze_service_identity_violation`**: One-hot service identity violation

### Файл: `reference/tick_flow_full/orderflow_services/exec_health_slo_exporter_v1.py`
- **`exec_health_slo_active_instances`**: Active ExecHealth instances by scope
- **`exec_health_slo_cross_scope_mode_distinct`**: Distinct modal modes across scopes
- **`exec_health_slo_cross_scope_threshold_distinct`**: Distinct modal thresholds across scopes
- **`exec_health_slo_exporter_up`**: 1 if exporter can read Redis summary
- **`exec_health_slo_last_age_seconds`**: Age of last SLO summary in seconds
- **`exec_health_slo_last_updated_ts_ms`**: Last SLO summary updated_ts_ms
- **`exec_health_slo_rollout_drift_instances`**: Instances with rollout drift by scope
- **`exec_health_slo_rollout_drift_instances_total`**: Total instances with rollout drift
- **`exec_health_slo_scope_deploy_distinct`**: Distinct deploy ids by scope
- **`exec_health_slo_scope_mode_distinct`**: Distinct effective modes by scope
- **`exec_health_slo_scope_threshold_distinct`**: Distinct threshold values by scope/metric
- **`exec_health_slo_share`**: ExecHealth share by scope/outcome
- **`exec_health_slo_stale_instances`**: Stale ExecHealth instances by scope
- **`exec_health_slo_stale_instances_total`**: Total stale instances

### Файл: `reference/tick_flow_full/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `reference/tick_flow_full/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `reference/tick_flow_full/orderflow_services/latency_contract_deploy_lint_exporter_v1.py`
- **`latency_contract_deploy_lint_errors_total`**: latest deploy lint errors count
- **`latency_contract_deploy_lint_exporter_read_ok`**: latency deploy lint exporter redis read ok
- **`latency_contract_deploy_lint_exporter_up`**: latency deploy lint exporter loop running
- **`latency_contract_deploy_lint_fail_age_seconds`**: age of current deploy lint failure streak
- **`latency_contract_deploy_lint_gate_active`**: persistent deploy lint gate active
- **`latency_contract_deploy_lint_last_checked_age_seconds`**: age of last deploy lint check
- **`latency_contract_deploy_lint_notifier_active`**: deploy lint notifier sees active persistent drift
- **`latency_contract_deploy_lint_notifier_last_run_age_seconds`**: age of deploy lint notifier last run
- **`latency_contract_deploy_lint_notifier_silenced`**: deploy lint notifier currently suppressed by silence workflow
- **`latency_contract_deploy_lint_notifier_silenced_purposes_total`**: count of currently silenced purposes in notifier state
- **`latency_contract_deploy_lint_notifier_state_present`**: deploy lint notifier state present
- **`latency_contract_deploy_lint_ok`**: latest deploy lint result ok
- **`latency_contract_deploy_lint_silence_active`**: deploy lint notifier silence active
- **`latency_contract_deploy_lint_silence_policy_denied_total`**: times silence ack was denied by policy for this purpose
- **`latency_contract_deploy_lint_silence_policy_limit_hit_total`**: times ack policy limits were hit for this purpose
- **`latency_contract_deploy_lint_silence_policy_override_active`**: current notifier silence is using escalation-ticket override
- **`latency_contract_deploy_lint_silence_policy_window_ack_count`**: ack count used in current silence policy window
- **`latency_contract_deploy_lint_silence_policy_window_budget_minutes_used`**: budget minutes used in current silence policy window
- **`latency_contract_deploy_lint_silence_remaining_seconds`**: remaining notifier silence time
- **`latency_contract_deploy_lint_silence_state_present`**: deploy lint silence state present
- **`latency_contract_deploy_lint_silence_ttl_expired`**: last silence window for this purpose expired and escalation should remain active until fixed/re-acked
- **`latency_contract_deploy_lint_silence_ttl_expired_age_seconds`**: age since notifier observed silence TTL expiry
- **`latency_contract_deploy_lint_state_present`**: deploy lint state present
- **`latency_contract_deploy_lint_summary_expired_gate_active_total`**: number of purposes with persistent deploy lint gate active after silence TTL expiry
- **`latency_contract_deploy_lint_summary_fail_total`**: number of purposes currently failing deploy lint
- **`latency_contract_deploy_lint_summary_gate_active_total`**: number of purposes with persistent deploy lint gate active
- **`latency_contract_deploy_lint_summary_policy_blocked_gate_active_total`**: number of active gate purposes where latest ack attempt was blocked by silence policy
- **`latency_contract_deploy_lint_summary_policy_override_gate_active_total`**: number of active gate purposes currently silenced via escalation-ticket override
- **`latency_contract_deploy_lint_summary_silenced_gate_active_total`**: number of purposes with persistent deploy lint gate active but silenced in notifier
- **`latency_contract_deploy_lint_summary_unsilenced_gate_active_total`**: number of purposes with persistent deploy lint gate active and not silenced in notifier
- **`latency_contract_deploy_lint_warnings_total`**: latest deploy lint warnings count

### Файл: `reference/tick_flow_full/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `reference/tick_flow_full/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `reference/tick_flow_full/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `reference/tick_flow_full/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `reference/tick_flow_full/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `reference/tick_flow_full/orderflow_services/orchestration_composite_preflight_exporter_v1.py`
- **`orchestration_composite_preflight_block_total`**: Number of purposes currently blocked
- **`orchestration_composite_preflight_decision_status`**: One-hot composite decision status
- **`orchestration_composite_preflight_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`orchestration_composite_preflight_exporter_up`**: 1 if composite preflight exporter loop is alive
- **`orchestration_composite_preflight_invalid_total`**: Number of purposes currently invalid
- **`orchestration_composite_preflight_ok_total`**: Number of purposes currently OK
- **`orchestration_composite_preflight_present_total`**: Number of purposes with persisted state
- **`orchestration_composite_preflight_purposes_total`**: Configured orchestration purposes covered by exporter
- **`orchestration_composite_preflight_selected_priority_rank`**: Priority rank of the selected composite reason
- **`orchestration_composite_preflight_selected_reason_code`**: One-hot normalized selected reason code
- **`orchestration_composite_preflight_selected_source`**: One-hot selected source dominating the composite decision
- **`orchestration_composite_preflight_soft_total`**: Number of purposes currently soft-blocked
- **`orchestration_composite_preflight_source_status`**: Per-source orchestration preflight status
- **`orchestration_composite_preflight_state_age_seconds`**: Age of latest persisted composite preflight decision
- **`orchestration_composite_preflight_state_present`**: 1 if persisted composite preflight state exists for the purpose

### Файл: `reference/tick_flow_full/orderflow_services/strategy_research_guard_state_exporter_v1.py`
- **`strategy_research_guard_blocker_active`**: 1 if promotion/apply blocker is active
- **`strategy_research_guard_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_guard_blocker_reason`**: One-hot blocker reason kind
- **`strategy_research_guard_chosen_variant_unique`**: 1 if latest best variant is uniquely identified
- **`strategy_research_guard_cscv_splits`**: CSCV split count used in latest report
- **`strategy_research_guard_downside_adjusted_return`**: Downside-adjusted return
- **`strategy_research_guard_dsr`**: Deflated Sharpe Ratio or conservative proxy
- **`strategy_research_guard_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`strategy_research_guard_exporter_up`**: 1 if exporter loop is alive
- **`strategy_research_guard_hit_rate_conditioned_on_cost`**: Hit-rate conditioned on cost
- **`strategy_research_guard_last_success`**: 1 if latest research guard job succeeded
- **`strategy_research_guard_last_updated_ts_ms`**: updated_ts_ms from latest research report
- **`strategy_research_guard_mean_r`**: Mean R of research sample
- **`strategy_research_guard_net_expectancy`**: Research net expectancy
- **`strategy_research_guard_pbo`**: Probability of Backtest Overfitting
- **`strategy_research_guard_precision_at_top_x`**: Precision at selected top-X bucket
- **`strategy_research_guard_primary_metric_value`**: Primary evaluator metric value
- **`strategy_research_guard_psr`**: Probabilistic Sharpe Ratio or equivalent normalized score
- **`strategy_research_guard_report_age_seconds`**: Age of latest research report in seconds
- **`strategy_research_guard_report_only`**: 1 if blocker is in report-only mode
- **`strategy_research_guard_summary_present`**: 1 if summary hash exists

### Файл: `reference/tick_flow_full/orderflow_services/strategy_research_stats_alert_policy_exporter_v1.py`
- **`strategy_research_stats_alert_policy_active_suppressions_total`**: Number of active TTL-backed suppress overrides by family
- **`strategy_research_stats_alert_policy_defaults_present`**: 1 if defaults hash exists
- **`strategy_research_stats_alert_policy_delta_vs_7d`**: Required 24h-vs-7d delta for purpose/family alerts
- **`strategy_research_stats_alert_policy_enabled`**: 1 if family alerting is enabled for purpose
- **`strategy_research_stats_alert_policy_exporter_up`**: 1 if alert policy exporter loop is running
- **`strategy_research_stats_alert_policy_hash_present`**: 1 if explicit purpose policy hash exists
- **`strategy_research_stats_alert_policy_min_events_24h`**: Minimum 24h events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_min_events_7d`**: Minimum 7d events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_override_active`**: 1 if a TTL-backed suppress override is active for purpose/family
- **`strategy_research_stats_alert_policy_override_budget_remaining_seconds`**: Remaining suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_budget_used_seconds`**: Cumulative suppression budget used by purpose/family override chain
- **`strategy_research_stats_alert_policy_override_created_unixtime`**: Creation time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approval_age_seconds`**: Age of approved dual-control approval for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_approver_present`**: 1 if a second approver is recorded for purpose/family
- **`strategy_research_stats_alert_policy_override_dual_control_required`**: 1 if limit-hit renewal for purpose/family requires dual-control approval
- **`strategy_research_stats_alert_policy_override_dual_control_state`**: Dual-control approval state for purpose/family
- **`strategy_research_stats_alert_policy_override_escalation_present`**: 1 if a renewal acknowledgement contains escalation fields for purpose/family
- **`strategy_research_stats_alert_policy_override_expire_unixtime`**: Expiry time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_expired_recently`**: 1 if suppress override expired recently for purpose/family
- **`strategy_research_stats_alert_policy_override_expiring_soon`**: 1 if active suppress override is within reminder window for purpose/family
- **`strategy_research_stats_alert_policy_override_last_expired_unixtime`**: Unix time of the most recent observed override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_last_reminder_unixtime`**: Unix time of the most recent expiry reminder for purpose/family
- **`strategy_research_stats_alert_policy_override_lifecycle_state`**: Lifecycle state of suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit`**: 1 if the latest suppression workflow hit a policy limit kind for purpose/family
- **`strategy_research_stats_alert_policy_override_limit_hit_age_seconds`**: Age of the latest policy limit hit for purpose/family
- **`strategy_research_stats_alert_policy_override_max_budget_seconds`**: Configured max suppression budget for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_max_renew_count`**: Configured max renew count for purpose/family override chain
- **`strategy_research_stats_alert_policy_override_operator_present`**: 1 if active suppress override contains an operator for purpose/family
- **`strategy_research_stats_alert_policy_override_present`**: 1 if an override hash is present and still active for purpose/family
- **`strategy_research_stats_alert_policy_override_reason_present`**: 1 if active suppress override contains a reason for purpose/family
- **`strategy_research_stats_alert_policy_override_remaining_seconds`**: Seconds until suppress override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_age_seconds`**: Age of the current renewal acknowledgement for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_present`**: 1 if a renewal acknowledgement is currently stored for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_required`**: 1 if reminder/expiry requires explicit acknowledgement before renew for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_count`**: How many times a suppress override has been renewed for purpose/family
- **`strategy_research_stats_alert_policy_override_requires_escalation`**: 1 if policy requires escalation once limit is exceeded for purpose/family
- **`strategy_research_stats_alert_policy_override_state_present`**: 1 if persistent lifecycle state exists for purpose/family
- **`strategy_research_stats_alert_policy_override_ticket_present`**: 1 if active suppress override contains a ticket for purpose/family
- **`strategy_research_stats_alert_policy_redis_read_ok`**: 1 if alert policy exporter can read Redis
- **`strategy_research_stats_alert_policy_share_threshold_24h`**: 24h share threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_static_suppress_active`**: 1 if family alerting is statically suppressed by policy hash for purpose
- **`strategy_research_stats_alert_policy_suppress_active`**: 1 if family alerting is suppressed for purpose after TTL-aware overrides are applied

### Файл: `reference/tick_flow_full/orderflow_services/strategy_research_stats_exporter_v1.py`
- **`strategy_research_stats_blocker_active`**: 1 if hard blocker is active
- **`strategy_research_stats_blocker_present`**: 1 if blocker hash exists
- **`strategy_research_stats_downside_adjusted_return`**: Downside adjusted return
- **`strategy_research_stats_dsr`**: Deflated Sharpe ratio proxy
- **`strategy_research_stats_exporter_redis_read_ok`**: 1 if exporter read redis successfully
- **`strategy_research_stats_exporter_up`**: 1 if exporter loop is running
- **`strategy_research_stats_gate_mode`**: One-hot gate mode
- **`strategy_research_stats_gate_status`**: One-hot gate status
- **`strategy_research_stats_hit_rate_conditioned_on_cost`**: Hit rate conditioned on cost
- **`strategy_research_stats_invalid_state`**: 1 if gate state is invalid
- **`strategy_research_stats_mean_r`**: Mean R
- **`strategy_research_stats_net_expectancy`**: Net expectancy
- **`strategy_research_stats_pbo`**: Probability of backtest overfitting
- **`strategy_research_stats_period_count`**: Period count used in latest report
- **`strategy_research_stats_precision_at_top_x`**: Precision@topX
- **`strategy_research_stats_primary_metric_value`**: Primary strategy research metric
- **`strategy_research_stats_psr`**: Probabilistic Sharpe ratio proxy
- **`strategy_research_stats_reason`**: One-hot blocker reason
- **`strategy_research_stats_report_age_seconds`**: Age of latest strategy research stats report
- **`strategy_research_stats_rows`**: Row count used in latest report
- **`strategy_research_stats_soft_block_active`**: 1 if soft blocker is active
- **`strategy_research_stats_summary_present`**: 1 if summary hash exists
- **`strategy_research_stats_variant_count`**: Variant count used in latest report

### Файл: `reference/utilities/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `reference/utilities/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `reference/utilities/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `tick_flow_full/orderflow_services/feature_drift_batch_exporter_v1.py`
- **`feature_drift_batch_crit_n`**: Crit-level feature drift count
- **`feature_drift_batch_denylist_suggest_n`**: Features suggested for denylist AB
- **`feature_drift_batch_exporter_up`**: 1 if exporter can read Redis summary
- **`feature_drift_batch_feature_delta`**: Per-feature missing/zero/clip deltas
- **`feature_drift_batch_feature_flag`**: Per-feature drift flags
- **`feature_drift_batch_feature_ks_pvalue`**: Per-feature KS p-value
- **`feature_drift_batch_feature_ks_stat`**: Per-feature KS statistic
- **`feature_drift_batch_feature_psi`**: Per-feature PSI
- **`feature_drift_batch_features_evaluated`**: Features evaluated
- **`feature_drift_batch_features_total`**: Total features considered
- **`feature_drift_batch_last_age_seconds`**: Age of latest drift-batch summary
- **`feature_drift_batch_last_success`**: 1 if latest drift batch status is ok
- **`feature_drift_batch_last_updated_ts_ms`**: updated_ts_ms from Redis hash
- **`feature_drift_batch_shadow_disable_suggest_n`**: Features suggested for shadow disable
- **`feature_drift_batch_warn_n`**: Warn-level feature drift count
- **`feature_drift_batch_worst_ks_stat`**: Worst KS stat in latest report
- **`feature_drift_batch_worst_psi`**: Worst PSI in latest report

### Файл: `tick_flow_full/orderflow_services/latency_contract_deploy_lint_exporter_v1.py`
- **`latency_contract_deploy_lint_errors_total`**: latest deploy lint errors count
- **`latency_contract_deploy_lint_exporter_read_ok`**: latency deploy lint exporter redis read ok
- **`latency_contract_deploy_lint_exporter_up`**: latency deploy lint exporter loop running
- **`latency_contract_deploy_lint_fail_age_seconds`**: age of current deploy lint failure streak
- **`latency_contract_deploy_lint_gate_active`**: persistent deploy lint gate active
- **`latency_contract_deploy_lint_last_checked_age_seconds`**: age of last deploy lint check
- **`latency_contract_deploy_lint_notifier_active`**: deploy lint notifier sees active persistent drift
- **`latency_contract_deploy_lint_notifier_last_run_age_seconds`**: age of deploy lint notifier last run
- **`latency_contract_deploy_lint_notifier_silenced`**: deploy lint notifier currently suppressed by silence workflow
- **`latency_contract_deploy_lint_notifier_silenced_purposes_total`**: count of currently silenced purposes in notifier state
- **`latency_contract_deploy_lint_notifier_state_present`**: deploy lint notifier state present
- **`latency_contract_deploy_lint_ok`**: latest deploy lint result ok
- **`latency_contract_deploy_lint_silence_active`**: deploy lint notifier silence active
- **`latency_contract_deploy_lint_silence_approval_age_seconds`**: age of latest override approval request
- **`latency_contract_deploy_lint_silence_approval_binding_match`**: latest override approval still matches current deploy-lint drift binding
- **`latency_contract_deploy_lint_silence_approval_binding_schema_version`**: binding schema version used by latest override approval request
- **`latency_contract_deploy_lint_silence_approval_cancelled`**: latest override approval request auto-cancelled after approval freshness elapsed
- **`latency_contract_deploy_lint_silence_approval_details_fingerprint_match`**: latest override approval still matches current deploy-lint semantic details_json fingerprint
- **`latency_contract_deploy_lint_silence_approval_expired`**: latest override approval request auto-expired before approval
- **`latency_contract_deploy_lint_silence_approval_freshness_remaining_seconds`**: remaining freshness time for latest override approval request
- **`latency_contract_deploy_lint_silence_approval_invalidated`**: latest override approval request was invalidated because deploy-lint drift changed before final ack
- **`latency_contract_deploy_lint_silence_approval_pending`**: latest override approval request is prepared and awaiting second approver
- **`latency_contract_deploy_lint_silence_approval_ready`**: latest override approval request is approved and ready for requester ack
- **`latency_contract_deploy_lint_silence_dual_control_denied_total`**: times silence ack was denied because dual-control approval was missing or invalid
- **`latency_contract_deploy_lint_silence_dual_control_override_active`**: current notifier silence is active under an approved dual-control exception
- **`latency_contract_deploy_lint_silence_dual_control_required`**: current silence requires dual-control exception approval metadata
- **`latency_contract_deploy_lint_silence_policy_denied_total`**: times silence ack was denied by policy for this purpose
- **`latency_contract_deploy_lint_silence_policy_limit_hit_total`**: times ack policy limits were hit for this purpose
- **`latency_contract_deploy_lint_silence_policy_override_active`**: current notifier silence is using escalation-ticket override
- **`latency_contract_deploy_lint_silence_policy_window_ack_count`**: ack count used in current silence policy window
- **`latency_contract_deploy_lint_silence_policy_window_budget_minutes_used`**: budget minutes used in current silence policy window
- **`latency_contract_deploy_lint_silence_remaining_seconds`**: remaining notifier silence time
- **`latency_contract_deploy_lint_silence_state_present`**: deploy lint silence state present
- **`latency_contract_deploy_lint_silence_ttl_expired`**: last silence window for this purpose expired and escalation should remain active until fixed/re-acked
- **`latency_contract_deploy_lint_silence_ttl_expired_age_seconds`**: age since notifier observed silence TTL expiry
- **`latency_contract_deploy_lint_state_present`**: deploy lint state present
- **`latency_contract_deploy_lint_summary_dual_control_binding_mismatch_total`**: number of latest override approvals whose bound deploy-lint drift no longer matches the current drift snapshot
- **`latency_contract_deploy_lint_summary_dual_control_cancelled_gate_active_total`**: number of active gate purposes whose latest approved override request auto-cancelled before ack consumption
- **`latency_contract_deploy_lint_summary_dual_control_expired_gate_active_total`**: number of active gate purposes whose latest prepared override request auto-expired
- **`latency_contract_deploy_lint_summary_dual_control_invalidated_gate_active_total`**: number of active gate purposes whose latest override approval was invalidated because deploy-lint drift changed before final ack
- **`latency_contract_deploy_lint_summary_dual_control_override_gate_active_total`**: number of active gate purposes currently silenced under an approved dual-control exception
- **`latency_contract_deploy_lint_summary_dual_control_pending_total`**: number of purposes with pending dual-control override approval requests
- **`latency_contract_deploy_lint_summary_dual_control_ready_total`**: number of purposes with approved dual-control override requests waiting to be consumed
- **`latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total`**: number of latest override approvals whose semantic drift binding no longer matches current gate_reason_code/errors_count/details fingerprint
- **`latency_contract_deploy_lint_summary_expired_gate_active_total`**: number of purposes with persistent deploy lint gate active after silence TTL expiry
- **`latency_contract_deploy_lint_summary_fail_total`**: number of purposes currently failing deploy lint
- **`latency_contract_deploy_lint_summary_gate_active_total`**: number of purposes with persistent deploy lint gate active
- **`latency_contract_deploy_lint_summary_policy_blocked_gate_active_total`**: number of active gate purposes where latest ack attempt was blocked by silence policy
- **`latency_contract_deploy_lint_summary_policy_override_gate_active_total`**: number of active gate purposes currently silenced via escalation-ticket override
- **`latency_contract_deploy_lint_summary_silenced_gate_active_total`**: number of purposes with persistent deploy lint gate active but silenced in notifier
- **`latency_contract_deploy_lint_summary_unsilenced_gate_active_total`**: number of purposes with persistent deploy lint gate active and not silenced in notifier
- **`latency_contract_deploy_lint_warnings_total`**: latest deploy lint warnings count

### Файл: `tick_flow_full/orderflow_services/orchestration_composite_preflight_exporter_v1.py`
- **`orchestration_composite_preflight_block_total`**: Number of purposes currently blocked
- **`orchestration_composite_preflight_decision_status`**: One-hot composite decision status
- **`orchestration_composite_preflight_exporter_redis_read_ok`**: 1 if exporter read Redis successfully
- **`orchestration_composite_preflight_exporter_up`**: 1 if composite preflight exporter loop is alive
- **`orchestration_composite_preflight_invalid_total`**: Number of purposes currently invalid
- **`orchestration_composite_preflight_ok_total`**: Number of purposes currently OK
- **`orchestration_composite_preflight_present_total`**: Number of purposes with persisted state
- **`orchestration_composite_preflight_purposes_total`**: Configured orchestration purposes covered by exporter
- **`orchestration_composite_preflight_selected_priority_rank`**: Priority rank of the selected composite reason
- **`orchestration_composite_preflight_selected_reason_code`**: One-hot normalized selected reason code
- **`orchestration_composite_preflight_selected_source`**: One-hot selected source dominating the composite decision
- **`orchestration_composite_preflight_soft_total`**: Number of purposes currently soft-blocked
- **`orchestration_composite_preflight_source_status`**: Per-source orchestration preflight status
- **`orchestration_composite_preflight_state_age_seconds`**: Age of latest persisted composite preflight decision
- **`orchestration_composite_preflight_state_present`**: 1 if persisted composite preflight state exists for the purpose

### Файл: `tick_flow_full/orderflow_services/strategy_research_stats_alert_policy_exporter_v1.py`
- **`strategy_research_stats_alert_policy_active_suppressions_total`**: Number of active TTL-backed suppress overrides by family
- **`strategy_research_stats_alert_policy_defaults_present`**: 1 if defaults hash exists
- **`strategy_research_stats_alert_policy_delta_vs_7d`**: Required 24h-vs-7d delta for purpose/family alerts
- **`strategy_research_stats_alert_policy_enabled`**: 1 if family alerting is enabled for purpose
- **`strategy_research_stats_alert_policy_exporter_up`**: 1 if alert policy exporter loop is running
- **`strategy_research_stats_alert_policy_hash_present`**: 1 if explicit purpose policy hash exists
- **`strategy_research_stats_alert_policy_min_events_24h`**: Minimum 24h events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_min_events_7d`**: Minimum 7d events threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_override_active`**: 1 if a TTL-backed suppress override is active for purpose/family
- **`strategy_research_stats_alert_policy_override_created_unixtime`**: Creation time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_expire_unixtime`**: Expiry time of the active suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_expired_recently`**: 1 if suppress override expired recently for purpose/family
- **`strategy_research_stats_alert_policy_override_expiring_soon`**: 1 if active suppress override is within reminder window for purpose/family
- **`strategy_research_stats_alert_policy_override_last_expired_unixtime`**: Unix time of the most recent observed override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_last_reminder_unixtime`**: Unix time of the most recent expiry reminder for purpose/family
- **`strategy_research_stats_alert_policy_override_lifecycle_state`**: Lifecycle state of suppress override for purpose/family
- **`strategy_research_stats_alert_policy_override_operator_present`**: 1 if active suppress override contains an operator for purpose/family
- **`strategy_research_stats_alert_policy_override_present`**: 1 if an override hash is present and still active for purpose/family
- **`strategy_research_stats_alert_policy_override_reason_present`**: 1 if active suppress override contains a reason for purpose/family
- **`strategy_research_stats_alert_policy_override_remaining_seconds`**: Seconds until suppress override expiry for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_age_seconds`**: Age of the current renewal acknowledgement for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_present`**: 1 if a renewal acknowledgement is currently stored for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_ack_required`**: 1 if reminder/expiry requires explicit acknowledgement before renew for purpose/family
- **`strategy_research_stats_alert_policy_override_renew_count`**: How many times a suppress override has been renewed for purpose/family
- **`strategy_research_stats_alert_policy_override_state_present`**: 1 if persistent lifecycle state exists for purpose/family
- **`strategy_research_stats_alert_policy_override_ticket_present`**: 1 if active suppress override contains a ticket for purpose/family
- **`strategy_research_stats_alert_policy_redis_read_ok`**: 1 if alert policy exporter can read Redis
- **`strategy_research_stats_alert_policy_share_threshold_24h`**: 24h share threshold for purpose/family alerts
- **`strategy_research_stats_alert_policy_static_suppress_active`**: 1 if family alerting is statically suppressed by policy hash for purpose
- **`strategy_research_stats_alert_policy_suppress_active`**: 1 if family alerting is suppressed for purpose after TTL-aware overrides are applied

### Файл: `tmp/p112_bundle/services/ab_winner_apply_runner.py`
- **`ab_apply_applied_total`**: Applied suggestions total
- **`ab_apply_backlog_gauge`**: Backlog estimate (seen in cycle)
- **`ab_apply_considered_total`**: Considered sids total
- **`ab_apply_errors_total`**: Apply runner errors total
- **`ab_apply_last_success_ts_ms`**: Last success ts_ms
- **`ab_apply_skipped_total`**: Skipped suggestions total

### Файл: `tmp/p112_bundle/services/async_signal_publisher.py`
- **`signals_publish_busy_total`**: BusyLoading Redis errors
- **`signals_publish_dropped_total`**: Signals dropped after max retries or overflow
- **`signals_publish_errors_total`**: Failed signal publishes
- **`signals_publish_ok_total`**: Successful signal publishes
- **`signals_publish_retries_enqueued_total`**: Signals queued for retry
- **`signals_publish_retries_success_total`**: Successful retries

### Файл: `tmp/p112_bundle/services/binance_account_reporter.py`
- **`binance_account_available_balance_usdt`**: Available balance
- **`binance_account_initial_margin_usdt`**: Initial margin
- **`binance_account_maint_margin_usdt`**: Maintenance margin
- **`binance_account_margin_balance_usdt`**: Margin balance
- **`binance_account_open_notional_usdt`**: Total absolute notional exposure
- **`binance_account_open_orders`**: Number of open orders
- **`binance_account_open_positions`**: Number of open positions
- **`binance_account_report_last_err_ts_seconds`**: Last failed report time
- **`binance_account_report_last_ok_ts_seconds`**: Last successful report time
- **`binance_account_snapshot_age_ms`**: Age of last stored snapshot
- **`binance_account_unrealized_pnl_usdt`**: Unrealized PnL
- **`binance_account_wallet_balance_usdt`**: Wallet balance

### Файл: `tmp/p112_bundle/services/execution_gate_service.py`
- **`exec_gate_confirmations_received_total`**: Total confirmations received
- **`exec_gate_orders_published_total`**: Total verified orders published
- **`exec_gate_pending_proposals`**: Current number of pending proposals
- **`exec_gate_proposals_received_total`**: Total signal proposals received
- **`exec_gate_telegram_notifications_total`**: Total Telegram notifications sent

### Файл: `tmp/p112_bundle/services/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `tmp/p112_bundle/services/of_confirm_service.py`
- **`of_confirm_events_received_total`**: Total events received
- **`of_confirm_signals_out_total`**: Total confirmed signals published
- **`of_confirm_signals_processed_total`**: Total signals processed

### Файл: `tmp/p112_bundle/services/orderflow/meta_ab_v2_report_exporter_v1.py`
- **`meta_ab_v2_action`**: one-hot action
- **`meta_ab_v2_delta_exp_r_per_candidate`**: delta exp_r_per_candidate (chall - champ)
- **`meta_ab_v2_delta_tail_rate_per_candidate`**: delta tail_rate_per_candidate (chall - champ)
- **`meta_ab_v2_last_ts_ms`**: report ts_ms
- **`meta_ab_v2_n_eligible`**: eligible rows
- **`meta_ab_v2_n_total`**: dataset rows total
- **`meta_ab_v2_p_min`**: p_min threshold
- **`meta_ab_v2_report_parse_errors_total`**: JSON parse/read errors
- **`meta_ab_v2_report_present`**: 1 if report file parsed OK
- **`meta_ab_v2_share_current`**: current challenger share
- **`meta_ab_v2_share_next`**: recommended challenger share
- **`meta_ab_v2_winner`**: one-hot winner

### Файл: `tmp/p112_bundle/services/orderflow/signal_quality_exporter_v1.py`
- **`signal_quality_ece_24h`**: Expected Calibration Error
- **`signal_quality_expectancy_r_24h`**: Expectancy (Mean R)
- **`signal_quality_last_ts_ms`**: Timestamp of last calculation
- **`signal_quality_n_24h`**: Number of trades in calculation
- **`signal_quality_precision_top5p_24h`**: Precision at Top 5%

### Файл: `tmp/p112_bundle/services/orderflow/tools/signal_quality_exporter_v3.py`
- **`policy_effectiveness_baseline_ok_present`**: Whether OK baseline was present (1/0)
- **`policy_effectiveness_ece_delta_24h`**: ECE delta vs OK baseline in last 24h (positive = worse calibration)
- **`policy_effectiveness_expectancy_r_delta_24h`**: Expectancy(R) delta vs OK baseline in last 24h
- **`policy_effectiveness_input_age_seconds`**: Age of last input timestamp used by report (seconds)
- **`policy_effectiveness_input_last_ts_ms`**: Last input timestamp used by report (epoch ms)
- **`policy_effectiveness_last_age_seconds`**: Age of policy effectiveness report (seconds)
- **`policy_effectiveness_last_ts_ms`**: Last policy effectiveness report timestamp (epoch ms)
- **`policy_effectiveness_precision_top5p_delta_24h`**: Precision@top5% delta vs OK baseline in last 24h
- **`policy_effectiveness_share_24h`**: Share of effective_mode in last 24h
- **`policy_effectiveness_total_n_24h`**: Total decisions in last 24h used for policy effectiveness report
- **`signal_quality_ece_24h`**: ECE over 24h
- **`signal_quality_ece_24h_by_bucket`**: ECE by cov_bucket,applied
- **`signal_quality_ece_24h_by_mode`**: ECE by drift_mode,dq_state
- **`signal_quality_expectancy_r_24h`**: Expectancy R over 24h
- **`signal_quality_expectancy_r_24h_by_bucket`**: Expectancy R by cov_bucket,applied
- **`signal_quality_expectancy_r_24h_by_mode`**: Expectancy R by drift_mode,dq_state
- **`signal_quality_last_ts_ms`**: Last close ts used in KPIs (ms)
- **`signal_quality_n_24h`**: N closed trades over 24h
- **`signal_quality_n_24h_by_bucket`**: N by cov_bucket,applied
- **`signal_quality_n_24h_by_mode`**: N by drift_mode,dq_state
- **`signal_quality_precision_top5p_24h`**: Precision@top5% over 24h
- **`signal_quality_precision_top5p_24h_by_bucket`**: Precision@top5% by cov_bucket,applied
- **`signal_quality_precision_top5p_24h_by_mode`**: Precision@top5% by drift_mode,dq_state
- **`signal_quality_staleness_sec`**: Staleness of KPIs (sec)

### Файл: `tmp/p112_bundle/services/orderflow/tools/signal_quality_kpi_worker_v3.py`
- **`signal_quality_kpi_v3_runs_total`**: KPI v3 runs

### Файл: `tmp/p112_bundle/services/orderflow/tools/trade_close_joiner_worker_v5.py`
- **`trade_close_joiner_close_wait_total`**: Close events sent to wait stream
- **`trade_close_joiner_events_total`**: Events processed
- **`trade_close_joiner_last_ok_ts_ms`**: Last successful join timestamp (ms)
- **`trade_close_joiner_runs_total`**: Joiner loop runs
- **`trade_close_joiner_trades_closed_dedup_total`**: Dedup drops
- **`trade_close_joiner_trades_closed_written_total`**: Closed trades written

### Файл: `tmp/p112_bundle/services/tb_labeler_worker_v10_1.py`
- **`tb_label_input_lag_ms`**: Lag between now and input ts_ms
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written

### Файл: `tmp/p112_bundle/services/tb_labeler_worker_v10_2.py`
- **`tb_label_input_lookup_total`**: OF input lookup mode
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written
- **`tb_of_inputs_claim_total`**: Claimed pending OF inputs
- **`tb_of_inputs_group_lag_ms`**: Approx lag between stream head and group last-delivered (ms)
- **`tb_of_inputs_group_pending`**: OF inputs consumer group pending

### Файл: `tmp/p112_bundle/services/telegram_notifier_worker_v2.py`
- **`notify_last_err_ts_seconds`**: Timestamp of last failed send
- **`notify_last_ok_ts_seconds`**: Timestamp of last successful send
- **`notify_pending_n`**: Number of pending messages
- **`notify_queue_lag_ms`**: Time lag of messages in queue
- **`notify_receipt_latency_ms`**: Time to receive receipt
- **`notify_send_latency_ms`**: Time to send notification
- **`notify_send_total`**: Total notifications sent

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/conf_cal_live_status_exporter_v1.py`
- **`conf_cal_live_brier_cal`**: Live Brier Score (Calibrated)
- **`conf_cal_live_brier_raw`**: Live Brier Score (Raw)
- **`conf_cal_live_bucket_exact_rate`**: Rate of exact bucket hits
- **`conf_cal_live_degrade`**: Degradation status (1=Bad, 0=OK)
- **`conf_cal_live_ece_cal`**: Live ECE (Calibrated Confidence)
- **`conf_cal_live_ece_raw`**: Live ECE (Raw Confidence)
- **`conf_cal_rollback_total`**: Total auto-rollbacks triggered
- **`conf_cal_status_age_seconds`**: Age of the status file

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/conf_score_guard_state_exporter_v1.py`
- **`conf_score_guard_apply_applied`**: How many symbols were applied to Redis in the last run
- **`conf_score_guard_apply_skipped`**: How many symbols were skipped (canary=0) in the last run
- **`conf_score_guard_bundle_changed_symbols`**: Count of symbols with decision changes in bundle
- **`conf_score_guard_bundle_ts_ms`**: Timestamp of the current bundle (ms)
- **`conf_score_guard_canary`**: Symbol selected for canary application (1/0)
- **`conf_score_guard_canary_symbols`**: How many symbols have canary=1
- **`conf_score_guard_drift_max_abs_z`**: Max absolute drift z across confidence parts
- **`conf_score_guard_freeze`**: Guardrails freeze active (1/0)
- **`conf_score_guard_latch_remaining_sec`**: Remaining freeze latch time (sec)
- **`conf_score_guard_promote_last_age_seconds`**: Age of current.json in seconds
- **`conf_score_guard_promote_last_ok`**: 1 if current.json is valid and recent
- **`conf_score_guard_scale`**: Guardrails confidence scale
- **`conf_score_guard_stable_streak`**: Consecutive stable runs (for recovery)
- **`conf_score_guard_stage_pointer_age_seconds`**: Age of staged.json in seconds
- **`conf_score_guard_stage_present`**: 1 if staged.json exists
- **`conf_score_guard_status_age_seconds`**: Age of the guard state file (seconds)
- **`conf_score_guard_symbols`**: How many symbols are present in the state file

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/conf_score_guardrails_autopromo_exporter_v1.py`
- **`conf_score_guard_autopromo_arm_delta_brier_cal`**: Paired delta brier_cal (challenger - champion)
- **`conf_score_guard_autopromo_arm_delta_ece_cal`**: Paired delta ece_cal (challenger - champion)
- **`conf_score_guard_autopromo_blocked`**: Autopromo blocked state (1 blocked)
- **`conf_score_guard_autopromo_candidate_active`**: Active candidate marker
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_max`**: Matched-cohort worst delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_brier_cal_wmean`**: Matched-cohort weighted mean delta brier_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_max`**: Matched-cohort worst delta ece_cal
- **`conf_score_guard_autopromo_cohort_delta_ece_cal_wmean`**: Matched-cohort weighted mean delta ece_cal
- **`conf_score_guard_autopromo_delta_brier_cal`**: Delta brier_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_delta_ece_cal`**: Delta ece_cal vs baseline (canary eval)
- **`conf_score_guard_autopromo_last_eval_ok`**: Last canary eval verdict (1 ok, 0 fail)
- **`conf_score_guard_autopromo_observing_remaining_sec`**: Remaining observe window in seconds (if observing)
- **`conf_score_guard_autopromo_phase`**: Autopromo phase code (see runbook)
- **`conf_score_guard_autopromo_state_age_sec`**: Age of autopromo state file in seconds

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/enforce_bucket_state_exporter_v1.py`
- **`of_enforce_state_exporter_up`**: exporter loop running (1/0)

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/feature_registry_contract_exporter_v1.py`
- **`feature_registry_contract_exporter_up`**: 1 if exporter can read Redis metrics
- **`feature_registry_contract_last_age_seconds`**: Age of metrics record in seconds
- **`feature_registry_contract_last_feature_cols_hash_mismatch`**: 1 if feature_cols_hash mismatches pinned
- **`feature_registry_contract_last_pins_present`**: 1 if pins are present in cfg hash
- **`feature_registry_contract_last_schema_hash_mismatch`**: 1 if schema_hash mismatches pinned
- **`feature_registry_contract_last_schema_ver_mismatch`**: 1 if schema_ver mismatches pinned
- **`feature_registry_contract_last_success`**: 1 if last contract check is ok
- **`feature_registry_contract_last_updated_ts_ms`**: updated_ts_ms from Redis hash

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/golden_replay_capture_exporter_p112.py`
- **`golden_replay_last_ok_day`**: Last clean day (YYYYMMDD) as integer, 0 if unknown
- **`golden_replay_mismatches_total`**: Total mismatches across all policies in latest run
- **`golden_replay_policy_cnt`**: Number of policy groups processed in latest nightly run
- **`golden_replay_state_updated_ts_ms`**: State update timestamp (ms)
- **`gr_capture_exporter_scrape_errors_total`**: Exporter scrape errors
- **`ofc_capture_bytes_total`**: Captured NDJSON bytes written
- **`ofc_capture_errors_total`**: Capture write errors
- **`ofc_capture_last_error_ts_ms`**: Last capture error timestamp (ms)
- **`ofc_capture_last_write_ts_ms`**: Last successful capture write timestamp (ms)
- **`ofc_capture_sampled_out_total`**: Records skipped by sampler
- **`ofc_capture_written_total`**: Captured NDJSON records written

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/meta_cov_rollout_exporter_v1.py`
- **`auto_apply_block`**: 1 if auto-apply is blocked by reason
- **`auto_apply_block_age_s`**: Age of block in seconds by reason
- **`auto_apply_block_ts_ms`**: Timestamp of block by reason
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_decision`**: Last decision code (1 for current code)
- **`meta_cov_ops_last_decision_age_s`**: Age of last decision in seconds
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage
- **`signal_quality_ece_24h`**: Signal quality ECE (24h)
- **`signal_quality_ece_24h_by_regime`**: ECE over last 24h by regime
- **`signal_quality_expectancy_r_24h`**: Signal quality Expectancy R (24h)
- **`signal_quality_expectancy_r_24h_by_regime`**: Mean R over last 24h by regime
- **`signal_quality_last_ts_ms`**: Last KPI compute timestamp (ms)
- **`signal_quality_n_24h`**: Signal quality N trades (24h)
- **`signal_quality_n_24h_by_regime`**: N over last 24h by regime
- **`signal_quality_precision_top5p_24h`**: Signal quality Precision @ Top 5% (24h)
- **`signal_quality_precision_top5p_24h_by_regime`**: Win rate in top 5%% by score over last 24h by regime
- **`signal_quality_staleness_sec`**: How old the KPI snapshot is (seconds)

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/of_gate_contract_smoke_exporter_v1.py`
- **`of_gate_contract_smoke_bad_share`**: Share of invalid rows in metrics:of_gate tail
- **`of_gate_contract_smoke_bad_total`**: Invalid rows count in metrics:of_gate tail
- **`of_gate_contract_smoke_dq_bad_total`**: Top DQ codes among invalid rows
- **`of_gate_contract_smoke_last_ts_ms`**: Timestamp of the last smoke-check record (ms)
- **`of_gate_contract_smoke_missing_schema_share`**: Share of rows missing schema markers
- **`of_gate_contract_smoke_n_total`**: Total rows sampled from metrics:of_gate
- **`of_gate_contract_smoke_reason_code_bad_total`**: Top reason_code among invalid rows
- **`of_gate_contract_smoke_reason_code_total`**: Top reason_code among all rows
- **`of_gate_contract_smoke_schema_version_mode`**: Mode of schema_version (int) in sampled rows

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/of_gate_dlq_exporter_v1.py`
- **`of_gate_dlq_exporter_up`**: DLQ exporter loop running (1/0)
- **`of_gate_dlq_len`**: DLQ stream length

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/of_inputs_dlq_exporter_v1.py`
- **`of_inputs_dlq_age_sec`**: Age in seconds: now - last_id_ts_ms/1000
- **`of_inputs_dlq_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_dlq_exporter_up`**: 1 if exporter loop is running
- **`of_inputs_dlq_last_id_ts_ms`**: Last entry timestamp derived from stream id
- **`of_inputs_dlq_len`**: Redis stream length
- **`of_inputs_dlq_replay_last_dur_ms`**: Duration of last replay run (ms)
- **`of_inputs_dlq_replay_last_failed`**: How many messages failed in last run
- **`of_inputs_dlq_replay_last_ok`**: 1 if last replay run succeeded
- **`of_inputs_dlq_replay_last_ok_age_sec`**: Age since last successful replay run (sec)
- **`of_inputs_dlq_replay_last_ok_ts_ms`**: Timestamp of last successful replay run (ms)
- **`of_inputs_dlq_replay_last_replayed`**: How many messages were replayed in last run
- **`of_inputs_dlq_replay_last_run_age_sec`**: Age since last replay run (sec)
- **`of_inputs_dlq_replay_last_run_ok`**: 1 if last replay run succeeded (regardless of previous success)
- **`of_inputs_dlq_replay_last_run_ts_ms`**: Timestamp of last replay run (ms)
- **`of_inputs_dlq_replay_last_skipped`**: How many messages were skipped in last run

### Файл: `tmp/p112_bundle/tick_flow_full/orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`
- **`of_inputs_v3_circuit_state_exporter_errors_total`**: Cumulative exporter loop errors
- **`of_inputs_v3_circuit_state_exporter_poll_ts_ms`**: Last poll timestamp (ms)
- **`of_inputs_v3_circuit_state_exporter_up`**: 1 if exporter loop is running

### Файл: `tmp/p112_bundle/tick_flow_full/services/ab_winner_apply_runner.py`
- **`ab_apply_applied_total`**: Applied suggestions total
- **`ab_apply_backlog_gauge`**: Backlog estimate (seen in cycle)
- **`ab_apply_considered_total`**: Considered sids total
- **`ab_apply_errors_total`**: Apply runner errors total
- **`ab_apply_last_success_ts_ms`**: Last success ts_ms
- **`ab_apply_skipped_total`**: Skipped suggestions total

### Файл: `tmp/p112_bundle/tick_flow_full/services/async_signal_publisher.py`
- **`signals_publish_busy_total`**: BusyLoading Redis errors
- **`signals_publish_dropped_total`**: Signals dropped after max retries or overflow
- **`signals_publish_errors_total`**: Failed signal publishes
- **`signals_publish_ok_total`**: Successful signal publishes
- **`signals_publish_retries_enqueued_total`**: Signals queued for retry
- **`signals_publish_retries_success_total`**: Successful retries

### Файл: `tmp/p112_bundle/tick_flow_full/services/binance_account_reporter.py`
- **`binance_account_available_balance_usdt`**: Available balance
- **`binance_account_initial_margin_usdt`**: Initial margin
- **`binance_account_maint_margin_usdt`**: Maintenance margin
- **`binance_account_margin_balance_usdt`**: Margin balance
- **`binance_account_open_notional_usdt`**: Total absolute notional exposure
- **`binance_account_open_orders`**: Number of open orders
- **`binance_account_open_positions`**: Number of open positions
- **`binance_account_report_last_err_ts_seconds`**: Last failed report time
- **`binance_account_report_last_ok_ts_seconds`**: Last successful report time
- **`binance_account_snapshot_age_ms`**: Age of last stored snapshot
- **`binance_account_unrealized_pnl_usdt`**: Unrealized PnL
- **`binance_account_wallet_balance_usdt`**: Wallet balance

### Файл: `tmp/p112_bundle/tick_flow_full/services/execution_gate_service.py`
- **`exec_gate_confirmations_received_total`**: Total confirmations received
- **`exec_gate_orders_published_total`**: Total verified orders published
- **`exec_gate_pending_proposals`**: Current number of pending proposals
- **`exec_gate_proposals_received_total`**: Total signal proposals received
- **`exec_gate_telegram_notifications_total`**: Total Telegram notifications sent

### Файл: `tmp/p112_bundle/tick_flow_full/services/liquidation_map_service.py`
- **`liqmap_evt_dlq_total`**: Total liquidation events sent to DLQ
- **`liqmap_evt_drop_total`**: Total liquidation events dropped
- **`liqmap_evt_ok_total`**: Total liquidation events accepted
- **`liqmap_evt_read_total`**: Total liquidation events read
- **`liqmap_last_event_ts_ms`**: Last accepted event ts_event (ms)
- **`liqmap_last_publish_ts_ms`**: Last publish wallclock ts (ms)
- **`liqmap_levels`**: Number of levels in last snapshot
- **`liqmap_snapshot_bytes`**: Snapshot JSON bytes
- **`liqmap_snapshot_total`**: Snapshots published

### Файл: `tmp/p112_bundle/tick_flow_full/services/of_confirm_service.py`
- **`of_confirm_events_received_total`**: Total events received
- **`of_confirm_signals_out_total`**: Total confirmed signals published
- **`of_confirm_signals_processed_total`**: Total signals processed

### Файл: `tmp/p112_bundle/tick_flow_full/services/orderflow/meta_ab_v2_report_exporter_v1.py`
- **`meta_ab_v2_action`**: one-hot action
- **`meta_ab_v2_delta_exp_r_per_candidate`**: delta exp_r_per_candidate (chall - champ)
- **`meta_ab_v2_delta_tail_rate_per_candidate`**: delta tail_rate_per_candidate (chall - champ)
- **`meta_ab_v2_last_ts_ms`**: report ts_ms
- **`meta_ab_v2_n_eligible`**: eligible rows
- **`meta_ab_v2_n_total`**: dataset rows total
- **`meta_ab_v2_p_min`**: p_min threshold
- **`meta_ab_v2_report_parse_errors_total`**: JSON parse/read errors
- **`meta_ab_v2_report_present`**: 1 if report file parsed OK
- **`meta_ab_v2_share_current`**: current challenger share
- **`meta_ab_v2_share_next`**: recommended challenger share
- **`meta_ab_v2_winner`**: one-hot winner

### Файл: `tmp/p112_bundle/tick_flow_full/services/orderflow/signal_quality_exporter_v1.py`
- **`signal_quality_ece_24h`**: Expected Calibration Error
- **`signal_quality_expectancy_r_24h`**: Expectancy (Mean R)
- **`signal_quality_last_ts_ms`**: Timestamp of last calculation
- **`signal_quality_n_24h`**: Number of trades in calculation
- **`signal_quality_precision_top5p_24h`**: Precision at Top 5%

### Файл: `tmp/p112_bundle/tick_flow_full/services/orderflow/tools/signal_quality_exporter_v3.py`
- **`policy_effectiveness_baseline_ok_present`**: Whether OK baseline was present (1/0)
- **`policy_effectiveness_ece_delta_24h`**: ECE delta vs OK baseline in last 24h (positive = worse calibration)
- **`policy_effectiveness_expectancy_r_delta_24h`**: Expectancy(R) delta vs OK baseline in last 24h
- **`policy_effectiveness_input_age_seconds`**: Age of last input timestamp used by report (seconds)
- **`policy_effectiveness_input_last_ts_ms`**: Last input timestamp used by report (epoch ms)
- **`policy_effectiveness_last_age_seconds`**: Age of policy effectiveness report (seconds)
- **`policy_effectiveness_last_ts_ms`**: Last policy effectiveness report timestamp (epoch ms)
- **`policy_effectiveness_precision_top5p_delta_24h`**: Precision@top5% delta vs OK baseline in last 24h
- **`policy_effectiveness_share_24h`**: Share of effective_mode in last 24h
- **`policy_effectiveness_total_n_24h`**: Total decisions in last 24h used for policy effectiveness report
- **`signal_quality_ece_24h`**: ECE over 24h
- **`signal_quality_ece_24h_by_bucket`**: ECE by cov_bucket,applied
- **`signal_quality_ece_24h_by_mode`**: ECE by drift_mode,dq_state
- **`signal_quality_expectancy_r_24h`**: Expectancy R over 24h
- **`signal_quality_expectancy_r_24h_by_bucket`**: Expectancy R by cov_bucket,applied
- **`signal_quality_expectancy_r_24h_by_mode`**: Expectancy R by drift_mode,dq_state
- **`signal_quality_last_ts_ms`**: Last close ts used in KPIs (ms)
- **`signal_quality_n_24h`**: N closed trades over 24h
- **`signal_quality_n_24h_by_bucket`**: N by cov_bucket,applied
- **`signal_quality_n_24h_by_mode`**: N by drift_mode,dq_state
- **`signal_quality_precision_top5p_24h`**: Precision@top5% over 24h
- **`signal_quality_precision_top5p_24h_by_bucket`**: Precision@top5% by cov_bucket,applied
- **`signal_quality_precision_top5p_24h_by_mode`**: Precision@top5% by drift_mode,dq_state
- **`signal_quality_staleness_sec`**: Staleness of KPIs (sec)

### Файл: `tmp/p112_bundle/tick_flow_full/services/orderflow/tools/signal_quality_kpi_worker_v3.py`
- **`signal_quality_kpi_v3_runs_total`**: KPI v3 runs

### Файл: `tmp/p112_bundle/tick_flow_full/services/orderflow/tools/trade_close_joiner_worker_v5.py`
- **`trade_close_joiner_close_wait_total`**: Close events sent to wait stream
- **`trade_close_joiner_events_total`**: Events processed
- **`trade_close_joiner_last_ok_ts_ms`**: Last successful join timestamp (ms)
- **`trade_close_joiner_runs_total`**: Joiner loop runs
- **`trade_close_joiner_trades_closed_dedup_total`**: Dedup drops
- **`trade_close_joiner_trades_closed_written_total`**: Closed trades written

### Файл: `tmp/p112_bundle/tick_flow_full/services/tb_labeler_worker_v10_1.py`
- **`tb_label_input_lag_ms`**: Lag between now and input ts_ms
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written

### Файл: `tmp/p112_bundle/tick_flow_full/services/tb_labeler_worker_v10_2.py`
- **`tb_label_input_lookup_total`**: OF input lookup mode
- **`tb_label_jobs_total`**: TB labeler jobs processed
- **`tb_label_write_total`**: TB labels written
- **`tb_of_inputs_claim_total`**: Claimed pending OF inputs
- **`tb_of_inputs_group_lag_ms`**: Approx lag between stream head and group last-delivered (ms)
- **`tb_of_inputs_group_pending`**: OF inputs consumer group pending

### Файл: `tmp/p112_bundle/tick_flow_full/services/telegram_notifier_worker_v2.py`
- **`notify_last_err_ts_seconds`**: Timestamp of last failed send
- **`notify_last_ok_ts_seconds`**: Timestamp of last successful send
- **`notify_pending_n`**: Number of pending messages
- **`notify_queue_lag_ms`**: Time lag of messages in queue
- **`notify_receipt_latency_ms`**: Time to receive receipt
- **`notify_send_latency_ms`**: Time to send notification
- **`notify_send_total`**: Total notifications sent

### Файл: `tools/edge_stack_shadow_exporter_v1.py`
- **`edge_stack_shadow_last_n`**: Number of samples in last eval
- **`edge_stack_shadow_last_success`**: 1 if last eval was status=ok
- **`edge_stack_shadow_last_updated_ts_ms`**: Timestamp of last update
- **`edge_stack_shadow_promoted`**: 1 if last run triggered promotion

### Файл: `tools/edge_stack_train_exporter_v1.py`
- **`edge_stack_train_last_joined`**: Joined records in last dataset
- **`edge_stack_train_last_oof_meta_brier`**: OOF meta brier score
- **`edge_stack_train_last_oof_meta_ece`**: OOF meta ECE
- **`edge_stack_train_last_oof_meta_precision_top5pct`**: OOF meta precision@top5%
- **`edge_stack_train_last_pos_rate`**: Positive rate in last dataset
- **`edge_stack_train_last_success`**: Last edge_stack train status (1=ok)
- **`edge_stack_train_last_updated_ts_ms`**: Last edge_stack train metrics update time (ms)

### Файл: `tools/meta_cov_rollout_exporter_v1.py`
- **`meta_cov_bucket_a_ge`**: Threshold for bucket A (excellent coverage)
- **`meta_cov_bucket_b_ge`**: Threshold for bucket B (good coverage)
- **`meta_cov_bucket_c_ge`**: Threshold for bucket C (minimal coverage)
- **`meta_cov_bucket_rate`**: Fraction of events in each coverage bucket
- **`meta_cov_ops_apply_effective`**: 1 if last run effectively applied changes
- **`meta_cov_ops_last_exit_code`**: Exit code of last run
- **`meta_cov_ops_last_ok`**: 1 if last run was fully OK
- **`meta_cov_ops_last_ts_ms`**: Timestamp of last ops bundle run
- **`meta_cov_ops_preflight_rc`**: Return code of preflight check
- **`meta_cov_ops_step_rc`**: Return code of individual steps
- **`meta_cov_outcome_last_apply_ms`**: last successful outcome application ms
- **`meta_cov_quarantine_active`**: 1 if bucket is quarantined (cfg2)
- **`meta_cov_quarantine_ttl_ms`**: remaining quarantine ttl ms (cfg2)
- **`meta_cov_quarantine_ttl_sec`**: remaining quarantine ttl seconds (cfg2)
- **`meta_cov_recovery_target_share`**: recovery target share after quarantine (cfg2)
- **`meta_cov_samples`**: Number of samples in lookback window
- **`meta_enforce_per_cov`**: 1 if coverage-based canary is enabled
- **`meta_enforce_share_cov`**: Canary share per coverage bucket
- **`meta_feature_coverage_p10`**: 10th percentile of feature coverage
- **`meta_feature_coverage_p50`**: Median feature coverage

### Файл: `tools/meta_enforce_guard_exporter_v1.py`
- **`meta_enforce_guard_block_rate`**: blocked/canary in last decision
- **`meta_enforce_guard_blocked_n`**: blocked-by-meta count in last decision
- **`meta_enforce_guard_canary_n`**: canary applied count in last decision
- **`meta_enforce_guard_cov_bad_rate`**: coverage-bad/canary in last decision
- **`meta_enforce_guard_n`**: events considered in last decision
- **`meta_enforce_guard_trigger`**: last guard decision trigger flag
- **`meta_enforce_guard_ts_ms`**: timestamp of last decision
- **`meta_model_enable`**: cfg2 meta_model_enable
- **`meta_model_freeze`**: cfg2 meta_model_freeze (latch)
- **`meta_model_mode_enforce`**: cfg2 meta_model_mode == ENFORCE

## 2. Алерты (Prometheus Rules)

### Алерт: `BlackboxExporterScrapeDownCritical` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
min(up{job=~"blackbox_http|blackbox_public"}) == 0
```

### Алерт: `BlackboxProbeMetricsMissingInternal` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
absent(probe_success{job="blackbox_http"})
```

### Алерт: `BlackboxProbeMetricsMissingPublic` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
absent(probe_success{job="blackbox_public"})
```

### Алерт: `CloseWaitBacklogGrowing` (Файл: `prometheus_alerts_close_wait_drainer_v1.yml`)
```promql
close_wait_pending_count > 1000
```

### Алерт: `CloseWaitDeadLettering` (Файл: `prometheus_alerts_close_wait_drainer_v1.yml`)
```promql
rate(close_wait_dead_letter_total[10m]) > 0
```

### Алерт: `CloseWaitDecisionMissingRateHigh` (Файл: `prometheus_alerts_close_wait_drainer_v1.yml`)
```promql
(rate(close_wait_missing_decision_total[10m]) / clamp_min(rate(close_wait_seen_total[10m]), 1)) > 0.2
```

### Алерт: `CloseWaitDrainerStale` (Файл: `prometheus_alerts_close_wait_drainer_v1.yml`)
```promql
close_wait_staleness_sec > 900
```

### Алерт: `ConfCalLiveBadStreakHigh` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
bad_streak >= 3
```

### Алерт: `ConfCalLiveDegradeActive` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
conf_cal_live_degrade == 1
```

### Алерт: `ConfCalLiveExporterReadFailing` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
conf_cal_live_exporter_read_ok == 0
```

### Алерт: `ConfCalLiveRollbackDetected` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
increase(conf_cal_live_rollback_events_total[10m]) > 0
```

### Алерт: `ConfCalLiveStatusStale` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
conf_cal_live_status_age_sec > 7200
```

### Алерт: `ConfCalProofReadFailed` (Файл: `prometheus_alerts_conf_cal_proof_controller_v1.yml`)
```promql
conf_cal_proof_read_ok == 0
```

### Алерт: `ConfCalProofStale` (Файл: `prometheus_alerts_conf_cal_proof_controller_v1.yml`)
```promql
(conf_cal_proof_valid == 1) and (conf_cal_proof_evidence_age_sec > 21600)
```

### Алерт: `ConfCalProofValidButLiveDegrade` (Файл: `prometheus_alerts_conf_cal_proof_controller_v1.yml`)
```promql
(conf_cal_proof_valid == 1) and (conf_cal_live_degrade == 1)
```

### Алерт: `ConfScoreAutopromoBlocked` (Файл: `prometheus_alerts_conf_score_guardrails_autopromo_v1.yml`)
```promql
conf_score_guard_autopromo_blocked == 1
```

### Алерт: `ConfScoreAutopromoCohortRegressionAvg` (Файл: `prometheus_alerts_conf_score_guardrails_autopromo_v1.yml`)
```promql
(conf_score_guard_autopromo_cohort_delta_ece_cal_wmean > 0.010) or (conf_score_guard_autopromo_cohort_delta_brier_cal_wmean > 0.010)
```

### Алерт: `ConfScoreAutopromoCohortRegressionWorst` (Файл: `prometheus_alerts_conf_score_guardrails_autopromo_v1.yml`)
```promql
(conf_score_guard_autopromo_cohort_delta_ece_cal_max > 0.020) or (conf_score_guard_autopromo_cohort_delta_brier_cal_max > 0.020)
```

### Алерт: `ConfScoreAutopromoObserveTooLong` (Файл: `prometheus_alerts_conf_score_guardrails_autopromo_v1.yml`)
```promql
conf_score_guard_autopromo_observing_remaining_sec > 3600
```

### Алерт: `ConfScoreAutopromoPairedRegression` (Файл: `prometheus_alerts_conf_score_guardrails_autopromo_v1.yml`)
```promql
(conf_score_guard_autopromo_arm_delta_ece_cal > 0.010) or (conf_score_guard_autopromo_arm_delta_brier_cal > 0.010)
```

### Алерт: `ConfScoreAutopromoStateStale` (Файл: `prometheus_alerts_conf_score_guardrails_autopromo_v1.yml`)
```promql
conf_score_guard_autopromo_state_age_sec > 900
```

### Алерт: `ConfScoreGuardFreezeActive` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_freeze == 1
```

### Алерт: `ConfScoreGuardFreezeLatched` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_latch_remaining_sec > 0
```

### Алерт: `ConfScoreGuardFreezeLatchedTooLong` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_latch_remaining_sec > 7200
```

### Алерт: `ConfScoreGuardHighDrift` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_drift_max_abs_z >= 6
```

### Алерт: `ConfScoreGuardRecoveryStuck` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_freeze == 1 and conf_score_guard_latch_remaining_sec == 0 and conf_score_guard_stable_streak >= 3
```

### Алерт: `ConfScoreGuardScaleReduced` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_scale < 0.9
```

### Алерт: `ConfScoreGuardStateStale` (Файл: `prometheus_alerts_conf_score_guardrails_v1.yml`)
```promql
conf_score_guard_status_age_seconds > 300
```

### Алерт: `ConfirmationSchemaDrift` (Файл: `prometheus_alerts_contract_v4.yml`)
```promql
rate(confirmation_unknown_total[5m]) > 0.01
```

### Алерт: `ConfirmationsCoverageAllZero` (Файл: `alerts_confirmations_coverage_v1.yml`)
```promql
confirmations_coverage_conf_bad_all_zero == 1
```

### Алерт: `ConfirmationsCoverageColumnsMissing` (Файл: `alerts_confirmations_coverage_v1.yml`)
```promql
confirmations_coverage_reason{reason="conf_cols_missing"} == 1
```

### Алерт: `ConfirmationsCoverageReportMissing` (Файл: `alerts_confirmations_coverage_v1.yml`)
```promql
confirmations_coverage_report_present == 0 or confirmations_coverage_report_parsed_ok == 0
```

### Алерт: `ConfirmationsCoverageReportStale` (Файл: `alerts_confirmations_coverage_v1.yml`)
```promql
confirmations_coverage_report_stale == 1
```

### Алерт: `DQFlagRateHighSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
dq_flag_rate > 0.05
```

### Алерт: `DQ_BookMissingSeqEMA_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v3.yml`)
```promql
book_missing_seq_ema > 0.35
```

### Алерт: `DQ_TickGapP95_Extreme_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v3.yml`)
```promql
tick_gap_p95_ms > 5000
```

### Алерт: `DQ_TickGapP95_Hard_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v3.yml`)
```promql
tick_gap_p95_ms > 1000
```

### Алерт: `DQ_TickGapP95_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v3.yml`)
```promql
tick_gap_p95_ms > 250
```

### Алерт: `DQ_TickMissingSeqEMA_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v3.yml`)
```promql
tick_missing_seq_ema > 0.6
```

### Алерт: `DQ_TickMissingSeqEMA_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v3.yml`)
```promql
tick_missing_seq_ema > 0.25
```

### Алерт: `DecisionFinalStale` (Файл: `prometheus_alerts_regime_tradeoff_p65.yml`)
```promql
(time() * 1000 - decision_last_ts_ms) > 1800000
```

### Алерт: `DecisionFinalStaleSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
(time() - (decision_last_ts_ms / 1000)) > 1800
```

### Алерт: `DecisionRegimeBlockShareHigh` (Файл: `prometheus_alerts_regime_tradeoff_p65.yml`)
```promql
decision_regime_share_24h{regime="block"} > 0.50
```

### Алерт: `DecisionRegimeBlockShareHighSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
decision_regime_share_24h{regime="block"} > 0.15 and decision_n_24h > 200
```

### Алерт: `DecisionRegimeUnknownHigh` (Файл: `prometheus_alerts_regime_tradeoff_p65.yml`)
```promql
decision_regime_share_24h{regime="unknown"} > 0.10
```

### Алерт: `DecisionRegimeUnknownHighSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
decision_regime_share_24h{regime="unknown"} > 0.05 and decision_n_24h > 200
```

### Алерт: `DecisionRegimeUnknownShareHighSLO` (Файл: `prometheus_alerts_tradeoff_p66_v1.yml`)
```promql
decision_regime_share_24h{regime="unknown"} > 0.05 and decision_n_24h > 200
```

### Алерт: `DriftPsiHighSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
psi_max_24h > 0.25
```

### Алерт: `DriftRobustZHighSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
feature_drift_max_z_24h > 5
```

### Алерт: `EdgeStackChampionQualityDegraded` (Файл: `prometheus_alerts_edge_stack_shadow_p60.yml`)
```promql
edge_stack_shadow_champion_brier > 0.25
```

### Алерт: `EdgeStackShadowEvalFailed` (Файл: `prometheus_alerts_edge_stack_shadow_p60.yml`)
```promql
edge_stack_shadow_last_success == 0
```

### Алерт: `EdgeStackShadowEvalStale` (Файл: `prometheus_alerts_edge_stack_shadow_p60.yml`)
```promql
(time() * 1000 - edge_stack_shadow_last_updated_ts_ms) > 93600000
```

### Алерт: `EdgeStackTrainFailed` (Файл: `prometheus_alerts_edge_stack_train_p59.yml`)
```promql
edge_stack_train_last_success == 0
```

### Алерт: `EdgeStackTrainQualityDegraded` (Файл: `prometheus_alerts_edge_stack_train_p59.yml`)
```promql
(edge_stack_train_last_oof_meta_brier > 0.30) or (edge_stack_train_last_oof_meta_ece > 0.08)
```

### Алерт: `EdgeStackTrainStale` (Файл: `prometheus_alerts_edge_stack_train_p59.yml`)
```promql
(time() * 1000 - edge_stack_train_last_updated_ts_ms) > 36 * 3600 * 1000
```

### Алерт: `EnforceApplyBlockedHard` (Файл: `prometheus_alerts_enforce_health_v82.yml`)
```promql
of_enforce_apply_blocked{severity="hard"} > 0
```

### Алерт: `EnforceAutoApplyBlocked` (Файл: `prometheus_alerts_exec_slip_stats_refresher_p81.yml`)
```promql
of_auto_apply_block_active{source="enforce_bucket_promoter"} > 0
```

### Алерт: `EnforceAutoApplyBlockedLong` (Файл: `prometheus_alerts_exec_slip_stats_refresher_p81.yml`)
```promql
of_auto_apply_block_active{source="enforce_bucket_promoter"} > 0
```

### Алерт: `EnforceBucketRollbackControllerMissing` (Файл: `prometheus_alerts_enforce_bucket_promoter_rollback_v1.yml`)
```promql
(of_enforce_promoter_last_apply_ts_ms > 0) and (of_enforce_promoter_last_apply_age_sec > 7200) and (of_enforce_promoter_last_rollback_ts_ms == 0)
```

### Алерт: `EnforceBucketRollbackTriggered` (Файл: `prometheus_alerts_enforce_bucket_promoter_rollback_v1.yml`)
```promql
of_enforce_promoter_last_rollback_age_sec < 3600
```

### Алерт: `EnforceDbViewStale` (Файл: `prometheus_alerts_enforce_health_v82.yml`)
```promql
of_enforce_db_view_age_sec > 1800
```

### Алерт: `EnforceHealthReportStale` (Файл: `prometheus_alerts_enforce_health_v82.yml`)
```promql
of_enforce_health_report_age_sec > 900
```

### Алерт: `EnforceRedisStreamStale` (Файл: `prometheus_alerts_enforce_health_v82.yml`)
```promql
of_enforce_redis_stream_age_sec > 900
```

### Алерт: `EnforceSloFreezerActive` (Файл: `prometheus_alerts_exec_slip_stats_refresher_p81.yml`)
```promql
sum by (sym) (of_enforce_freezer_block_active) > 0
```

### Алерт: `EvidenceMissingForSignal` (Файл: `prometheus_alerts_contract_v4.yml`)
```promql
(sum by(symbol) (rate(confirmation_seen_total[5m]))) > 0 and (sum by(symbol) (rate(evidence_used_total_session[5m]))) == 0
```

### Алерт: `ExecSlipEdgeNegShareHigh` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
(of_exec_slip_db_n > 80) and on(sym,bucket) (of_exec_slip_edge_neg_share > 0.55)
```

### Алерт: `ExecSlipEdgeNegShareWarn` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
(of_exec_slip_db_n > 80) and on(sym,bucket) (of_exec_slip_edge_neg_share > 0.40)
```

### Алерт: `ExecSlipModelEdgeNegShareWarn` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
(of_exec_slip_db_n > 80) and on(sym,bucket) (of_exec_slip_model_edge_neg_share > 0.45)
```

### Алерт: `ExecSlipResidualDBDown` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
of_exec_slip_stats_db_up == 0
```

### Алерт: `ExecSlipResidualModelP99Warn` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
(of_exec_slip_db_n > 80) and on(sym,bucket) (of_exec_slip_model_resid_p99_bps > 30)
```

### Алерт: `ExecSlipResidualP95Warn` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
(of_exec_slip_db_n > 80) and on(sym,bucket) (of_exec_slip_resid_p95_bps > 10)
```

### Алерт: `ExecSlipResidualP99High` (Файл: `prometheus_alerts_exec_slip_residual_validation_p86.yml`)
```promql
(of_exec_slip_db_n > 80) and on(sym,bucket) (of_exec_slip_resid_p99_bps > 20)
```

### Алерт: `ExecSlipStatsRefresherStale` (Файл: `prometheus_alerts_exec_slip_stats_refresher_p81.yml`)
```promql
of_exec_slip_stats_refresh_last_ok_age_sec > 7200
```

### Алерт: `ExecutionEmergencyFlattenTriggered` (Файл: `prometheus_alerts_execution_hardening_p104.yml`)
```promql
increase(execution_emergency_flatten_total[5m]) > 0
```

### Алерт: `ExecutionPositionUnprotected` (Файл: `prometheus_alerts_execution_hardening_p104.yml`)
```promql
execution_position_unprotected_seconds > 2.5
```

### Алерт: `ExecutionUserStreamStale` (Файл: `prometheus_alerts_execution_hardening_p104.yml`)
```promql
increase(execution_user_stream_stale_total[10m]) > 0
```

### Алерт: `FeatureDenylistABRunnerFailing` (Файл: `prometheus_alerts_feature_denylist_p103.yml`)
```promql
feature_denylist_ab_runner_fail > 0
```

### Алерт: `FeatureDenylistABRunnerStale` (Файл: `prometheus_alerts_feature_denylist_p103.yml`)
```promql
feature_denylist_ab_runner_age_seconds > 6*3600
```

### Алерт: `FeatureDenylistApprovedNotAppliedStale` (Файл: `prometheus_alerts_feature_denylist_p103.yml`)
```promql
feature_denylist_oldest_approved_not_applied_age_seconds > 24*3600
```

### Алерт: `FeatureDenylistPendingABStale` (Файл: `prometheus_alerts_feature_denylist_p103.yml`)
```promql
feature_denylist_oldest_pending_age_seconds > 72*3600
```

### Алерт: `FeatureRegistryContractExporterDown` (Файл: `prometheus_alerts_feature_registry_p94.yml`)
```promql
feature_registry_contract_exporter_up == 0
```

### Алерт: `FeatureRegistryContractFailed` (Файл: `prometheus_alerts_feature_registry_p94.yml`)
```promql
feature_registry_contract_last_success == 0
```

### Алерт: `FeatureRegistryContractStale` (Файл: `prometheus_alerts_feature_registry_p94.yml`)
```promql
feature_registry_contract_last_age_seconds > 6 * 3600
```

### Алерт: `FeatureSelectionLoopExporterDown` (Файл: `prometheus_alerts_feature_selection_loop_p101.yml`)
```promql
absent(feature_selection_loop_exporter_up) OR (feature_selection_loop_exporter_up == 0)
```

### Алерт: `FeatureSelectionLoopFailed` (Файл: `prometheus_alerts_feature_selection_loop_p101.yml`)
```promql
(feature_selection_loop_last_success == 0) AND (feature_selection_loop_age_seconds < 21600)
```

### Алерт: `FeatureSelectionLoopNoiseHigh` (Файл: `prometheus_alerts_feature_selection_loop_p101.yml`)
```promql
(feature_selection_loop_noise_share > 0.60) AND (feature_selection_loop_features >= 10)
```

### Алерт: `FeatureSelectionLoopStale` (Файл: `prometheus_alerts_feature_selection_loop_p101.yml`)
```promql
feature_selection_loop_age_seconds > 129600
```

### Алерт: `HighAtrGateVetoRate` (Файл: `regime_alerts.yml`)
```promql
rate(atr_gate_veto_total{mode="ENFORCE"}[1h]) / rate(signals_total[1h]) > 0.5
```

### Алерт: `HighWebSocketErrorRate` (Файл: `websocket_alerts.yml`)
```promql
websocket_alert_high_error_rate > 0
```

### Алерт: `HighWebSocketReconnections` (Файл: `websocket_alerts.yml`)
```promql
websocket_alert_high_reconnections > 0
```

### Алерт: `IcebergStrictMissing` (Файл: `prometheus_alerts_signal_quality_v1.yml`)
```promql
increase(confirmation_incomplete_total{kind="iceberg_strict_missing"}[15m]) > 0
```

### Алерт: `LiqMapLevelsOverlayEnabledButNeverAppliedCrit` (Файл: `prometheus_alerts_liqmap_levels_overlay_v1.yml`)
```promql
(max_over_time(liqmap_levels_overlay_enabled[120m]) == 1)
and on(symbol, window)
(sum by(symbol, window) (increase(liqmap_levels_attempt_total[120m])) > 0)
and on(symbol, window)
(max_over_time(liqmap_levels_applied_last[120m]) == 0)

```

### Алерт: `LiqMapLevelsOverlayEnabledButNeverAppliedWarn` (Файл: `prometheus_alerts_liqmap_levels_overlay_v1.yml`)
```promql
(max_over_time(liqmap_levels_overlay_enabled[30m]) == 1)
and on(symbol, window)
(sum by(symbol, window) (increase(liqmap_levels_attempt_total[30m])) > 0)
and on(symbol, window)
(max_over_time(liqmap_levels_applied_last[30m]) == 0)

```

### Алерт: `LongWebSocketConnections` (Файл: `websocket_alerts.yml`)
```promql
websocket_connection_duration_seconds > 3600
```

### Алерт: `LowWebSocketMessageActivity` (Файл: `websocket_alerts.yml`)
```promql
rate(websocket_messages_received_total[5m]) < 0.1
```

### Алерт: `MLConfirmCfgMissing` (Файл: `ml_confirm_alerts.yml`)
```promql
min_over_time(ml_confirm_cfg_present[2m]) == 0
```

### Алерт: `MLConfirmEnforceShareMissing` (Файл: `ml_confirm_alerts.yml`)
```promql
increase(ml_missing_critical_total{field="champion.enforce_share"}[30m]) > 0
```

### Алерт: `MLConfirmInvalidCfg` (Файл: `ml_confirm_alerts.yml`)
```promql
increase(ml_confirm_errors_total{reason="invalid_cfg"}[10m]) > 0
```

### Алерт: `MLConfirmModelLoadFail` (Файл: `ml_confirm_alerts.yml`)
```promql
increase(ml_confirm_errors_total{reason="load_fail"}[10m]) > 0
```

### Алерт: `MLConfirmModelNotLoaded` (Файл: `ml_confirm_alerts.yml`)
```promql
ml_confirm_model_loaded == 0
```

### Алерт: `MLConfirmNoCfgRateHigh` (Файл: `ml_confirm_alerts.yml`)
```promql
(
  increase(ml_confirm_errors_total{reason="no_cfg"}[5m])
/
  clamp_min(increase(ml_confirm_events_total[5m]), 1)
) > 0.01

```

### Алерт: `MLConfirmPromoFail` (Файл: `ml_confirm_alerts.yml`)
```promql
increase(tb_promo_fail_total[1h]) > 0
```

### Алерт: `MetaAbV2IncreaseShareButNoEdge` (Файл: `prometheus_alerts_meta_ab_v2_v1.yml`)
```promql
(meta_ab_v2_action{action="increase_share"} > 0.5) and (meta_ab_v2_delta_exp_r_per_candidate < 0.0)
```

### Алерт: `MetaAbV2PolicyBlockedAllChanges` (Файл: `prometheus_alerts_meta_ab_v2_policy_v1.yml`)
```promql
meta_ab_v2_policy_blocked == 1
```

### Алерт: `MetaAbV2PolicyBlockedIncrease` (Файл: `prometheus_alerts_meta_ab_v2_policy_v1.yml`)
```promql
meta_ab_v2_policy_blocked == 1 and meta_ab_v2_action_raw{action="increase_share"} == 1
```

### Алерт: `MetaAbV2ReportMissing` (Файл: `prometheus_alerts_meta_ab_v2_v1.yml`)
```promql
meta_ab_v2_report_parsed_ok < 0.5
```

### Алерт: `MetaAbV2ReportStale` (Файл: `prometheus_alerts_meta_ab_v2_v1.yml`)
```promql
meta_ab_v2_report_age_sec > 36*3600
```

### Алерт: `MetaAbV2RunFailed` (Файл: `prometheus_alerts_meta_ab_v2_v1.yml`)
```promql
meta_ab_v2_run_ok < 0.5
```

### Алерт: `MetaDQCorrelationNegative` (Файл: `alerts_meta_dq.yml`)
```promql
meta_quality_corr_meta_p_dq_health < -0.10
```

### Алерт: `MetaDQNoCoverage` (Файл: `alerts_meta_dq.yml`)
```promql
meta_quality_dq_present_n == 0
```

### Алерт: `MetaDQWorstBucketECEHigh` (Файл: `alerts_meta_dq.yml`)
```promql
meta_quality_worst_dq_bucket_ece > 0.12
```

### Алерт: `MetaDQWorstBucketPRAUCLow` (Файл: `alerts_meta_dq.yml`)
```promql
meta_quality_worst_dq_bucket_pr_auc < 0.52
```

### Алерт: `NoActiveWebSocketConnections` (Файл: `websocket_alerts.yml`)
```promql
sum(websocket_connection_status) == 0
```

### Алерт: `OFExecPenaltyP95High` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
histogram_quantile(0.95, sum(rate(trade_exec_pen_bucket[10m])) by (le, sym, bucket)) > 0.35

```

### Алерт: `OFInputsDLQDBAnyEvents` (Файл: `prometheus_alerts_of_inputs_dlq_db_p99.yml`)
```promql
sum(of_inputs_dlq_db_events_lookback_total{kind="dlq"}) > 0
```

### Алерт: `OFInputsDLQDBLargeBacklog` (Файл: `prometheus_alerts_of_inputs_dlq_db_p99.yml`)
```promql
sum(of_inputs_dlq_db_events_lookback_total{kind="dlq"}) > 500
```

### Алерт: `OFInputsDLQDBMissingLOBFieldsSpike` (Файл: `prometheus_alerts_of_inputs_dlq_db_p99.yml`)
```promql
of_inputs_dlq_db_events_lookback_total{kind="dlq",reason="missing_lob_fields"} > 50
```

### Алерт: `OFInputsDLQNonZero` (Файл: `prometheus_alerts_of_inputs_dlq_p96.yml`)
```promql
of_inputs_dlq_len{stream="stream:dlq:of_inputs"} > 0
```

### Алерт: `OFInputsDLQReplayFailing` (Файл: `prometheus_alerts_of_inputs_dlq_p96.yml`)
```promql
max(of_inputs_dlq_len{stream="stream:dlq:of_inputs"}) > 0 and of_inputs_dlq_replay_last_run_ts_ms > 0 and of_inputs_dlq_replay_last_run_ok == 0
```

### Алерт: `OFInputsDLQReplayNotRunning` (Файл: `prometheus_alerts_of_inputs_dlq_p96.yml`)
```promql
max(of_inputs_dlq_len{stream="stream:dlq:of_inputs"}) > 0 and (of_inputs_dlq_replay_last_run_ts_ms == 0 or of_inputs_dlq_replay_last_run_age_sec > 7200)
```

### Алерт: `OFInputsPublisherErrors` (Файл: `prometheus_alerts_of_inputs_dlq_p96.yml`)
```promql
increase(of_inputs_publish_error_total[5m]) > 0
```

### Алерт: `OFInputsQuarantineNonZero` (Файл: `prometheus_alerts_of_inputs_dlq_p96.yml`)
```promql
of_inputs_dlq_len{stream="quarantine:signals:of:inputs"} > 0
```

### Алерт: `OFInputsV3CircuitAutoApplyBlockedGlobal` (Файл: `prometheus_alerts_of_inputs_v3_circuit_p100.yml`)
```promql
max(of_inputs_v3_circuit_auto_apply_block_global_active) > 0
```

### Алерт: `OFInputsV3CircuitDisabledAny` (Файл: `prometheus_alerts_of_inputs_v3_circuit_p100.yml`)
```promql
sum(of_inputs_v3_circuit_cfg_disabled) > 0
```

### Алерт: `OFInputsV3CircuitDisabledMany` (Файл: `prometheus_alerts_of_inputs_v3_circuit_p100.yml`)
```promql
sum(of_inputs_v3_circuit_cfg_disabled) > 5
```

### Алерт: `OFInputsV3CircuitExporterStale` (Файл: `prometheus_alerts_of_inputs_v3_circuit_p100.yml`)
```promql
(time() * 1000) - max(of_inputs_v3_circuit_state_exporter_poll_ts_ms) > 60000
```

### Алерт: `OFInputsV3CircuitPreTripDowngrades` (Файл: `prometheus_alerts_of_inputs_v3_circuit_p100.yml`)
```promql
max by (symbol, reason) (of_inputs_v3_circuit_downgrades_window) >= 2
```

### Алерт: `OFInputsV3DowngradeRateHigh` (Файл: `prometheus_alerts_of_inputs_dlq_p96.yml`)
```promql
increase(of_inputs_downgrade_total{from_version="3",to_version="2"}[15m]) > 100
```

### Алерт: `OFSpreadP95High` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
histogram_quantile(0.95, sum(rate(trade_spread_bps_bucket[10m])) by (le, sym, bucket)) > 8

```

### Алерт: `OF_AutoApplyBlockedByEnforceBucket_Crit` (Файл: `prometheus_alerts_enforce_bucket_state_exporter_p90.yml`)
```promql
of_auto_apply_block_active{source="enforce_bucket_promoter"} > 0
```

### Алерт: `OF_AutoApplyBlockedByEnforceBucket_Warn` (Файл: `prometheus_alerts_enforce_bucket_state_exporter_p90.yml`)
```promql
of_auto_apply_block_active{source="enforce_bucket_promoter"} > 0
```

### Алерт: `OF_DQ_BookMissingSeqEmaHard_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v1.yml`)
```promql
of_dq_diagnostic_book_missing_seq_ema > 0.05
```

### Алерт: `OF_DQ_BookMissingSeqEmaHigh_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
book_missing_seq_ema > 0.25
```

### Алерт: `OF_DQ_BookMissingSeqEmaHigh_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
book_missing_seq_ema > 0.125
```

### Алерт: `OF_DQ_GateHardState_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v1.yml`)
```promql
of_dq_gate_level >= 2
```

### Алерт: `OF_DQ_GateVetoRate_High_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v1.yml`)
```promql
rate(of_dq_veto_total[5m]) > 0
```

### Алерт: `OF_DQ_HealthScoreLow_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v1.yml`)
```promql
of_dq_gate_health_score < 0.8
```

### Алерт: `OF_DQ_LevelHardShareHigh_Crit` (Файл: `prometheus_alerts_strict_dq_stream_health_v1.yml`)
```promql
avg_over_time((dq_level == bool 2)[5m:30s]) > 0.20
```

### Алерт: `OF_DQ_TickGapP95Extreme_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
tick_gap_p95_ms > 12000
```

### Алерт: `OF_DQ_TickGapP95High_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
tick_gap_p95_ms > 8000
```

### Алерт: `OF_DQ_TickGapP95High_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
tick_gap_p95_ms > 5000
```

### Алерт: `OF_DQ_TickMissingSeqEmaHard_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v1.yml`)
```promql
of_dq_diagnostic_tick_missing_seq_ema > 0.05
```

### Алерт: `OF_DQ_TickMissingSeqEmaHigh_Crit` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
tick_missing_seq_ema > 0.25
```

### Алерт: `OF_DQ_TickMissingSeqEmaHigh_Warn` (Файл: `prometheus_alerts_dq_gate_policy_v2.yml`)
```promql
tick_missing_seq_ema > 0.125
```

### Алерт: `OF_EnforceBucketPromoterReportStale_Crit` (Файл: `prometheus_alerts_enforce_bucket_promoter_v1.yml`)
```promql
of_enforce_promoter_report_age_sec > 48*3600
```

### Алерт: `OF_EnforceBucketPromoterReportStale_Warn` (Файл: `prometheus_alerts_enforce_bucket_promoter_v1.yml`)
```promql
of_enforce_promoter_report_age_sec > 30*3600
```

### Алерт: `OF_EnforceBucketSLOFreezerActive_Warn` (Файл: `prometheus_alerts_enforce_bucket_state_exporter_p90.yml`)
```promql
of_enforce_freezer_block_active > 0
```

### Алерт: `OF_EnforceStateExporterStale_Crit` (Файл: `prometheus_alerts_enforce_bucket_state_exporter_p90.yml`)
```promql
(time() * 1000 - of_enforce_state_exporter_poll_ts_ms) > 30 * 60 * 1000

```

### Алерт: `OF_EnforcedBucketEdgeNegShareHigh_Crit` (Файл: `prometheus_alerts_enforce_bucket_promoter_v1.yml`)
```promql
(of_enforce_promoter_bucket_edge_neg_share > 0.55) * on(bucket) group_left() (of_enforce_bucket_flag{component="slippage", sym="global"} == 1)

```

### Алерт: `OF_EnforcedBucketEdgeNegShareHigh_Warn` (Файл: `prometheus_alerts_enforce_bucket_promoter_v1.yml`)
```promql
(of_enforce_promoter_bucket_edge_neg_share > 0.40) * on(bucket) group_left() (of_enforce_bucket_flag{component="slippage", sym="global"} == 1)

```

### Алерт: `OF_EnforcedBucketResidualHigh_Crit` (Файл: `prometheus_alerts_enforce_bucket_promoter_v1.yml`)
```promql
(of_enforce_promoter_bucket_resid_p95_bps > 8) * on(bucket) group_left() (of_enforce_bucket_flag{component="slippage", sym="global"} == 1)

```

### Алерт: `OF_EnforcedBucketResidualHigh_Warn` (Файл: `prometheus_alerts_enforce_bucket_promoter_v1.yml`)
```promql
(of_enforce_promoter_bucket_resid_p95_bps > 5) * on(bucket) group_left() (of_enforce_bucket_flag{component="slippage", sym="global"} == 1)

```

### Алерт: `OF_ExecHealth_AutoGuard_ExporterStale_Warn` (Файл: `prometheus_alerts_exec_health_slo_autoguard_v1.yml`)
```promql
exec_health_slo_autoguard_exporter_up < 1
```

### Алерт: `OF_ExecHealth_AutoGuard_FreezeActive_Warn` (Файл: `prometheus_alerts_exec_health_slo_autoguard_v1.yml`)
```promql
exec_health_slo_autoguard_freeze_active > 0
```

### Алерт: `OF_ExecHealth_AutoGuard_RollbackPerformed_Warn` (Файл: `prometheus_alerts_exec_health_slo_autoguard_v1.yml`)
```promql
increase(exec_health_slo_autoguard_rollback_total[15m]) > 0
```

### Алерт: `OF_ExecHealth_FreezeHook_Blocks_Warn` (Файл: `prometheus_alerts_exec_health_freeze_hook_v1.yml`)
```promql
increase(exec_health_freeze_hook_block_total[15m]) > 0
```

### Алерт: `OF_ExecHealth_FreezeHook_ReaderErrors_Warn` (Файл: `prometheus_alerts_exec_health_freeze_hook_v1.yml`)
```promql
increase(exec_health_freeze_hook_reader_errors_total[15m]) > 0
```

### Алерт: `OF_ExecHealth_ReconnectNightly_Failed_Crit` (Файл: `prometheus_alerts_exec_health_freeze_reconnect_nightly_v1.yml`)
```promql
exec_health_freeze_reconnect_smoke_last_run_ok == 0
```

### Алерт: `OF_ExecHealth_ReconnectNightly_RolloutGate_Crit` (Файл: `prometheus_alerts_exec_health_freeze_reconnect_nightly_v1.yml`)
```promql
exec_health_freeze_reconnect_rollout_gate_active == 1
```

### Алерт: `OF_ExecHealth_ReconnectNightly_Stale_Warn` (Файл: `prometheus_alerts_exec_health_freeze_reconnect_nightly_v1.yml`)
```promql
(time() * 1000 - exec_health_freeze_reconnect_smoke_last_success_ts_ms) > 36 * 3600 * 1000
```

### Алерт: `OF_ExecSlipStatsRefreshStale_Crit` (Файл: `prometheus_alerts_enforce_bucket_state_exporter_p90.yml`)
```promql
of_exec_slip_stats_refresh_last_ok_age_sec > 6 * 3600
```

### Алерт: `OF_ExecSlipStatsRefreshStale_Warn` (Файл: `prometheus_alerts_enforce_bucket_state_exporter_p90.yml`)
```promql
of_exec_slip_stats_refresh_last_ok_age_sec > 3 * 3600
```

### Алерт: `OF_ExecSlippageEvalRowcountProbeStale_Crit` (Файл: `prometheus_alerts_slippage_calibrator_health_v1.yml`)
```promql
of_exec_slippage_eval_rows_24h_age_sec > 7200
```

### Алерт: `OF_ExecSlippageEvalRowsLow_Warn` (Файл: `prometheus_alerts_slippage_calibrator_health_v1.yml`)
```promql
sum(of_exec_slippage_eval_rows_24h) < 30
```

### Алерт: `OF_ExpectedSlippageDecompHigh_Crit` (Файл: `prometheus_alerts_slippage_qa_v1.yml`)
```promql
of_gate:expected_slip_decomp_p99:10m > 40
```

### Алерт: `OF_ExpectedSlippageDecompHigh_Warn` (Файл: `prometheus_alerts_slippage_qa_v1.yml`)
```promql
of_gate:expected_slip_decomp_p99:10m > 25
```

### Алерт: `OF_ExpectedSlippageLimitExceeded` (Файл: `prometheus_alerts_slippage_qa_v1.yml`)
```promql
(
  sum by (sym) (rate(trade_expected_slippage_limit_exceed_total[5m]))
  /
  sum by (sym) (rate(trade_expected_slippage_bps_count{model="decomp"}[5m]))
) > 0.05

```

### Алерт: `OF_Gate_Archiver_Errors` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
increase(of_gate_archiver_error_total{kind="metrics"}[10m]) > 0
```

### Алерт: `OF_Gate_Archiver_Metrics_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
(time() * 1000 - of_gate_archiver_last_run_ts_ms{kind="metrics"}) / 1000 > 900
```

### Алерт: `OF_Gate_Archiver_Quarantine_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
(time() * 1000 - of_gate_archiver_last_run_ts_ms{kind="quarantine"}) / 1000 > 900
```

### Алерт: `OF_Gate_ContractBadShareHigh` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
(of_gate:contract_bad_share2h > 0.01) and (max_over_time(of_gate_contract_smoke_n_total[2h]) > 0)
```

### Алерт: `OF_Gate_ContractMissingSchemaShareHigh` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
(of_gate:contract_missing_schema_share2h > 0.25) and (max_over_time(of_gate_contract_smoke_n_total[2h]) > 0)
```

### Алерт: `OF_Gate_ContractSchemaVersionMissing` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
(of_gate:contract_schema_version_mode2h == 0) and (max_over_time(of_gate_contract_smoke_n_total[2h]) > 0)
```

### Алерт: `OF_Gate_ContractSmokeStale2h` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
of_gate:contract_age_seconds > 7200
```

### Алерт: `OF_Gate_DLQ_ExporterDown` (Файл: `prometheus_alerts_of_gate_dlq_p82.yml`)
```promql
of_gate_dlq_exporter_up == 0
```

### Алерт: `OF_Gate_DLQ_Large` (Файл: `prometheus_alerts_of_gate_dlq_exporter_p82.yml`)
```promql
sum(of_gate_dlq_len) > 1000
```

### Алерт: `OF_Gate_DLQ_NonZero` (Файл: `prometheus_alerts_of_gate_dlq_exporter_p82.yml`)
```promql
sum(of_gate_dlq_len) > 0
```

### Алерт: `OF_Gate_DLQ_NonZero15m` (Файл: `prometheus_alerts_of_gate_dlq_p82.yml`)
```promql
sum(of_gate_dlq_len) > 0
```

### Алерт: `OF_Gate_DLQ_OldestAgeHigh` (Файл: `prometheus_alerts_of_gate_dlq_p82.yml`)
```promql
max(of_gate_dlq_oldest_age_sec) > 3600
```

### Алерт: `OF_Gate_EligibleAbsent15m` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
absent_over_time(of_gate_eligible_total[15m])
```

### Алерт: `OF_Gate_NoEligible15m` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
sum(increase(of_gate_eligible_total[15m])) == 0
```

### Алерт: `OF_Gate_OkRateStrictLow` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
(sum(rate(of_gate_ok_hard_total[5m])) / sum(rate(of_gate_eligible_total[5m])) < 0.10) and (sum(rate(of_gate_eligible_total[5m])) > 0)
```

### Алерт: `OF_Gate_QuarantineArchiver_Errors` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
increase(of_gate_archiver_error_total{kind="quarantine"}[10m]) > 0
```

### Алерт: `OF_Gate_QuarantineRateHigh` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
of_gate:quarantine_rate5m > 1
```

### Алерт: `OF_Gate_QuarantineShareHigh` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
(of_gate:quarantine_share5m > 0.01) and (sum(rate(of_gate_quarantined_total[5m])) + sum(rate(of_gate_eligible_total[5m])) > 0)
```

### Алерт: `OF_Gate_RollupsFreshnessProbe_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
(time() * 1000 - of_gate_archiver_last_run_ts_ms{kind="rollups_freshness"}) / 1000 > 1800
```

### Алерт: `OF_Gate_RollupsRefresh_Errors` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
increase(of_gate_archiver_error_total{kind="rollups_refresh"}[24h]) > 0
```

### Алерт: `OF_Gate_RollupsRefresh_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
of_gate_archiver_last_run_ts_ms{kind="rollups_refresh"} > 0 and (time() * 1000 - of_gate_archiver_last_run_ts_ms{kind="rollups_refresh"}) / 1000 > 86400
```

### Алерт: `OF_Gate_Rollups_1h_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
of_gate_rollups_freshness_ok == 1 and of_gate_rollups_bucket_age_sec{view="1h"} > 10800
```

### Алерт: `OF_Gate_Rollups_5m_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
of_gate_rollups_freshness_ok == 1 and of_gate_rollups_bucket_age_sec{view="5m"} > 1800
```

### Алерт: `OF_Gate_SoftShareHigh` (Файл: `prometheus_alerts_of_gate_ok_rate_v1.yml`)
```promql
(of_gate:soft_share5m > 0.50) and (sum(rate(of_gate_ok_soft_total[5m])) + sum(rate(of_gate_ok_hard_total[5m])) > 0)
```

### Алерт: `OF_Gate_TimescaleMissing` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
of_gate_timescale_expect == 1 and of_gate_timescale_present == 0
```

### Алерт: `OF_Gate_TimescalePoliciesDisabled` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
of_gate_timescale_present == 1 and of_gate_timescale_policies_disabled > 0
```

### Алерт: `OF_Gate_TimescalePoliciesMissing` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
of_gate_timescale_present == 1 and of_gate_timescale_policies_missing > 0
```

### Алерт: `OF_Gate_TimescalePolicyProbe_Stale` (Файл: `prometheus_alerts_of_gate_archiver_p78.yml`)
```promql
(time() * 1000 - of_gate_archiver_last_run_ts_ms{kind="timescale_policy_probe"}) / 1000 > 7200
```

### Алерт: `OF_LOB_DwObiUnstableUnderPressure_Crit` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob_dw_obi_stability_score < 0.15) and (abs(of_lob_dw_obi) > 0.50)
```

### Алерт: `OF_LOB_DwObiUnstableUnderPressure_Warn` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob_dw_obi_stability_score < 0.35) and (abs(of_lob_dw_obi) > 0.30)
```

### Алерт: `OF_LOB_MicroMidDivExtreme_Crit` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob:micro_mid_div_p99_10m > 25) or (of_lob:micro_mid_div_p01_10m < -25)
```

### Алерт: `OF_LOB_MicroMidDivExtreme_Warn` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob:micro_mid_div_p99_10m > 12) or (of_lob:micro_mid_div_p01_10m < -12)
```

### Алерт: `OF_LOB_MicroShiftExtreme_Crit` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob:micro_shift_p99_10m > 40) or (of_lob:micro_shift_p01_10m < -40)
```

### Алерт: `OF_LOB_MicroShiftExtreme_Warn` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob:micro_shift_p99_10m > 15) or (of_lob:micro_shift_p01_10m < -15)
```

### Алерт: `OF_LOB_PersistentImbalance_Crit` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob:qi_mean_p99_10m > 0.80) or (of_lob:qi_mean_p01_10m < -0.80)
```

### Алерт: `OF_LOB_PersistentImbalance_Warn` (Файл: `prometheus_alerts_lob_pressure_p92.yml`)
```promql
(of_lob:qi_mean_p99_10m > 0.60) or (of_lob:qi_mean_p01_10m < -0.60)
```

### Алерт: `OF_LiqMap_ParseErrorsHigh_Crit` (Файл: `prometheus_alerts_liqmap_observability_v1.yml`)
```promql
increase(liqmap_parse_errors_total[5m]) > 25
```

### Алерт: `OF_LiqMap_ParseErrorsHigh_Warn` (Файл: `prometheus_alerts_liqmap_observability_v1.yml`)
```promql
increase(liqmap_parse_errors_total[5m]) > 5
```

### Алерт: `OF_LiqMap_SnapshotAgeHigh_Crit` (Файл: `prometheus_alerts_liqmap_observability_v1.yml`)
```promql
max by (symbol, window) (liqmap_snapshot_age_ms) > 180000
```

### Алерт: `OF_LiqMap_SnapshotAgeHigh_Warn` (Файл: `prometheus_alerts_liqmap_observability_v1.yml`)
```promql
max by (symbol, window) (liqmap_snapshot_age_ms) > 60000
```

### Алерт: `OF_OFInputsDBArchiverErrorsSpike_Warn` (Файл: `prometheus_alerts_of_inputs_archiver_p98.yml`)
```promql
(max_over_time(of_inputs_archiver_error_total{kind="dlq"}[30m]) - min_over_time(of_inputs_archiver_error_total{kind="dlq"}[30m])) > 0 or (max_over_time(of_inputs_archiver_error_total{kind="quarantine"}[30m]) - min_over_time(of_inputs_archiver_error_total{kind="quarantine"}[30m])) > 0

```

### Алерт: `OF_OFInputsDLQDBArchiverStale_Crit` (Файл: `prometheus_alerts_of_inputs_archiver_p98.yml`)
```promql
(of_inputs_archiver_staleness_sec{kind="dlq"} > 6*3600) and on() (max(of_inputs_dlq_len{stream="stream:dlq:of_inputs"}) > 0)

```

### Алерт: `OF_OFInputsDLQDBArchiverStale_Warn` (Файл: `prometheus_alerts_of_inputs_archiver_p98.yml`)
```promql
(of_inputs_archiver_staleness_sec{kind="dlq"} > 3600) and on() (max(of_inputs_dlq_len{stream="stream:dlq:of_inputs"}) > 0)

```

### Алерт: `OF_OFInputsQuarantineDBArchiverStale_Crit` (Файл: `prometheus_alerts_of_inputs_archiver_p98.yml`)
```promql
(of_inputs_archiver_staleness_sec{kind="quarantine"} > 6*3600) and on() (max(of_inputs_dlq_len{stream="quarantine:signals:of:inputs"}) > 0)

```

### Алерт: `OF_OFInputsQuarantineDBArchiverStale_Warn` (Файл: `prometheus_alerts_of_inputs_archiver_p98.yml`)
```promql
(of_inputs_archiver_staleness_sec{kind="quarantine"} > 3600) and on() (max(of_inputs_dlq_len{stream="quarantine:signals:of:inputs"}) > 0)

```

### Алерт: `OF_PromRuleGroupEvalStall_Warn` (Файл: `prometheus_alerts_prom_rules_loaded_probe_v1.yml`)
```promql
(time() - prometheus_rule_group_last_evaluation_timestamp_seconds) > 300
```

### Алерт: `OF_PromRulesBundleValidationErrorsPresent` (Файл: `prometheus_alerts_prom_rules_bundle_health_v1.yml`)
```promql
of_prom_rules_bundle_last_error_n > 0
```

### Алерт: `OF_PromRulesBundleValidationFailed` (Файл: `prometheus_alerts_prom_rules_bundle_health_v1.yml`)
```promql
of_prom_rules_bundle_last_ok == 0
```

### Алерт: `OF_PromRulesBundleValidationStale` (Файл: `prometheus_alerts_prom_rules_bundle_health_v1.yml`)
```promql
of_prom_rules_bundle_last_ok_age_sec > 36 * 3600
```

### Алерт: `OF_PromRulesFilesMissing_Crit` (Файл: `prometheus_alerts_prom_rules_loaded_probe_v1.yml`)
```promql
rules_files_missing > 0
```

### Алерт: `OF_PromRulesLoadedProbeFailing_Warn` (Файл: `prometheus_alerts_prom_rules_loaded_probe_v1.yml`)
```promql
rules_loaded_probe_last_ok == 0
```

### Алерт: `OF_PromRulesLoadedProbeStale_Warn` (Файл: `prometheus_alerts_prom_rules_loaded_probe_v1.yml`)
```promql
rules_loaded_probe_last_run_age_sec > 10800
```

### Алерт: `OF_SQ_BookMissingSeqEmaHigh_Ticket` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
((book_missing_seq_ema_gauge) or (book_missing_seq_ema)) > 0.10
```

### Алерт: `OF_SQ_DQHardShareHigh_Crit` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
avg_over_time((((dq_level_gauge) or (dq_level)) == bool 2)[1h:15s]) > 0.20
```

### Алерт: `OF_SQ_DQHardShareHigh_Warn` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
avg_over_time((((dq_level_gauge) or (dq_level)) == bool 2)[1h:15s]) > 0.10
```

### Алерт: `OF_SQ_TickGapP95Extreme_Crit` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
tick_gap_p95_ms > 9000 and on(symbol) tick_gap_n >= 50
```

### Алерт: `OF_SQ_TickGapP95High_Crit` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
tick_gap_p95_ms > 4500 and on(symbol) tick_gap_n >= 50
```

### Алерт: `OF_SQ_TickGapP95High_Warn` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
tick_gap_p95_ms > 3000 and on(symbol) tick_gap_n >= 50
```

### Алерт: `OF_SQ_TickMissingSeqEmaHigh_Crit` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
tick_missing_seq_ema > 0.15
```

### Алерт: `OF_SQ_TickMissingSeqEmaHigh_Warn` (Файл: `prometheus_alerts_signal_quality_v2.yml`)
```promql
tick_missing_seq_ema > 0.05
```

### Алерт: `OF_SlippageCalibratorNoUpdates_Warn` (Файл: `prometheus_alerts_slippage_calibrator_health_v1.yml`)
```promql
(of_slippage_calib_last_ok_age_sec < 14400) and (of_slippage_calib_last_updated_groups == 0)
```

### Алерт: `OF_SlippageCalibratorStale_Crit` (Файл: `prometheus_alerts_slippage_calibrator_health_v1.yml`)
```promql
of_slippage_calib_last_ok_age_sec > 172800
```

### Алерт: `OF_SlippageCalibratorStale_Warn` (Файл: `prometheus_alerts_slippage_calibrator_v1.yml`)
```promql
of_slippage_calibrator_last_ok_age_sec > 30*3600
```

### Алерт: `OF_SlippageCoeffStaleHVLL_Warn` (Файл: `prometheus_alerts_slippage_calibrator_health_v1.yml`)
```promql
max by (sym) (of_slippage_decomp_impact_coeff_age_sec{bucket="HIGH_VOL_LOW_LIQ"}) > 172800
```

### Алерт: `OF_WP_AdverseRdBadShareHigh_Warn` (Файл: `prometheus_alerts_world_practice_adverse_rd_v1.yml`)
```promql
trade_adverse_rd_bad_share > 0.60 and trade_adverse_rd_n >= 40
```

### Алерт: `OF_WP_AdverseRdVeto_Crit` (Файл: `prometheus_alerts_world_practice_adverse_rd_v1.yml`)
```promql
trade_adverse_rd_veto == 1
```

### Алерт: `OF_WP_AdverseRdWiringStuck_Crit` (Файл: `prometheus_alerts_world_practice_adverse_rd_v1.yml`)
```promql
(increase(adverse_rd_eval_total[30m]) > 0) and on(sym, bucket) (max_over_time(trade_adverse_rd_n[30m]) == 0)
```

### Алерт: `OF_WP_BookChurnHiPersistent_Warn` (Файл: `prometheus_alerts_world_practice_flow_v1.yml`)
```promql
(
  avg_over_time(trade_book_churn_hi[10m]) >= 0.9
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_CancelToTradeExtreme_Crit` (Файл: `prometheus_alerts_world_practice_flow_v1.yml`)
```promql
(
  (trade_cancel_to_trade_bid > 8) or (trade_cancel_to_trade_ask > 8)
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_CancelToTradeHigh_Warn` (Файл: `prometheus_alerts_world_practice_flow_v1.yml`)
```promql
(
  (trade_cancel_to_trade_bid > 4) or (trade_cancel_to_trade_ask > 4)
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_DWOBIStableHigh_Warn` (Файл: `prometheus_alerts_world_practice_lob_pressure_v1.yml`)
```promql
(
  abs(trade_dw_obi_z) > 2.5
  and trade_dw_obi_stable > 0.5
  and trade_dw_obi_stability_score >= 0.60
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_DepthConvexityImbHigh_Warn` (Файл: `prometheus_alerts_world_practice_lob_pressure_v1.yml`)
```promql
(
  abs(trade_depth_convexity_imb) > 0.35
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_EtaFillHighHVLL_Warn` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
(
  trade_eta_fill_sec{bucket="HIGH_VOL_LOW_LIQ"} > 8
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_ExecPenP95High_Warn` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
(
  histogram_quantile(
    0.95,
    sum by (le, sym, bucket) (rate(trade_exec_pen_bucket[10m]))
  ) > 0.35
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_FillProbLowHVLL_Warn` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
(
  trade_fill_prob{bucket="HIGH_VOL_LOW_LIQ"} < 0.15
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_FillProbStuckZero_Crit` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
(
  max by (sym) (max_over_time(trade_eta_fill_sec[20m])) > 0.2
)
and on(sym)
(
  max by (sym) (max_over_time(trade_fill_prob[20m])) <= 0.001
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.05
)

```

### Алерт: `OF_WP_FlowSnapshotsStuckZero_Crit` (Файл: `prometheus_alerts_world_practice_flow_v1.yml`)
```promql
(
  max_over_time(trade_taker_buy_rate_ema[20m]) == 0
  and max_over_time(trade_taker_sell_rate_ema[20m]) == 0
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.05
)

```

### Алерт: `OF_WP_LobPressureSnapshotsStuckZero_Crit` (Файл: `prometheus_alerts_world_practice_lob_pressure_v1.yml`)
```promql
(
  max_over_time(trade_micro_mid_div_bps[30m]) == 0
  and max_over_time(trade_micro_shift_bps[30m]) == 0
  and max_over_time(trade_dw_obi_z[30m]) == 0
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.05
)
and on(sym)
(
  label_replace(
    max_over_time(book_rate_ema[5m]),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.05
)

```

### Алерт: `OF_WP_MicroMidDivHigh_Warn` (Файл: `prometheus_alerts_world_practice_lob_pressure_v1.yml`)
```promql
(
  abs(trade_micro_mid_div_bps) > 2.5
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_MicroShiftSpike_Warn` (Файл: `prometheus_alerts_world_practice_lob_pressure_v1.yml`)
```promql
(
  (max_over_time(trade_micro_shift_bps[5m]) > 1.0)
  or
  (min_over_time(trade_micro_shift_bps[5m]) < -1.0)
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_SpreadP95High_Warn` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
(
  histogram_quantile(
    0.95,
    sum by (le, sym, bucket) (rate(trade_spread_bps_bucket[10m]))
  ) > 8
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_TakerFlowImbZHigh_Warn` (Файл: `prometheus_alerts_world_practice_flow_v1.yml`)
```promql
(
  abs(trade_taker_flow_imb_z) > 3
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.02
)

```

### Алерт: `OF_WP_VolRatioZHighInNormalBucket_Warn` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
trade_vol_ratio_z{bucket="NORMAL"} > 3
```

### Алерт: `OF_WP_VolTrackersStuckZero_Crit` (Файл: `prometheus_alerts_world_practice_trackers_v1.yml`)
```promql
(
  max_over_time(trade_vol_ratio[20m]) == 0
)
and on(sym)
(
  label_replace(
    sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
    "sym", "$1", "symbol", "(.*)"
  ) > 0.05
)

```

### Алерт: `PolicyCalibrationSuggestLoosenBlock` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_block_action_code == -1 and policy_calibration_suggest_block_severity < 0.5 and policy_calibration_suggest_block_share_24h > 0.05 and policy_calibration_suggest_ok_baseline_present == 1 and signal_quality_n_24h_by_policy_mode{mode="block"} >= 20
```

### Алерт: `PolicyCalibrationSuggestLoosenWarn` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_warn_action_code == -1 and policy_calibration_suggest_warn_severity < 0.5 and policy_calibration_suggest_warn_share_24h > 0.30 and policy_calibration_suggest_ok_baseline_present == 1 and signal_quality_n_24h_by_policy_mode{mode="warn"} >= 50
```

### Алерт: `PolicyCalibrationSuggestTightenBlock` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_block_action_code == 1 and policy_calibration_suggest_block_severity > 1.0 and policy_calibration_suggest_block_share_24h > 0.01 and policy_calibration_suggest_ok_baseline_present == 1 and signal_quality_n_24h_by_policy_mode{mode="block"} >= 10
```

### Алерт: `PolicyCalibrationSuggestTightenWarn` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_warn_action_code == 1 and policy_calibration_suggest_warn_severity > 1.0 and policy_calibration_suggest_warn_share_24h > 0.05 and policy_calibration_suggest_ok_baseline_present == 1 and signal_quality_n_24h_by_policy_mode{mode="warn"} >= 30
```

### Алерт: `PolicyCalibrationSuggestionInputsStale` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_inputs_stale == 1
```

### Алерт: `PolicyCalibrationSuggestionStale` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_staleness_sec > 7200
```

### Алерт: `PolicyCalibrationSuggestionUnknownShareHigh` (Файл: `prometheus_alerts_policy_calibration_suggester_p74.yml`)
```promql
policy_calibration_suggest_unknown_share_24h > 0.02 and signal_quality_n_24h > 200
```

### Алерт: `PolicyEffectivenessReportStale` (Файл: `prometheus_alerts_policy_effectiveness_p71.yml`)
```promql
policy_effectiveness_staleness_sec > 7200
```

### Алерт: `PolicyEffectivenessUnknownShareHigh` (Файл: `prometheus_alerts_policy_effectiveness_p71.yml`)
```promql
policy_effectiveness_share_24h{mode="unknown"} > 0.02 and signal_quality_n_24h > 200
```

### Алерт: `PolicyEffectivenessWarnMuchWorseThanOk` (Файл: `prometheus_alerts_policy_effectiveness_p71.yml`)
```promql
policy_effectiveness_baseline_ok_present == 1 and signal_quality_n_24h_by_policy_mode{mode="ok"} >= 50 and signal_quality_n_24h_by_policy_mode{mode="warn"} >= 30 and policy_effectiveness_expectancy_r_delta_24h{mode="warn"} < -0.25
```

### Алерт: `PolicyModeBlockRegimeNotBlockedSLO` (Файл: `prometheus_alerts_tradeoff_p66_v1.yml`)
```promql
policy_mode_n_24h_total > 200 and policy_mode_mismatch_share_24h{kind="block_regime_effective_not_block"} > 0.01
```

### Алерт: `PolicyModeBlockShareHigh` (Файл: `prometheus_alerts_policy_flap_p69.yml`)
```promql
decision_policy_mode_share_24h{mode="block"} > 0.20
```

### Алерт: `PolicyModeUnknownShareHigh` (Файл: `prometheus_alerts_policy_flap_p69.yml`)
```promql
decision_policy_mode_share_24h{mode="unknown"} > 0.02
```

### Алерт: `PolicyModeWarnRegimeActiveShareHighSLO` (Файл: `prometheus_alerts_tradeoff_p66_v1.yml`)
```promql
policy_mode_n_24h_total > 200 and policy_mode_mismatch_share_24h{kind="warn_regime_effective_active"} > 0.05
```

### Алерт: `PolicyRegimeEffectivenessReportStale` (Файл: `prometheus_alerts_policy_regime_effectiveness_p72.yml`)
```promql
policy_regime_effectiveness_staleness_sec > 7200
```

### Алерт: `PolicyRegimeEffectivenessWorstBlockEceHigh` (Файл: `prometheus_alerts_policy_regime_effectiveness_p72.yml`)
```promql
policy_regime_effectiveness_cells_ok_baseline >= 3 and policy_regime_effectiveness_worst_block_ece_delta > 0.12
```

### Алерт: `PolicyRegimeEffectivenessWorstWarnEceHigh` (Файл: `prometheus_alerts_policy_regime_effectiveness_p72.yml`)
```promql
policy_regime_effectiveness_cells_ok_baseline >= 3 and policy_regime_effectiveness_worst_warn_ece_delta > 0.12
```

### Алерт: `PolicyRegimeEffectivenessWorstWarnTooBad` (Файл: `prometheus_alerts_policy_regime_effectiveness_p72.yml`)
```promql
policy_regime_effectiveness_cells_ok_baseline >= 3 and policy_regime_effectiveness_worst_warn_expectancy_r_delta < -0.50
```

### Алерт: `PromRulesBundleAutoApplyBlocked` (Файл: `prometheus_alerts_exec_slip_stats_refresher_p81.yml`)
```promql
of_auto_apply_block_active{source="prom_rules_bundle_smoke"} > 0
```

### Алерт: `RegimeQuantilesStale` (Файл: `regime_alerts.yml`)
```promql
regime_quantiles_fresh_seconds > 86400
```

### Алерт: `RegimeSampleCountLow` (Файл: `regime_alerts.yml`)
```promql
regime_quantiles_sample_count < 80
```

### Алерт: `ReplayInputsArchiverErrors` (Файл: `prometheus_alerts_replay_inputs_archiver_p56.yml`)
```promql
increase(replay_inputs_archiver_error_total[10m]) > 0
```

### Алерт: `ReplayInputsArchiverStale` (Файл: `prometheus_alerts_replay_inputs_archiver_p56.yml`)
```promql
(time()*1000 - replay_inputs_archiver_last_run_ts_ms) / 1000 > 900
```

### Алерт: `SignalQualityBlockBetterThanOk` (Файл: `prometheus_alerts_regime_tradeoff_p65.yml`)
```promql
signal_quality_expectancy_24h{regime="block"} > signal_quality_expectancy_24h{regime="ok"}
```

### Алерт: `SignalQualityBlockRegimeStillTradingSLO` (Файл: `prometheus_alerts_tradeoff_p66_v1.yml`)
```promql
signal_quality_n_24h_by_regime{regime="block"} >= 20
```

### Алерт: `SignalQualityEceWarnHigh` (Файл: `prometheus_alerts_regime_tradeoff_p65.yml`)
```promql
signal_quality_ece_24h{regime="warn"} > 0.15
```

### Алерт: `SignalQualityEceWarnHighSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
signal_quality_n_24h_by_regime{regime="warn"} >= 30 and signal_quality_ece_24h_by_regime{regime="warn"} > 0.10
```

### Алерт: `SignalQualityOkExpectancyLowSLO` (Файл: `prometheus_alerts_slo_tradeoff_p66.yml`)
```promql
signal_quality_n_24h_by_regime{regime="ok"} >= 50 and signal_quality_expectancy_r_24h_by_regime{regime="ok"} < 0.0
```

### Алерт: `SignalQualityOkRegimeExpectancyNegativeSLO` (Файл: `prometheus_alerts_tradeoff_p66_v1.yml`)
```promql
signal_quality_n_24h_by_regime{regime="ok"} >= 50 and signal_quality_expectancy_r_24h_by_regime{regime="ok"} < 0.0
```

### Алерт: `SignalQualityPolicyModeBlockWorseThanOk` (Файл: `prometheus_alerts_signal_quality_policy_mode_p70.yml`)
```promql
signal_quality_n_24h_by_policy_mode{mode="ok"} >= 50 and signal_quality_n_24h_by_policy_mode{mode="block"} >= 30 and (signal_quality_expectancy_r_24h_by_policy_mode{mode="ok"} - signal_quality_expectancy_r_24h_by_policy_mode{mode="block"}) > 0.10
```

### Алерт: `SignalQualityPolicyModeDataMissing` (Файл: `prometheus_alerts_signal_quality_policy_mode_p70.yml`)
```promql
signal_quality_n_24h > 50 and signal_quality_n_24h_by_policy_mode{mode="ok"} == 0
```

### Алерт: `SignalQualityPolicyModeEceOkHigh` (Файл: `prometheus_alerts_signal_quality_policy_mode_p70.yml`)
```promql
signal_quality_n_24h_by_policy_mode{mode="ok"} >= 50 and signal_quality_ece_24h_by_policy_mode{mode="ok"} > 0.10
```

### Алерт: `SignalQualityPolicyModeWarnMuchWorseThanOk` (Файл: `prometheus_alerts_signal_quality_policy_mode_p70.yml`)
```promql
signal_quality_n_24h_by_policy_mode{mode="ok"} >= 50 and signal_quality_n_24h_by_policy_mode{mode="warn"} >= 30 and (signal_quality_expectancy_r_24h_by_policy_mode{mode="ok"} - signal_quality_expectancy_r_24h_by_policy_mode{mode="warn"}) > 0.25
```

### Алерт: `SignalQualityWarnMuchWorseThanOk` (Файл: `prometheus_alerts_regime_tradeoff_p65.yml`)
```promql
(signal_quality_expectancy_24h{regime="ok"} - signal_quality_expectancy_24h{regime="warn"}) > 0.5
```

### Алерт: `SignalQualityWarnRegimeEceHighSLO` (Файл: `prometheus_alerts_tradeoff_p66_v1.yml`)
```promql
signal_quality_n_24h_by_regime{regime="warn"} >= 30 and signal_quality_ece_24h_by_regime{regime="warn"} > 0.10
```

### Алерт: `StrongGateHighVeto` (Файл: `prometheus_alerts_contract_v4.yml`)
```promql
rate(strong_gate_veto_total[5m]) > 1.0
```

### Алерт: `SweepContractBreak` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
(increase(sweep_detected_total[1h]) > 10) and (increase(evidence_used_total{key=~"sweep.*"}[1h]) == 0)
```

### Алерт: `SweepDetectedButNoEvidenceUsed` (Файл: `prometheus_alerts_contract_v4.yml`)
```promql
(sum by(symbol) (rate(sweep_detected_total[10m]))) > 0 and (sum by(symbol) (rate(evidence_used_total_session[10m]))) == 0
```

### Алерт: `SweepSideMissingHigh` (Файл: `prometheus_alerts_signal_quality_v1.yml`)
```promql
rate(sweep_side_missing_total[5m]) > 0.1
```

### Алерт: `TBLabelsBelowMin` (Файл: `ml_confirm_alerts.yml`)
```promql
max_over_time(tb_labels_xlen[60m]) < 500
```

### Алерт: `TBLabelsEmpty` (Файл: `ml_confirm_alerts.yml`)
```promql
max_over_time(tb_labels_xlen[30m]) < 1
```

### Алерт: `TBLabelsNoNewData` (Файл: `ml_confirm_alerts.yml`)
```promql
tb_labels_xadd_rate == 0
```

### Алерт: `TBTrainEmptyRun` (Файл: `ml_confirm_alerts.yml`)
```promql
increase(tb_train_empty_run_total[24h]) > 0
```

### Алерт: `TickDedupDropHigh` (Файл: `tick_quality_alerts.yml`)
```promql
(sum by(symbol) (rate(tick_dedup_drop_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_read_total[5m])), 1)) > 0.010000
```

### Алерт: `TickIngestE2EDelayP99High` (Файл: `tick_ingest_latency_alerts.yml`)
```promql
histogram_quantile(
  0.99,
  sum(rate(tick_ingest_e2e_delay_ms_bucket[5m])) by (le, symbol)
) > 5000

```

### Алерт: `TickIngestProcessP99High` (Файл: `tick_ingest_latency_alerts.yml`)
```promql
histogram_quantile(
  0.99,
  sum(rate(tick_ingest_process_ms_bucket[5m])) by (le, symbol)
) > 25

```

### Алерт: `TickSkewAbsEMAHigh` (Файл: `tick_quality_alerts.yml`)
```promql
max by(symbol) (tick_event_stream_skew_abs_ema_ms) > 30000
```

### Алерт: `TickTimeHardDropsHigh` (Файл: `tick_quality_alerts.yml`)
```promql
(sum by(symbol) (rate(tick_time_hard_drop_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_read_total[5m])), 1)) > 0.001000
```

### Алерт: `TickTimeQuarantineActive` (Файл: `tick_quality_alerts.yml`)
```promql
max by(symbol) (tick_time_quarantine_active) > 0
```

### Алерт: `TickTimeSourceNowHigh` (Файл: `tick_quality_alerts.yml`)
```promql
(sum by(symbol) (rate(ticks_ts_source_total{ts_source="now"}[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > 0.020000
```

### Алерт: `TickTimeSourceStreamIdHigh` (Файл: `tick_quality_alerts.yml`)
```promql
(sum by(symbol) (rate(ticks_ts_source_total{ts_source="stream_id"}[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > 0.050000
```

### Алерт: `TickUnknownSideEMAHigh` (Файл: `tick_quality_alerts.yml`)
```promql
max by(symbol) (tick_unknown_side_ema) > 0.050000
```

### Алерт: `TickUnknownSideHigh` (Файл: `tick_quality_alerts.yml`)
```promql
(sum by(symbol) (rate(ticks_unknown_side_policy_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > 0.050000
```

### Алерт: `TradeCloseJoinerProbMissingCritical` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
(sum by (symbol, where) (rate(trade_close_joiner_prob_missing_total[10m])) / clamp_min(sum by (symbol, where) (rate(trade_close_joiner_join_ok_total[10m])), 1e-9)) > 0.02 and sum(rate(trade_close_joiner_join_ok_total[10m])) > 0

```

### Алерт: `TradeCloseJoinerProbMissingWarning` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
(sum by (symbol, where) (rate(trade_close_joiner_prob_missing_total[10m])) / clamp_min(sum by (symbol, where) (rate(trade_close_joiner_join_ok_total[10m])), 1e-9)) > 0.005 and sum(rate(trade_close_joiner_join_ok_total[10m])) > 0

```

### Алерт: `TradeCloseJoinerProbSourceScoreCritical` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
(sum by (symbol) (rate(trade_close_joiner_prob_source_total{source="score"}[30m])) / clamp_min(sum by (symbol) (rate(trade_close_joiner_prob_source_total[30m])), 1e-9)) > 0.50

```

### Алерт: `TradeCloseJoinerProbSourceScoreWarning` (Файл: `prometheus_alerts_conf_cal_live_exporter_v1.yml`)
```promql
(sum by (symbol) (rate(trade_close_joiner_prob_source_total{source="score"}[30m])) / clamp_min(sum by (symbol) (rate(trade_close_joiner_prob_source_total[30m])), 1e-9)) > 0.20

```

### Алерт: `TradeDQHardVeto` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
max by (symbol) (trade_dq_level) >= 2
```

### Алерт: `TradeDQSoftVetoBurst` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
increase(trade_dq_soft_veto_total[15m]) > 20
```

### Алерт: `TradeExecutionBootstrapBlocked` (Файл: `prometheus_alerts_execution_orchestration_p124.yml`)
```promql
trade_execution_bootstrap_blocked == 1
```

### Алерт: `TradeExecutionBootstrapHealthServiceDown` (Файл: `prometheus_alerts_execution_orchestration_p124.yml`)
```promql
up{job="execution-bootstrap-health"} == 0
```

### Алерт: `TradeExecutionBootstrapNotReady` (Файл: `prometheus_alerts_execution_bootstrap_p123.yml`)
```promql
trade_execution_bootstrap_ready == 0
```

### Алерт: `TradeExecutionBootstrapUserStreamNotReady` (Файл: `prometheus_alerts_execution_bootstrap_p123.yml`)
```promql
trade_execution_bootstrap_user_stream_ready == 0
```

### Алерт: `TradeExecutionEmergencyFlatten` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
increase(execution_emergency_flatten_total[5m]) > 0
```

### Алерт: `TradeExecutionJournalWriteFailures` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
increase(trade_execution_journal_write_fail_total[10m]) > 0
```

### Алерт: `TradeExecutionReconcileBacklog` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
sum(increase(execution_reconcile_pending_total[10m])) > 10
```

### Алерт: `TradeOrchestrationCompositePreflightBlockedCrit` (Файл: `prometheus_alerts_orchestration_composite_preflight_p54.yml`)
```promql
orchestration_composite_preflight_decision_status{status="block"} > 0
```

### Алерт: `TradeOrchestrationCompositePreflightInvalidWarn` (Файл: `prometheus_alerts_orchestration_composite_preflight_p54.yml`)
```promql
orchestration_composite_preflight_decision_status{status="invalid"} > 0
```

### Алерт: `TradeOrchestrationCompositePreflightStateStaleWarn` (Файл: `prometheus_alerts_orchestration_composite_preflight_p54.yml`)
```promql
(orchestration_composite_preflight_state_present > 0) and (orchestration_composite_preflight_state_age_seconds > 6 * 3600)
```

### Алерт: `TradePortfolioClusterExposureHigh` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
max by (cluster) (trade_portfolio_cluster_exposure_ratio) > 1.0
```

### Алерт: `TradePortfolioExposureHigh` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
trade_portfolio_total_exposure_ratio > 2.0
```

### Алерт: `TradePortfolioForceFlatten` (Файл: `prometheus_rules_execution_p34.yml`)
```promql
increase(trade_risk_force_flatten_total[5m]) > 0
```

### Алерт: `WebProbeLatencyHighPublic` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
max_over_time(probe_duration_seconds{job="blackbox_public"}[10m]) > 5
```

### Алерт: `WebServiceDownCritical` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
probe_success{job="blackbox_http"} == 0
```

### Алерт: `WebServiceDownWarningPublic` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
probe_success{job="blackbox_public"} == 0
```

### Алерт: `WebSocketConnectionLoss` (Файл: `websocket_alerts.yml`)
```promql
websocket_alert_connection_loss > 0
```

### Алерт: `WebSocketRedisPublishIssues` (Файл: `websocket_alerts.yml`)
```promql
rate(websocket_messages_published_total[5m]) / rate(websocket_messages_received_total[5m]) < 0.9
```

### Алерт: `WebTlsCertExpiringCritical` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
(probe_ssl_earliest_cert_expiry{job="blackbox_public"} - time()) / 86400 < 7
```

### Алерт: `WebTlsCertExpiringSoon` (Файл: `prometheus_alerts_web_uptime_v1.yml`)
```promql
(probe_ssl_earliest_cert_expiry{job="blackbox_public"} - time()) / 86400 < 14
```

## 3. Grafana Дашборды и PromQL Запросы

### Дашборд: `ChatOps Security (Telegram Freeze Bot)` (Файл: `chatops_security.json`)
Запросы в панелях:
- `(chatops_last_rate_limited_ts_ms > 0) * ((time() * 1000 - chatops_last_rate_limited_ts_ms) / 1000)`
- `(chatops_last_unauthorized_ts_ms > 0) * ((time() * 1000 - chatops_last_unauthorized_ts_ms) / 1000)`
- `increase(chatops_clear_pending_started_total[1h])`
- `increase(chatops_rate_limited_total[10m])`
- `increase(chatops_unauthorized_total[10m])`
- `rate(chatops_rate_limited_total[1m])`
- `rate(chatops_unauthorized_total[1m])`
- `sum by (cmd) (chatops_cmd_total)`
- `sum(increase(chatops_cmd_total[10m]))`
- `topk(5, sum by (cmd) (rate(chatops_cmd_total[5m])))`

### Дашборд: `Edge Stack Overview (P59 Train + P60 Shadow)` (Файл: `edge_stack_overview.json`)
Запросы в панелях:
- `(time() * 1000 - edge_stack_shadow_last_updated_ts_ms) / 3600000`
- `(time() * 1000 - edge_stack_train_last_updated_ts_ms) / 3600000`
- `edge_stack_shadow_champion_brier`
- `edge_stack_shadow_last_success`
- `edge_stack_train_last_oof_meta_brier`
- `edge_stack_train_last_oof_meta_ece`
- `edge_stack_train_last_success`

### Дашборд: `Monitoring Smoke (Nightly Contract)` (Файл: `monitoring_smoke.json`)
Запросы в панелях:
- `1 - monitoring_smoke_targets_stale`
- `monitoring_smoke_age_seconds`
- `monitoring_smoke_alertmanager_api_ok`
- `monitoring_smoke_blackbox_exporter_ok`
- `monitoring_smoke_dashboards_ok`
- `monitoring_smoke_failed_checks_total`
- `monitoring_smoke_last_success`
- `monitoring_smoke_prometheus_api_ok`
- `monitoring_smoke_runbooks_ok`
- `monitoring_smoke_targets_age_seconds`
- `monitoring_smoke_targets_stale`

### Дашборд: `OF-gate DLQ (P82)` (Файл: `of_gate_dlq_p82.json`)
Запросы в панелях:
- `max(of_gate_dlq_oldest_age_sec)`
- `of_gate_dlq_len`
- `of_gate_dlq_oldest_age_sec`
- `sum(of_gate_dlq_len)`
- `topk(10, sum by (stream, err_prefix) (of_gate_dlq_err_prefix_total))`

### Дашборд: `OFInputs DLQ / Quarantine (P96)` (Файл: `of_inputs_dlq_p96.json`)
Запросы в панелях:
- `of_inputs_dlq_age_sec`
- `of_inputs_dlq_len`

### Дашборд: `Policy effectiveness (P71)` (Файл: `policy_effectiveness_p71.json`)
Запросы в панелях:
- `policy_effectiveness_ece_delta_24h{instance=~"$instance",mode!="ok"}`
- `policy_effectiveness_expectancy_r_delta_24h{instance=~"$instance",mode!="ok"}`
- `policy_effectiveness_last_age_seconds{instance=~"$instance"}`
- `policy_effectiveness_precision_top5p_delta_24h{instance=~"$instance",mode!="ok"}`
- `policy_effectiveness_share_24h{instance=~"$instance"}`
- `policy_effectiveness_total_n_24h{instance=~"$instance"}`

### Дашборд: `Trade Execution P3.3 Autonomy` (Файл: `trade_execution_p33_autonomy.json`)
Запросы в панелях:
- `histogram_quantile(0.95, sum by (le) (rate(trade_execution_replay_latency_ms_bucket[5m])))`
- `trade_execution_autonomy_trigger_checkpoint_scrubber`
- `trade_execution_rebuild_last_replay_latency_p95_ms`
- `trade_execution_rebuild_last_retention_guard_count`

### Дашборд: `Trade Execution P4.8 Unified` (Файл: `trade_execution_p48_unified.json`)
Запросы в панелях:
- `sum(rate(execution_state_transition_total[5m]))`
- `sum(rate(trade_execution_rehydrate_total[5m]))`
- `sum(rate(trade_risk_clamp_total[5m]))`
- `sum(rate(trade_risk_confidence_deny_total[5m]))`
- `sum(rate(trade_risk_decision_total[5m]))`
- `sum(trade_execution_replay_retention_guard_total)`
- `trade_risk_signal_mismatch_rate`
- `trade_risk_signal_repeated_sid_total`
- `trade_risk_summary_freshness_seconds`

### Дашборд: `Trade Execution P49 Risk Drift Drilldown` (Файл: `trade_execution_p49_risk_drift_drilldown.json`)
- Нет PromQL запросов или запросы скрыты в переменных.

### Дашборд: `Trade Execution P5` (Файл: `trade_execution_p5.json`)
Запросы в панелях:
- `max by (cluster) (trade_portfolio_cluster_exposure_ratio)`
- `max by (symbol) (trade_portfolio_symbol_exposure_ratio)`
- `sum by (action,symbol) (increase(execution_reconcile_pending_total[15m]))`
- `sum by (kind) (increase(trade_execution_journal_write_fail_total[15m]))`
- `sum by (symbol) (increase(execution_state_transition_total{next_state=~"TP[123]_WATCHDOG_MARKET_FALLBACK"}[15m]))`
- `sum by (symbol) (increase(trade_dq_hard_veto_total[15m]))`
- `sum by (symbol,reason) (increase(execution_emergency_flatten_total[15m]))`
- `sum by (symbol,reason) (increase(trade_risk_force_flatten_total[15m]))`
- `trade_portfolio_total_exposure_ratio`

### Дашборд: `Trade Execution P7 Panels` (Файл: `trade_execution_p7_panels.json`)
Запросы в панелях:
- `execution_emergency_flatten_total`
- `execution_reconcile_pending_total`
- `trade_dq_hard_veto_total`
- `trade_execution_consistency_critical_mismatches`
- `trade_execution_consistency_warning_mismatches`
- `trade_execution_health_status_code`
- `trade_execution_journal_write_fail_total`
- `trade_execution_user_stream_age_ms`
- `trade_portfolio_cluster_exposure_ratio`
- `trade_portfolio_total_exposure_ratio`
- `trade_risk_force_flatten_total`

### Дашборд: `Trade Execution P8 Annotations` (Файл: `trade_execution_p8_annotations.json`)
Запросы в панелях:
- `execution_emergency_flatten_total`
- `execution_reconcile_pending_total`
- `trade_dq_hard_veto_total`
- `trade_execution_consistency_critical_mismatches`
- `trade_execution_consistency_warning_mismatches`
- `trade_execution_health_status_code`
- `trade_execution_journal_write_fail_total`
- `trade_execution_user_stream_age_ms`
- `trade_portfolio_cluster_exposure_ratio`
- `trade_portfolio_total_exposure_ratio`
- `trade_risk_force_flatten_total`

### Дашборд: `Trade Execution P9 Canary` (Файл: `trade_execution_p9_canary.json`)
- Нет PromQL запросов или запросы скрыты в переменных.

### Дашборд: `Trade Risk Drift / Mismatch P4.7` (Файл: `trade_execution_p47_risk_drift.json`)
Запросы в панелях:
- `trade_risk_summary_freshness_seconds`
- `trade_risk_summary_stale`

### Дашборд: `Trade Risk Engine Quality P4.4-P4.5` (Файл: `trade_execution_p45_risk_quality.json`)
Запросы в панелях:
- `avg(trade_portfolio_cluster_exposure_ratio)`
- `avg(trade_portfolio_symbol_exposure_ratio)`
- `avg(trade_risk_maker_allowed)`
- `avg(trade_risk_min_confidence_required)`
- `avg(trade_risk_recommended_notional_usd) by (symbol)`
- `histogram_quantile(0.95, sum(rate(trade_risk_decision_latency_ms_bucket[5m])) by (le))`
- `sum(increase(trade_risk_audit_write_fail_total[1h]))`
- `sum(rate(trade_risk_clamp_total[5m])) / clamp_min(sum(rate(trade_risk_decision_total[5m])), 1)`
- `sum(rate(trade_risk_confidence_deny_total[5m])) by (tier)`
- `sum(rate(trade_risk_decision_total[5m])) by (tier, level)`
- `trade_portfolio_total_exposure_ratio`

### Дашборд: `Tradeoff: Coverage vs Quality (Regimes) — P66` (Файл: `tradeoff_p66.json`)
Запросы в панелях:
- `avg(decision_regime_share_24h{regime="block",instance=~"$instance"})`
- `avg(decision_regime_share_24h{regime="ok",instance=~"$instance"})`
- `avg(decision_regime_share_24h{regime="unknown",instance=~"$instance"})`
- `avg(decision_regime_share_24h{regime="warn",instance=~"$instance"})`
- `max(decision_last_age_seconds{instance=~"$instance"})`
- `policy_mode_mismatch_share_24h{kind="block_regime_effective_not_block",instance=~"$instance"}`
- `policy_mode_mismatch_share_24h{kind="warn_regime_effective_active",instance=~"$instance"}`
- `signal_quality_ece_24h_by_regime{regime="block",instance=~"$instance"}`
- `signal_quality_ece_24h_by_regime{regime="ok",instance=~"$instance"}`
- `signal_quality_ece_24h_by_regime{regime="warn",instance=~"$instance"}`
- `signal_quality_expectancy_r_24h_by_regime{regime="block",instance=~"$instance"}`
- `signal_quality_expectancy_r_24h_by_regime{regime="ok",instance=~"$instance"}`
- `signal_quality_expectancy_r_24h_by_regime{regime="warn",instance=~"$instance"}`
- `signal_quality_n_24h_by_regime{regime="block",instance=~"$instance"}`
- `signal_quality_n_24h_by_regime{regime="ok",instance=~"$instance"}`
- `signal_quality_n_24h_by_regime{regime="warn",instance=~"$instance"}`
- `signal_quality_precision_top5p_24h_by_regime{regime="block",instance=~"$instance"}`
- `signal_quality_precision_top5p_24h_by_regime{regime="ok",instance=~"$instance"}`
- `signal_quality_precision_top5p_24h_by_regime{regime="warn",instance=~"$instance"}`
- `sum(decision_n_24h{instance=~"$instance"})`
- `sum(decision_regime_n_24h{regime="block",instance=~"$instance"})`
- `sum(decision_regime_n_24h{regime="ok",instance=~"$instance"})`
- `sum(decision_regime_n_24h{regime="unknown",instance=~"$instance"})`
- `sum(decision_regime_n_24h{regime="warn",instance=~"$instance"})`
- `sum(policy_mode_share_24h{regime="block",effective_mode="block",instance=~"$instance"})`
- `sum(policy_mode_share_24h{regime="ok",effective_mode="active",instance=~"$instance"})`
- `sum(policy_mode_share_24h{regime="ok",effective_mode="shadow",instance=~"$instance"})`
- `sum(policy_mode_share_24h{regime="warn",effective_mode="active",instance=~"$instance"})`
- `sum(policy_mode_share_24h{regime="warn",effective_mode="shadow",instance=~"$instance"})`

### Дашборд: `Web Uptime (Blackbox)` (Файл: `web_uptime.json`)
Запросы в панелях:
- `min((probe_ssl_earliest_cert_expiry{job="blackbox_public"} - time()) / 86400)`
- `min(probe_success{job="blackbox_http"})`
- `min(probe_success{job="blackbox_public"})`
- `min(up{job="blackbox_http"})`
- `min(up{job="blackbox_public"})`
- `probe_duration_seconds{job="blackbox_http"}`
- `probe_duration_seconds{job="blackbox_public"}`
- `probe_http_status_code{job="blackbox_public"}`
- `probe_success{job="blackbox_http"}`
- `probe_success{job="blackbox_public"}`