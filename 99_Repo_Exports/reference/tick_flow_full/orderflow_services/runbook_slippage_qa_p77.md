# Runbook: Slippage QA + Bucket Enforcement Promotion (P77)

## Scope
This runbook covers:
- expected slippage decomposition (spread + impact_proxy)
- nightly calibrator for `slippage_decomp_impact_coeff_bps`
- bucket-aware enforcement (`*_enforce_buckets`)
- nightly promoter (`nightly_enforce_bucket_promoter_v1.py`) guardrails
- exporter `enforce_bucket_state_exporter_v1.py`

## Key toggles / rollback
### Fast rollback (no redeploy)
Redis keys:
- `cfg:slippage_decomp_enforce_buckets` -> set to `HIGH_VOL_LOW_LIQ` (or empty)
- `cfg:taker_flow_gate_enforce_buckets` -> set to `HIGH_VOL_LOW_LIQ` (or empty)

Runtime flags (env/cfg):
- set `slippage_decomp_enforce_max=0`
- set `taker_flow_gate_mode=shadow`

### Calibrator off
- `ENABLE_SLIPPAGE_CALIBRATOR=0`

### Promoter off
- `ENABLE_ENFORCE_BUCKET_PROMOTER=0`

## Observability
### Prometheus alerts
- `prometheus_alerts_slippage_qa_v1.yml`
- `prometheus_alerts_enforce_bucket_promoter_v1.yml`
- `prometheus_alerts_slippage_calibrator_health_v1.yml`

### Exporter metrics (port 9142)
- `of_enforce_bucket_flag{component="slippage|taker_flow", sym="global", bucket}`
- `of_slippage_decomp_impact_coeff_bps{sym,bucket}` (only if configured in env symbols)
- `of_slippage_calibrator_last_ok_age_sec` (staleness of nightly calibrator)
- `of_enforce_promoter_report_age_sec`
- `of_enforce_promoter_bucket_resid_p95_bps{bucket}`

## Triage
### Alert: Promoter report stale
1) Check timers: `of_timers_worker` logs around 06:10 UTC.
2) Quick DB check (rowcount by bucket, last 24h):

```sql
select exec_regime_bucket, count(*)::bigint as n
from v_exec_slippage_eval
where ts >= now() - interval '24 hours'
group by 1
order by n desc;
```

### Residual stats per bucket (uses view columns `slippage_residual_bps`, `edge_minus_expected_bps`)
```sql
select
  exec_regime_bucket,
  count(*)::bigint as n,
  percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
  percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
  avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
from v_exec_slippage_eval
where sym = 'BTCUSDT' and ts >= now() - interval '24 hours'
group by 1
order by n desc;
```


### Residual stats per bucket (model expected)
```sql
select
  exec_regime_bucket,
  count(*)::bigint as n,
  percentile_cont(0.95) within group (order by slippage_residual_model_bps) as resid_model_p95_bps,
  percentile_cont(0.99) within group (order by slippage_residual_model_bps) as resid_model_p99_bps,
  avg(case when edge_minus_expected_model_bps < 0 then 1 else 0 end) as edge_neg_share_model
from v_exec_slippage_eval
where sym = 'BTCUSDT' and ts >= now() - interval '24 hours'
group by 1
order by n desc;
```

3) Calibrator state keys (Redis):

```bash
redis-cli GET state:slippage_calib:last_ok_ts_ms
redis-cli GET state:slippage_calib:last_run_ts_ms
redis-cli --scan --pattern 'state:slippage_calib:last:*' | head
# per sym/bucket timestamp
redis-cli GET cfg:slippage_decomp_impact_coeff_bps_ts_ms:BTCUSDT:HIGH_VOL_LOW_LIQ
```

4) Check Redis: stream `metrics:of_gate` is populated (lookback 24h).

### Alert: Residual high while enforced
1) Narrow enforcement to safest bucket:
   - `cfg:slippage_decomp_enforce_buckets = HIGH_VOL_LOW_LIQ`
2) Disable strict max:
   - `slippage_decomp_enforce_max=0`
3) Ensure calibrator ran:
   - verify `cfg:slippage_decomp_impact_coeff_bps:{sym}:{bucket}` keys updated.
4) Re-run calibrator manually (one-off):
   - `python -m ml_analysis.tools.nightly_slippage_calibrator_v1 --once`

## Safe promotion policy
Default promotion order: `HIGH_VOL -> LOW_LIQ`.
Promotion adds at most 1 bucket per run.
Guardrails require:
- DB samples >= 100 per bucket
- residual p95 <= 3 bps, p99 <= 8 bps
- gate eligible >= 200 per bucket
- ok_soft_rate >= 0.05

## Notes
- Keep `ENFORCE_STATE_EXPORTER_SYMBOLS` small (2-5 symbols) to avoid metric cardinality.

## Rollback controller (recommended)
### Enable
- `ENABLE_ENFORCE_BUCKET_ROLLBACK=1`
- `ENFORCE_BUCKET_ROLLBACK_APPLY=1` (default)

### Behavior
- Waits `ENFORCE_BUCKET_ROLLBACK_MIN_AGE_SEC` after last apply.
- Evaluates post-change window in `v_exec_slippage_eval` (residual p95/p99 + edge_negative_share) for buckets that were newly added.
- If thresholds breached, rolls back `cfg:*_enforce_buckets` to previous values and blocks further auto-apply by setting:
  - `cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter` (+ meta/ts keys)

### Manual run
- Dry-run:
  - `python -m orderflow_services.enforce_bucket_promoter_rollback_controller_v1 --apply 0`
- Apply rollback:
  - `python -m orderflow_services.enforce_bucket_promoter_rollback_controller_v1 --apply 1`

### Alerts
- `prometheus_alerts_enforce_bucket_promoter_rollback_v1.yml`


## P78 Preflight + Notifications

### Preflight tool
- Module: `orderflow_services.enforce_bucket_ops_validate_p78`
- Purpose: fast gating before running promoter/apply.
- Exit codes:
  - `0` OK
  - `2` soft-block (insufficient data; promoter should skip)
  - `1` hard-fail (infra/config missing)

Env:
- `ENABLE_ENFORCE_BUCKET_PREFLIGHT=1` (default on)
- `ENFORCE_PREFLIGHT_MIN_OF_GATE=200`
- `ENFORCE_PREFLIGHT_MIN_DB_SAMPLES=100`

### Ops notifications
Promoter/rollback publish Telegram stream messages when they **apply** changes (cooldown-protected).

Env:
- `ENFORCE_BUCKET_NOTIFY=1`
- `ENFORCE_BUCKET_NOTIFY_COOLDOWN_SEC=1800`
- `ENFORCE_BUCKET_NOTIFY_STREAM` or `NOTIFY_TELEGRAM_STREAM` (default `notify:telegram`)

Messages:
- `[EnforceBucketPromoter] APPLY ...`
- `[EnforceBucketRollback] ROLLBACK ...`

## P81: Exec-slip stats refresher + Auto-apply block + SLO freezer

### Status files
- Exec-slip stats MV refresher status:
  - `/var/lib/trade/of_reports/out/enforce/stats/exec_slip_stats_refresh_status.json`
- SLO freezer status:
  - `/var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json`

### Prometheus checks (exporter)

- Residual validation (requires `ENFORCE_STATE_EXPORTER_DB_STATS=1` on enforce_bucket_state_exporter):
  - `of_exec_slip_resid_p95_bps{sym,bucket}` / `of_exec_slip_resid_p99_bps{sym,bucket}` (max over lookback)
  - `of_exec_slip_edge_neg_share{sym,bucket}` (max over lookback)
  - `of_exec_slip_model_resid_p95_bps{sym,bucket}` / `of_exec_slip_model_resid_p99_bps{sym,bucket}` (optional P90)
  - `of_exec_slip_model_edge_neg_share{sym,bucket}` (optional P90)
  - `of_exec_slip_db_n{sym,bucket}` (sum n over lookback)
  - `of_exec_slip_stats_db_up` should be 1
- `of_exec_slip_stats_refresh_last_ok_age_sec` should be < 3600 (typical)
- `of_auto_apply_block_active{source="enforce_bucket_promoter"}` should be 0 unless actively frozen
- `of_auto_apply_block_active{source="prom_rules_bundle_smoke"}` should be 0; if 1, your rules bundle / monitoring is broken and auto-apply is fail-closed
- `of_enforce_freezer_block_active{sym,bucket}` shows which sym/bucket triggered the freeze

### If ExecSlipStatsRefresherStale fires
1) Validate DB connectivity (`ANALYTICS_DB_DSN`) and permissions to `REFRESH MATERIALIZED VIEW`.
2) Run manually:
   - `python -m orderflow_services.refresh_exec_slip_stats_p80`
3) Optional (P90): use extended MV with model residuals:
   - refresher env: `EXEC_SLIP_STATS_MV=mv_exec_slippage_eval_1h_stats_v2`
   - exporter env: `ENFORCE_STATE_EXPORTER_DB_MV=mv_exec_slippage_eval_1h_stats_v2`
3) If concurrent refresh fails repeatedly, ensure no long-running transaction blocks MV.

### If EnforceAutoApplyBlocked fires
Block keys (default namespace):
- `cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter`
- `cfg:suggestions:entry_policy:auto_apply_block:prom_rules_bundle_smoke`
- `...:meta`, `...:ts_ms`

Read meta:
- enforce buckets: reason should be `slo_freeze` or `rollback`
  - `GET cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter:meta`
- prom rules bundle: reason should be `rules_bundle_invalid` (set by `prom_rules_bundle_smoke`)
  - `GET cfg:suggestions:entry_policy:auto_apply_block:prom_rules_bundle_smoke:meta`

Manual unblock:
- Enforce buckets (only after verifying SLO stable):
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter`
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter:meta`
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter:ts_ms`
- Prom rules bundle (only after fixing rules + promtool/validator clean):
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:prom_rules_bundle_smoke`
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:prom_rules_bundle_smoke:meta`
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:prom_rules_bundle_smoke:ts_ms`

### If EnforceSloFreezerActive fires
1) Check freezer status JSON for the exact sym/bucket and thresholds.
2) Inspect `mv_exec_slippage_eval_1h_stats` (or fallback view) for:
   - `resid_p95_bps`, `resid_p99_bps`, `edge_neg_share`.
3) Keep auto-apply blocked until residual returns to baseline.


## Prometheus rules validation (CI / local)

### Python validator (recommended)
Validates YAML schema + rule structure for **all** `prometheus_alerts_*.yml` under:
- `./orderflow_services/`
- `./tick_flow_full/orderflow_services/` (if present)

Run (no promtool required):
- `python -m orderflow_services.validate_prometheus_rules_bundle_v1 --promtool off`


### Bundle manifest (include-list)
The rules bundle is discovered via a single manifest (preferred):
- `orderflow_services/prometheus_rules_bundle_manifest_v2.yml`

You can override it via env:
- `PROM_RULES_BUNDLE_MANIFEST=...`

### promtool wrapper (single entry point)
If `promtool` is installed, validate the *entire bundle* discovered from the manifest:
- `python -m orderflow_services.promtool_check_rules_wrapper_v1`


Exit codes:
- `0` OK
- `2` invalid rules/YAML

### promtool (if available on the Prometheus node/container)
Examples:
- `promtool check rules orderflow_services/prometheus_alerts_slippage_qa_v1.yml`
- `find orderflow_services -maxdepth 1 -name 'prometheus_alerts_*.yml' -print0 | xargs -0 -n1 promtool check rules`

## Prometheus rules bundle smoke-check (P90)

Purpose: detect broken rule YAML (schema/structure) early and alert if the check stops updating.

### State (Redis)
Keys (prefix `state:prom_rules_bundle`):
- `state:prom_rules_bundle:last_run_ts_ms`
- `state:prom_rules_bundle:last_ok_ts_ms`
- `state:prom_rules_bundle:last_ok` (1/0)
- `state:prom_rules_bundle:last_files_checked`
- `state:prom_rules_bundle:last_error_n`
- `state:prom_rules_bundle:last_error_head`

### Prometheus metrics (exporter)
- `of_prom_rules_bundle_last_ok`
- `of_prom_rules_bundle_last_ok_age_sec`
- `of_prom_rules_bundle_last_error_n`

### Quick triage
1) Inspect state:
- `GET state:prom_rules_bundle:last_error_head`
- `GET state:prom_rules_bundle:last_errors_json`

2) Run the same check manually:
- `python -m orderflow_services.prom_rules_bundle_health_check_v1 --promtool auto`



## Prometheus rules loaded probe (P90.1)

Purpose: detect "file not picked up" (Prometheus include-list / volume mount wiring) separately from syntax errors.

### State (Redis)
Keys (prefix `state:prom_rules_loaded`):
- `state:prom_rules_loaded:last_run_ts_ms`
- `state:prom_rules_loaded:last_ok_ts_ms`
- `state:prom_rules_loaded:last_ok` (1/0)
- `state:prom_rules_loaded:files_expected`
- `state:prom_rules_loaded:files_loaded`
- `state:prom_rules_loaded:missing_n`
- `state:prom_rules_loaded:missing_head`

### Prometheus metrics (exporter)
- `rules_files_expected`
- `rules_files_loaded`
- `rules_files_missing`
- `rules_loaded_probe_last_ok`
- `rules_loaded_probe_last_ok_age_sec`
- `rules_loaded_probe_last_run_age_sec`

### Schedule
- `of_timers_worker` hourly at `:10` (env `ENABLE_PROM_RULES_LOADED_PROBE=1`)

### Manual run
- `python -m orderflow_services.prom_rules_loaded_probe_v1`

3) If the check never runs:
- confirm `of_timers_worker` schedule includes the `:09` smoke-check block
- confirm env `ENABLE_PROM_RULES_BUNDLE_SMOKE=1`
- confirm Redis connectivity (`REDIS_URL`)
