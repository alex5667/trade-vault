# Runbook: World-practice trackers (v1) — vol/regime × resilience × fill-prob

## Scope
This runbook covers the **world-practice trackers** that are exposed as low-cardinality gauges and validated by a periodic smoke-check:

- Volatility regime: `trade_vol_fast_bps`, `trade_vol_slow_bps`, `trade_vol_ratio`, `trade_vol_ratio_z`
- Execution regime bucket: `exec_regime_bucket` = `NORMAL|LOW_LIQ|HIGH_VOL|HIGH_VOL_LOW_LIQ`
- Book resilience: `trade_res_recovered`, `trade_res_recovery_ms`, `trade_res_speed_per_s`
- Passive executability: `trade_fill_prob`, `trade_eta_fill_sec`, `trade_exec_fill_pen`

- Execution-risk layer (P95/P99-ready histograms):
  - `trade_spread_bps`
  - `trade_exec_risk_ref_bps`, `trade_exec_risk_bps`, `trade_exec_risk_norm`, `trade_exec_pen`
  - `trade_expected_slippage_bps{model="legacy|eff"}` (legacy = pre-decomp, eff = decision-time effective)

Signals are produced on the hot path and also exported into the `metrics:of_gate` stream for smoke-check.

## Where data comes from
- `vol_*` and `vol_regime_label` are sourced from `bar_processor` / `VolRegimeTracker` snapshot.
- `liq_regime_label` is sourced from liquidity guard (book processor) via `runtime.last_liq_regime`.
- `res_*` is sourced from `BookResilienceTracker` snapshot.
- `fill_prob_proxy / eta_fill_*` are sourced from L3-lite stats, with a **fallback** in tick_processor using `core.fill_prob_proxy.compute_fill_prob_proxy()`.
- `spread_bps_submit` is computed from best bid/ask at decision time.
- `exec_risk_*` and `exec_pen` are computed by OF engine (execution-risk layer) and exported by tick_processor.

## Prometheus alerts
Alert rules:
- `orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml`

### Alert: OF_WP_VolRatioZHighInNormalBucket_Warn
Meaning: volatility is in shock-like state (high `vol_ratio_z`), but bucket remains `NORMAL`.

Triage:
1) Inspect `vol_regime_label` in `metrics:of_gate` rows.
2) Ensure `exec_regime_bucket` classifier accepts `shock` label.
3) Check `bar_processor` updates `runtime.dynamic_cfg["vol_regime_label"]`.

### Alert: OF_WP_FillProbLowHVLL_Warn
Meaning: in `HIGH_VOL_LOW_LIQ` bucket, passive fill probability is very low while the system still produces `allow` decisions.

Triage:
1) Confirm it is not a wiring issue:
   - `fill_prob_proxy` exists in `metrics:of_gate` and is not stuck at 0.
2) If real market condition:
   - check `cancel_to_trade_*`, `eta_fill_*`, churn / cancel hawkes.
3) Consider tightening: enforce safer buckets / reduce allowed aggressiveness.

### Alert: OF_WP_EtaFillHighHVLL_Warn
Meaning: passive ETA-to-fill is too high in `HIGH_VOL_LOW_LIQ` while system is active.

Triage:
- Check depth_near, taker_rate_ema, and cancel-to-trade ratios.

### Alert: OF_WP_VolTrackersStuckZero_Crit
Meaning: `trade_vol_ratio` stuck at 0 for prolonged time while `allow` decisions exist.

Triage:
1) Confirm `metrics:of_gate` contains `vol_fast_bps/vol_slow_bps/vol_ratio` fields.
2) If missing or zeros → snapshot wiring broken (bar_processor → runtime → indicators → gauges).

### Alert: OF_WP_ExecPenP95High_Warn
Meaning: execution penalty distribution shifted high (p95) while system is active.

Triage:
1) Check whether it is market-driven:
   - `spread_bps` p95 (often the primary driver)
   - `trade_expected_slippage_bps{model="eff"}` p95
2) Check wiring:
   - `metrics:of_gate` rows contain `exec_risk_norm` **and** `exec_pen` (non-zero when risk is non-trivial)
3) If widespread across symbols:
   - verify bucket labeling (`exec_regime_bucket`) and any recent snapshot regression.

### Alert: OF_WP_SpreadP95High_Warn
Meaning: decision-time spread distribution shifted high (p95) while system is active.

Triage:
1) Confirm it is not a data bug:
   - `metrics:of_gate` includes `spread_bps_submit` and it matches best bid/ask.
2) If real market:
   - thin book / widening spreads → consider tightening allow policy in affected buckets.




### Alert: OF_WP_FillProbStuckZero_Crit
Meaning: `trade_eta_fill_sec` indicates L3-lite is producing ETA-to-fill, but `trade_fill_prob` remains ~0 for prolonged time while the system is active.

Triage:
1) Confirm it is not an “inactive” state:
   - In Prometheus: `trade_eta_fill_sec` should be > 0.2s for the same symbol in the last 20m.
2) Inspect recent `metrics:of_gate` rows:
   - `eta_fill_bid_sec / eta_fill_ask_sec`
   - `cancel_to_trade_bid / cancel_to_trade_ask`
   - `fill_prob_proxy`, `exec_fill_pen`
3) Wiring checks:
   - L3-lite tracker running and updating stats for this symbol
   - tick_processor fallback compute is enabled and not overridden by stale placeholders
4) If values look real (not wiring):
   - consider tightening allow policy for `HIGH_VOL_LOW_LIQ` and/or enforcing more conservative exec bucket thresholds.
## Smoke-check (nightly orchestration)
Tool:
- `python -m orderflow_services.world_practice_gauges_smoke_check_v1`

Source-of-truth:
- Redis stream `metrics:of_gate` (recent tail window).

Exit codes:
- `0` OK (or no_data)
- `2` ALERT (missing/invalid/stuck beyond thresholds)
- `1` ERROR

Orchestration:
- executed hourly by `services/of_timers_worker.py` at `:11` (cooldown-protected notifications)

Env knobs:
- `ENABLE_WORLD_PRACTICE_SMOKE=1`
- `WORLD_PRACTICE_SMOKE_TIMEOUT_S=120`
- `WORLD_PRACTICE_SMOKE_COOLDOWN_S=21600` (6h)
- `WP_SMOKE_MAX_AGE_MS=900000` (15m)
- `WP_SMOKE_MIN_RECENT=200`

## A8: Microstructure extras (depth/gini/vwap/mom/rv/flow/flags)

### Gauges
New low-cardinality gauges (sym × bucket, plus `flag` enum):
- `trade_depth_total_10`, `trade_gini_depth_10`
- `trade_vwap_roll_diff_bps`, `trade_price_momentum_bps`, `trade_realized_vol_bps`
- `trade_pressure_per_min`, `trade_liquidity_pressure`, `trade_info_flow`
- `trade_flag_state{flag=...}` (0/1)

These are emitted by `tick_processor` from the same `indicators` dict that feeds model inputs.

### Smoke-check
Tool:
- `python -m orderflow_services.new_features_gauges_smoke_check_v1`

Checks (v1):
- `realized_vol_bps` stuck at ~0 while `realized_vol_no_data==0` is present in the recent window
- `nan_rate` across the tracked fields exceeds threshold

Orchestration:
- executed hourly by `services/of_timers_worker.py` at `:16` (cooldown-protected notifications)

Env knobs:
- `ENABLE_A8_NEW_FEATURES_SMOKE=1`
- `A8_NEW_FEATURES_SMOKE_TIMEOUT_S=120`
- `A8_NEW_FEATURES_SMOKE_COOLDOWN_S=21600` (6h)
- `A8_SMOKE_RECENT_S=600` (10m)
- `A8_SMOKE_NAN_RATE_MAX=0.01`
- `A8_SMOKE_RV_MIN_READY=40` (min rows with `realized_vol_no_data==0`)
- `A8_SMOKE_RV_EPS_BPS=1e-6`


## Grafana
Dashboard JSON:
- `orderflow_services/grafana/world_practice_trackers_v1.json`

Suggested panels:
- `trade_vol_ratio_z` by bucket
- `trade_fill_prob` + `trade_eta_fill_sec`
- `trade_res_recovery_ms` + `trade_res_recovered`

- P95 exec-risk (histograms):
  - `histogram_quantile(0.95, sum by (le,sym,bucket)(rate(trade_exec_pen_bucket[5m])))`
  - `histogram_quantile(0.95, sum by (le,sym,bucket)(rate(trade_spread_bps_bucket[5m])))`

## Prometheus rules loaded-probe (P91)

Probe: `orderflow_services/prom_rules_loaded_probe_v1.py`
Alert file: `orderflow_services/prometheus_alerts_prom_rules_loaded_probe_v1.yml`
Orchestration: `services/of_timers_worker.py` hourly `:10`

This probe is distinct from promtool/validator:
- `promtool` validates syntax/semantics.
- this probe validates **deployment wiring**: `Prometheus rule_files:` must actually pick up every expected file.

### Alert: OF_PromRulesFilesMissing_Crit (P91-A)
Meaning: one or more expected rule files are not loaded in Prometheus (`rules_files_missing > 0`).

Triage:
1. Check `rules_files_missing`, `rules_files_expected`, `rules_files_loaded` gauges.
2. Read Redis key `state:prom_rules_loaded:missing_json` for list of missing files.
3. Verify Prometheus volume mounts include the correct `rule_files:` globs.
4. Reload Prometheus (`curl -X POST http://prometheus:9090/-/reload`) and re-run probe.

### Alert: OF_PromRulesLoadedProbeFailing_Warn (P91-B)
Meaning: probe last run returned non-zero (can't communicate with Prometheus or found missing files).

Triage:
1. Check `PROMETHEUS_URL` env var (default `http://prometheus:9090`).
2. Read Redis `state:prom_rules_loaded:error_head` for the short error string.
3. Run probe manually: `python -m orderflow_services.prom_rules_loaded_probe_v1`.

### Alert: OF_PromRulesLoadedProbeStale_Warn (P91-C)
Meaning: no probe run for >3h.

Triage:
1. Check `of_timers_worker` container is running.
2. Verify `ENABLE_PROM_RULES_LOADED_PROBE=1`.
3. Check for scheduler errors in `of_timers_worker` logs.

### Alert: OF_PromRuleGroupEvalStall_Warn (P91-D)
Meaning: `prometheus_rule_group_last_evaluation_timestamp_seconds` not updating for >5min.

Triage:
1. Check Prometheus CPU/memory and rule evaluation queue.
2. Reduce rule group complexity or add `limit:` to slow groups.
3. Consider splitting large rule files into smaller ones.

### Env knobs
- `ENABLE_PROM_RULES_LOADED_PROBE=1`
- `PROM_RULES_LOADED_PROBE_TIMEOUT_S=60`
- `PROM_RULES_LOADED_PROBE_COOLDOWN_S=21600` (6h dedup)
- `PROMETHEUS_URL=http://prometheus:9090`
- `PROM_RULES_BUNDLE_MANIFEST` — path to manifest YAML (optional override)

### CI/Manual validation
```bash
# Run structural + runtime probe checks:
./python-worker/tools/validate_prometheus_rules_bundle_v1.sh

# Skip runtime probe (CI without live Prometheus):
./python-worker/tools/validate_prometheus_rules_bundle_v1.sh --skip-probe
```

---

## Rollback (fast)
- Disable smoke-check:
  - `ENABLE_WORLD_PRACTICE_SMOKE=0`
- Silence alerts (Prometheus side): do not load the rule file.

Note: the hot-path gauges are fail-open and safe to keep enabled.
