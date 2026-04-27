# P66 — Tradeoff dashboard (coverage vs quality) + SLO alerts

## Dashboard
File: `orderflow_services/grafana/tradeoff_coverage_quality_regimes_p66.json`

Import in Grafana:
- Dashboards → New → Import
- Upload JSON
- Select Prometheus datasource

## How to read
### 1) Decision Coverage (24h)
- `decision_regime_share_24h{regime="ok|warn|block|unknown"}`
  - If `unknown` grows: decisions missing `dq_state/drift_state` → fix writers (P52/P62).

### 2) Signal Quality (24h) by regime
- Expectancy/Precision/ECE per regime:
  - `ok` should be the baseline.
  - `warn` should not collapse; if it does, drift gating is catching real deterioration.
  - `block` is usually “rule-strong-only”; sample size may be small.

### 3) Drift & DQ
- PSI drift (`psi_max_24h`) and robust z drift (`feature_drift_max_z_24h`)
- DQ flag rate (`dq_flag_rate`) and staleness (`tick_time_age_p99_ms`, `book_stale_p99_ms`)

## Alert playbook (quick)
### DecisionFinalStaleSLO
- Check Redis connectivity / consumer-group lag.
- Verify decision writers (SignalPipeline + TickProcessor veto path).
- Confirm exporter scrape.

### DecisionRegimeBlockShareHighSLO
- Look at drift panels (PSI, robust z). If high: reduce risk, keep gating.
- Look at DQ panels; fix time monotonicity / stale book / quarantine rate.
- If false positive: adjust thresholds in drift gate (P50/P51).

### SignalQualityOkExpectancyLowSLO
- Compare ok vs warn/block on the dashboard.
- If all regimes degrade: likely strategy drift or execution changes.
- If only warn/block degrade: tighten gating or retrain edge_stack (P59/P60).

### DriftPsiHighSLO / DriftRobustZHighSLO
- Validate features and upstream feeds.
- Consider temporarily switching ML rollout to `shadow` (P61 env) if ML-veto spikes.

## Safe rollback knobs
- `ML_CONFIRM_ROLLOUT_MODE=shadow` (or `off`)
- Keep `EDGE_STACK_AUTO_PROMOTE_GUARDED=0`
- If DQ noisy: keep `drift_state=block` mode “rule-strong-only” while debugging
