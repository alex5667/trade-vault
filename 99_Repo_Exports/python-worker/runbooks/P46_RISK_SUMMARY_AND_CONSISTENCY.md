# P4.6 Risk Summary and Consistency

Operational runbook for the P4.6 layer:
- Materialized SQL summary (`risk_decision_summary_mv`)
- Alertmanager deep-links for risk incidents
- Risk signal ↔ SQL snapshot consistency checker

## 1. Refresh risk_decision_summary_mv

Run the refresh script manually (or rely on the docker-compose timer):

```bash
python3 scripts/refresh_risk_decision_summary.py
# Output: latest_risk_decision_summary.json in $RISK_DECISION_SUMMARY_REPORT_PATH
```

Verify via the runbook server:
```bash
curl http://trade-runbook-server:18080/api/risk-summary/latest | jq .
```

## 2. Verify canary and summary reports

```bash
# Risk engine canary
curl http://trade-runbook-server:18080/api/risk-canary/latest | jq .score

# Aggregated risk decision summary
curl http://trade-runbook-server:18080/api/risk-summary/latest | jq .rows
```

## 3. Run signal ↔ snapshot consistency check

Run after **any deploy** that changes the signal publish path or risk engine contract:

```bash
python3 scripts/check_risk_signal_snapshot_consistency.py
# Exit 0 with JSON report. Non-zero mismatch_count requires investigation.
```

Investigate mismatches for:
- `execution_policy` — signal and snapshot disagree on allowed/deny/clamp mode
- `planned_notional_usd` — notional calculation diverged between signal and snapshot
- `risk_leverage_cap` — leverage cap changed between signal publish and snapshot write
- `clamp_ratio_snapshot` — snapshot_jsonb clamp_ratio disagrees with snapshot table column

## 4. Alertmanager silence (only after capturing evidence)

Use the deep-link in the alert notification to create a silence:
```
Silence: http://alertmanager:9093/#/silences/new?filter={alertname="..."}
```

Before silencing, ensure:
- `latest_risk_engine_canary.json` is current (age < 30 min)
- `latest_risk_decision_summary.json` is current
- `check_risk_signal_snapshot_consistency.py` returns `mismatch_count: 0`
