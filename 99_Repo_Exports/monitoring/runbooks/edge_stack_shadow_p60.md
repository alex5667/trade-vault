# Runbook: Edge Stack Shadow Eval (P60)

## Owner
- team: **trade**
- component: **edge_stack**
- contact: (add your Telegram/Slack channel here)

## How to silence
- Alertmanager → Silences → New, match:
  - `team="trade"`, `component="edge_stack"`, (optional) `alertname="EdgeStackShadowEvalFailed"`

## Symptoms
- Alert: `EdgeStackShadowEvalFailed`
- Alert: `EdgeStackShadowEvalStale`
- Alert: `EdgeStackChampionQualityDegraded`

## Immediate checks
1) Prometheus Targets: `edge-stack-shadow-exporter-p60` is **UP**.
2) File exists and fresh:
   - `${OF_REPORTS_DIR}/out/edge_stack/shadow_status.json`
3) Shadow metrics hash:
   - `HGETALL metrics:edge_stack_shadow:last` (if enabled by bundle)

## Common causes / fixes
### A) Report path not mounted
- Ensure compose mounts `OF_REPORTS_DIR:/var/lib/trade/of_reports`.

### B) Bundle not running
- Verify timers schedule invokes `tools.edge_stack_shadow_eval_bundle_v1`.

### C) Champion degraded
- Compare candidate vs champion stats.
- If candidate better and stable, use guarded promote.

## Guarded promote (manual)
Run:
```
python -m tools.edge_stack_shadow_eval_bundle_v1 --window_hours 24 --auto_promote_guarded 1
```

## Links
- Grafana dashboard: `/d/edge_stack_overview/edge-stack-overview?orgId=1`
