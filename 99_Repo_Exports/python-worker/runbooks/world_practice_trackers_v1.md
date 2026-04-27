# Grafana: World-practice trackers v1

Files:
- Dashboard: `world_practice_trackers_v1.json`
- Prometheus rules: `../prometheus_alerts_world_practice_trackers_v1.yml`
- Runbook: `../runbook_world_practice_trackers_v1.md`

Variables:
- `$sym` (regex, default `.*`)
- `$bucket` (regex, default `.*`)

Key PromQL:
- `trade_vol_ratio_z{sym=~"$sym",bucket=~"$bucket"}`
- `trade_fill_prob{sym=~"$sym",bucket=~"$bucket"}`
- `trade_eta_fill_sec{sym=~"$sym",bucket=~"$bucket"}`
- `trade_res_recovery_ms{sym=~"$sym",bucket=~"$bucket"}`
- `trade_res_recovered{sym=~"$sym",bucket=~"$bucket"}`
