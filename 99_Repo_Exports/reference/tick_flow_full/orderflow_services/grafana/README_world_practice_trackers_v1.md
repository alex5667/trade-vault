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


## A8 row: microstructure extras

Dashboard now includes a dedicated row **A8 microstructure extras** with panels for:
- Depth10 + Gini (top-10)
- VWAP diff + Momentum (bps)
- Realized vol + Pressure
- Liquidity pressure + Info flow
- Flags (0/1)

Prometheus metrics used:
- `trade_depth_total_10`, `trade_gini_depth_10`
- `trade_vwap_roll_diff_bps`, `trade_price_momentum_bps`, `trade_realized_vol_bps`
- `trade_pressure_per_min`, `trade_liquidity_pressure`, `trade_info_flow`
- `trade_flag_state{flag=...}`
