# LOB Pressure P92 Dashboard — README

## Dashboard: `lob_pressure_p92.json`

### Import
Grafana → **Dashboards** → **Import** → upload `lob_pressure_p92.json`  
Select your Prometheus datasource when prompted.

### Panels

| Panel | Metrics | Notes |
|---|---|---|
| Queue imbalance mean / max_abs | `of_lob_queue_imbalance_mean`, `of_lob_queue_imbalance_max_abs` | Signed: +ve = bid pressure |
| Queue imbalance by level | `of_lob_queue_imbalance{level=...}` | L1..L5 via `$level` variable |
| QI slope | `of_lob_queue_imbalance_slope` | +ve = bid pressure builds deeper |
| Microprice divergence | `of_lob_micro_mid_div_bps` | bps; shows P99/P01 from recording rules |
| Microprice shift | `of_lob_micro_shift_bps` | Momentum proxy; bps vs prev snapshot |
| Depth slope | `of_lob_depth_slope{side=bid/ask/imb}` | qty/level; imb = bid−ask |
| Depth convexity | `of_lob_depth_convexity{side=bid/ask/imb}` | 2nd-diff of cumulative depth |
| DW OBI + z | `of_lob_dw_obi`, `of_lob_dw_obi_z` | Depth-weighted OBI with 1/level weights |
| DW OBI stability | `of_lob_dw_obi_stability_score`, `of_lob_dw_obi_stable`, `of_lob_dw_obi_stable_secs` | Stable flag requires score ≥ 0.60 and secs ≥ 1.5 |

### Alert file
```
orderflow_services/prometheus_alerts_lob_pressure_p92.yml
```

Add to `prometheus.yml` → `rule_files:`:
```yaml
  - orderflow_services/prometheus_alerts_lob_pressure_p92.yml
```

### Notes on signed metrics
- `qi_mean`, `micro_mid_div_bps`, `micro_shift_bps`, `dw_obi` are **signed** (positive = bid pressure)
- Alert thresholds apply to **absolute values** via P99/P01 recording rules
- Recording rules need `quantile_over_time()` which requires Prometheus 2.26+
