# Grafana dashboard: World-practice Adverse Realized Drift v1

File: `world_practice_adverse_rd_v1.json`

Panels
- Mean realized drift (bps) by bucket
- Sigma and Z-score
- Bad share (EW)
- Veto bit (stat)
- Eval rate (counter)

Variables
- `sym` (symbol)
- `bucket` (exec regime bucket)

Notes
- Negative mean / z indicates adverse selection (market moving against signal direction).
- Prefer analyzing by bucket: NORMAL vs WIDE vs STRESSED.
