# Runbook: World-practice LOB pressure v1

Scope
- Low-cardinality gauges: `trade_*` with labels `{sym,bucket}`.
- Signals:
  - Microprice divergence vs mid (`trade_micro_mid_div_bps`) and microprice shift (`trade_micro_shift_bps`)
  - Depth-weighted OBI (`trade_dw_obi`, `trade_dw_obi_z`) + stability (`trade_dw_obi_stability_score`, `trade_dw_obi_stable*`)
  - Depth shape proxies (`trade_depth_slope_imb_norm`, `trade_depth_convexity_imb`)
  - Queue imbalance aggregates (`trade_qi_*`)

## Alerts

### OF_WP_MicroMidDivHigh_Warn
Meaning
- Microprice deviates from mid persistently (hidden pressure near touch).

Checks
1) Confirm the move is real (not stuck/flat):
   - `trade_micro_mid_div_bps` and `trade_micro_shift_bps` panels.
   - `trade_dw_obi_z` should often agree in sign when pressure is real.
2) If divergence is high but `trade_dw_obi_z≈0`:
   - suspect top-of-book qty quality (missing/zeroed L1) or book parsing.

Actions
- Treat as regime flag: tighten expected slippage cap for passive orders, or require stronger confirmation on weaker side.

---

### OF_WP_MicroShiftSpike_Warn
Meaning
- Microprice is shifting fast (pressure flips / momentum bursts).

Checks
- Compare with:
  - `trade_dw_obi_z` (does it spike too?)
  - `trade_book_churn_hi` and `trade_cancel_to_trade_*` (from flow/churn dashboard)

Actions
- Consider temporary cooldown / widen slippage buffers / avoid passive joins if churn is also high.

---

### OF_WP_DWOBIStableHigh_Warn
Meaning
- Depth-weighted OBI robust-z is high and the stability flags indicate persistence.

Checks
- `trade_dw_obi_z` sign and magnitude; `trade_dw_obi_stability_score` ≥ ~0.6.
- Cross-check with `trade_micro_mid_div_bps` (should often agree).

Actions
- Treat as sustained pressure regime:
  - increase adverse-selection penalty,
  - reduce size for passive joins on the weaker side,
  - consider switching to more aggressive execution if fills degrade.

---

### OF_WP_DepthConvexityImbHigh_Warn
Meaning
- Depth curve shape differs between bid/ask sides.

Checks
- If convexity imbalance is sustained but microprice/DW OBI are calm, this may be structural (symbol-specific) rather than acute.

Actions
- Use as an impact asymmetry hint (execution-aware scoring): prefer entries that align with the thicker side.

---

### OF_WP_LobPressureSnapshotsStuckZero_Crit
Meaning
- LOB pressure snapshot gauges are stuck at 0 despite active decisions and book updates.

Primary suspects
- LOB pressure path not fed (book_processor not updating runtime / indicators).
- Indicator key propagation broken (keys exist but not passed into `indicators`).
- Gauge emission block not executed due to exception (should be fail-open, but verify logs).

Checks
1) Confirm book stream is alive:
   - `book_rate_ema` > 0
2) Confirm LOB pressure computations exist at all:
   - `of_lob_micro_mid_div_bps` / `of_lob_dw_obi` (per-symbol gauges)
3) Confirm indicators are present in gate payloads (metrics:of_gate):
   - look for keys like `lob_micro_mid_div_bps`, `lob_dw_obi_z`, `lob_qi_mean`.
4) Check recent silent errors:
   - `silent_errors_total{kind="tick_processor"}` and logs around tick_processor world-practice block.

Actions
- If `of_lob_*` are non-zero but `trade_*` are stuck:
  - the issue is in the tick processor gauge emission.
- If both `of_lob_*` and `trade_*` are stuck:
  - the issue is earlier: book feed, parsing, or `compute_lob_pressure` integration.
