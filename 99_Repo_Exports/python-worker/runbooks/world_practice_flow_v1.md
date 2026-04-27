# World-practice flow v1 – runbook

## What these signals mean

This bundle exposes **L3-lite / microstructure flow** snapshots as low-cardinality Prometheus gauges (labels: `sym`, `bucket`).

Key metrics:
- `trade_taker_buy_rate_ema`, `trade_taker_sell_rate_ema` (qty/sec): smoothed aggressive flow.
- `trade_cancel_bid_rate_ema`, `trade_cancel_ask_rate_ema` (qty/sec): smoothed cancellation flow.
- `trade_cancel_to_trade_bid`, `trade_cancel_to_trade_ask`: cancellation pressure relative to taker activity.
- `trade_taker_flow_imb_z`: signed robust-z of taker flow imbalance.
- `trade_book_churn_score` (0..1) and `trade_book_churn_hi` (0/1): quote-update intensity proxy.

## Alert triage checklist

### 1) OF_WP_FlowSnapshotsStuckZero_Crit
Meaning: strategy is active (`allow` decisions exist), but L3-lite rates stayed at 0.

Actions:
1. Confirm L3-lite tracker is enabled and fed.
2. Validate `runtime.l3_stats` updates (log sample / debug) and that your stream includes L3 events.
3. Scrape `/metrics` and verify the gauges are present.

### 2) OF_WP_CancelToTradeHigh_Warn / Extreme_Crit
Meaning: cancels dominate relative to taker flow.

Typical causes:
- quote stuffing / spoof-like behavior → passive fills degrade.
- book thinning (depth retreats) → cancels spike.
- connectivity / stream gaps causing partial L3 events.

Actions:
1. Compare cancel rates vs taker rates.
2. Cross-check with trackers dashboard:
   - `trade_fill_prob`, `trade_eta_fill_sec`, `trade_vol_ratio_z`.
3. If persistent, tighten allowed buckets or increase execution penalties for this regime.

### 3) OF_WP_TakerFlowImbZHigh_Warn
Meaning: one-sided aggressive flow dominates.

Actions:
1. Cross-check with `trade_taker_flow_imb_z_abs` histogram and any taker-flow contra gate.
2. If stuck at 0 while other rates move, treat as wiring issue.

### 4) OF_WP_BookChurnHiPersistent_Warn
Meaning: high quote-update intensity for most of the last window.

Actions:
1. Expect higher slippage variance; validate `v_exec_slippage_eval` quantiles.
2. Consider raising `max_expected_slippage_bps_eff` or adding vetoes for this bucket.

## Dashboards
- `/d/world_practice_flow_v1` – flow/churn snapshots
- `/d/world_practice_trackers_v1` – vol/regime + resilience + fill
