import os

yaml_add = """
  # ── Crit: fill-prob wiring likely broken (eta>0 but fill_prob stuck at 0) while system is active ─
  - alert: OF_WP_FillProbStuckZero_Crit
    expr: |
      (
        max by (sym) (max_over_time(trade_eta_fill_sec[20m])) > 0.2
      )
      and on(sym)
      (
        max by (sym) (max_over_time(trade_fill_prob[20m])) <= 0.001
      )
      and on(sym)
      (
        label_replace(
          sum by (symbol) (rate(decision_record_written_total{result="allow"}[5m])),
          "sym", "$1", "symbol", "(.*)"
        ) > 0.05
      )
    for: 20m
    labels:
      severity: critical
    annotations:
      summary: "{{ $labels.sym }}: eta_fill active but fill_prob stuck at 0"
      description: |
        Symbol {{ $labels.sym }}: trade_eta_fill_sec indicates L3-lite is producing ETA,
        but trade_fill_prob remains ~0 for 20m while allow decisions exist.
        This typically indicates broken wiring of fill_prob_proxy (L3-lite -> indicators -> gauges) or fallback compute disabled.
      runbook_path: /runbooks/world_practice_trackers_v1.md
      dashboard_path: /d/world_practice_trackers_v1
"""

md_add = """


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
"""

yaml_files = [
    "orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml",
    "python-worker/orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml",
    "python-worker/tick_flow_full/orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml",
    "reference/orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml",
    "reference/tick_flow_full/orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml",
    "tick_flow_full/orderflow_services/prometheus_alerts_world_practice_trackers_v1.yml",
]

md_files = [
    "orderflow_services/runbook_world_practice_trackers_v1.md",
    "python-worker/orderflow_services/runbook_world_practice_trackers_v1.md",
    "python-worker/tick_flow_full/orderflow_services/runbook_world_practice_trackers_v1.md",
    "reference/orderflow_services/runbook_world_practice_trackers_v1.md",
    "reference/tick_flow_full/orderflow_services/runbook_world_practice_trackers_v1.md",
    "tick_flow_full/orderflow_services/runbook_world_practice_trackers_v1.md",
]

for f in yaml_files:
    if os.path.exists(f):
        with open(f, 'r') as fp:
            content = fp.read()
            if "OF_WP_FillProbStuckZero_Crit" not in content:
                content += yaml_add
                with open(f, 'w') as fp2:
                    fp2.write(content)
                print(f"Updated {f}")

for f in md_files:
    if os.path.exists(f):
        with open(f, 'r') as fp:
            content = fp.read()
            if "OF_WP_FillProbStuckZero_Crit" not in content:
                # Add before "## Smoke-check (nightly orchestration)"
                rep_idx = content.find("## Smoke-check (nightly orchestration)")
                if rep_idx != -1:
                    new_content = content[:rep_idx] + md_add + content[rep_idx:]
                    with open(f, 'w') as fp2:
                        fp2.write(new_content)
                    print(f"Updated {f}")
