"""gate_value_reporter: compares passed vs gated_out signal outcomes.

Read-only analytic service. Joins:
  - labels:tb (TB-labelled outcomes of *passed* signals)
  - metrics:ml_confirm (p_edge/kind per sid for passed signals)
  - stream:signals:gated_out_outcomes (outcomes of gated-out cohort)

Computes cohort stats, gate lift, bootstrap CI, and emits a decision:
KEEP_GATE / RELAX_GATE / DISABLE_GATE / INSUFFICIENT_DATA / INCONCLUSIVE.

Writes JSON report to Redis (key + history stream) and Prometheus metrics.
NEVER mutates live signal pipeline behaviour.
"""
