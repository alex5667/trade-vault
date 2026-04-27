# Runbook: Meta AB v2 policy block (Stage4)

## Symptoms
- `meta_ab_v2_policy_blocked == 1`
- Optional: `meta_ab_v2_action_raw{action="increase_share"} == 1` but action final is HOLD.
- One or more `meta_ab_v2_policy_blocked_reason{reason="..."} == 1`

## Immediate intent
This is **fail-closed** behavior: the system refuses to auto-increase challenger share when statistical or safety conditions are not met.

## Checklist
1. Open the latest report JSON (`META_AB_V2_OUT_JSON`) and look at:
   - `winner`, `counts.n_eligible`, `delta.*`, `ci.*`, `policy.blocked_reasons`
2. Map the blocked reason:
   - `n_eligible_low` -> dataset too small; check dataset build job and filters.
   - `ci_missing` / `ci_not_positive` -> bootstrap/CI config; check evaluator config.
   - `tail_worse` -> challenger worsens tail risk; inspect tail definition and stratified breakdown.
   - `delta_exp_r_low` -> edge is too small; consider holding or adjusting p_min/labels.
   - `share_step_too_large` -> mismatch between cfg ramp_step and produced share_next.
   - `share_above_freeze_max` -> freeze cap active; verify meta_freeze_file state.
3. If it is expected, do nothing (HOLD is the safe default).
4. If it is unexpected:
   - verify the dataset freshness (`META_AB_V2_DATASET_MAX_AGE_H`)
   - verify model paths and that both models load (champ/chall)
   - verify evaluator thresholds (p_min, min_n, min_delta_exp_r, tail_slack, require_ci_positive)

## Operator override (discouraged)
- You can disable guardrails temporarily:
  - `META_AB_POLICY_ENABLED=0` (not recommended)
- Or allow apply even if blocked (not recommended):
  - set `META_AB_POLICY_FAIL_CLOSED=0` (still logs reasons)

## Rollback
- Keep policy enabled; roll back the challenger model or set share to 0 manually in cfg if needed.
