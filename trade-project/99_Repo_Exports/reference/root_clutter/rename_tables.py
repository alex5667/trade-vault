import os

REPLACEMENTS = {
    "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results": "llm_rca_gov_apply_flow_exp_res",
    "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_feedback": "llm_rca_gov_apply_flow_exp_feedback",
    "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_scorecards": "llm_rca_gov_apply_flow_exp_scorec",
    "llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_decisions": "llm_rca_gov_apply_flow_exp_win_dec",
    "idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results_ts": "idx_llm_rca_gov_apply_flow_exp_res_ts",
    "idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_feedback_ts": "idx_llm_rca_gov_apply_flow_exp_fb_ts",
    "idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_scorecards_ts": "idx_llm_rca_gov_apply_flow_exp_sc_ts",
    "idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_decisions_ts": "idx_llm_rca_gov_apply_flow_exp_wd_ts",
}

FILES = [
    "python-worker/orderflow_services/sql/ml_phase3_48_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results_v1.sql",
    "python-worker/orderflow_services/route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_result_consumer_v3_48.py",
    "python-worker/orderflow_services/route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_selector_v3_48.py"
]

for fpath in FILES:
    with open(fpath, "r") as f:
        content = f.read()

    # Apply all replacements in order of longest key first to prevent prefix substitution issues
    sorted_keys = sorted(REPLACEMENTS.keys(), key=len, reverse=True)
    for k in sorted_keys:
        content = content.replace(k, REPLACEMENTS[k])
    
    with open(fpath, "w") as f:
        f.write(content)

print(f"Replaced table names in {len(FILES)} files!")
