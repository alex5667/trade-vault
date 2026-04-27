import re
import os

# Files to update
BASE = "python-worker/orderflow_services/deploy/systemd"
TICK = "python-worker/tick_flow_full/orderflow_services/deploy/systemd"

wrappers = [
    "run_trade_conf_score_guardrails_apply_v1.sh",
    "run_trade_conf_score_guardrails_autopromo_controller_v1.sh",
    "run_trade_conf_score_guardrails_promote_v1.sh",
    "run_trade_meta_cov_rollout_controller_v1.sh"
]

services = [
    "trade-conf-score-guardrails-apply.service",
    "trade-conf-score-guardrails-autopromo-controller.service",
    "trade-conf-score-guardrails-promote.service",
    "trade-meta-cov-rollout-controller.service"
]

# 1. Update wrappers
for d in [BASE, TICK]:
    for w in wrappers:
        p = os.path.join(d, w)
        if os.path.exists(p):
            txt = open(p).read()
            # Replace /orderflow_services/ with /python-worker/orderflow_services/
            # and /tick_flow_full/orderflow_services/ if it's the tick dir
            py_dir = "python-worker/tick_flow_full" if "tick_flow_full" in d else "python-worker"
            txt = txt.replace('"$REPO_ROOT/orderflow_services/', f'"$REPO_ROOT/{py_dir}/orderflow_services/')
            open(p, 'w').write(txt)

    for s in services:
        p = os.path.join(d, s)
        if os.path.exists(p):
            txt = open(p).read()
            py_dir = "python-worker/tick_flow_full" if "tick_flow_full" in d else "python-worker"
            txt = txt.replace('./orderflow_services/', f'./{py_dir}/orderflow_services/')
            open(p, 'w').write(txt)

# 2. Update latency_deploy_contract.py
p = "python-worker/services/observability/latency_deploy_contract.py"
if os.path.exists(p):
    txt = open(p).read()
    txt = txt.replace("'orderflow_services/", "'python-worker/orderflow_services/")
    open(p, 'w').write(txt)

print("P4.4 paths fixed")
