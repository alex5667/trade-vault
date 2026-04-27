import re

with open("patch_trade_p68_strategy_research_stats_alert_policy_expiry_v1.clean.diff", "r") as f:
    lines = f.readlines()

new_lines = []
skip_mode = False
for line in lines:
    if line.startswith("diff -ruN") and " tick_flow_full/" in line:
        skip_mode = True
        continue
    elif line.startswith("diff -ruN"):
        skip_mode = False
        
    if skip_mode:
        continue
        
    if line.startswith("--- orderflow_services/"):
        line = line.replace("--- orderflow_services/", "--- python-worker/orderflow_services/")
    elif line.startswith("+++ orderflow_services/"):
        line = line.replace("+++ orderflow_services/", "+++ python-worker/orderflow_services/")
    elif line.startswith("diff -ruN") and " orderflow_services/" in line:
        line = line.replace(" orderflow_services/", " python-worker/orderflow_services/")
        
    new_lines.append(line)

with open("patch_p68_fixed_no_tickflow.diff", "w") as f:
    f.writelines(new_lines)
