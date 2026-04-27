import re

with open("patch_trade_p68_strategy_research_stats_alert_policy_expiry_v1.clean.diff", "r") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    if line.startswith("--- orderflow_services/"):
        line = line.replace("--- orderflow_services/", "--- python-worker/orderflow_services/")
    elif line.startswith("+++ orderflow_services/"):
        line = line.replace("+++ orderflow_services/", "+++ python-worker/orderflow_services/")
    elif line.startswith("diff -ruN") and " orderflow_services/" in line:
        line = line.replace(" orderflow_services/", " python-worker/orderflow_services/")
    
    elif line.startswith("--- tick_flow_full/"):
        line = line.replace("--- tick_flow_full/", "--- python-worker/tick_flow_full/")
    elif line.startswith("+++ tick_flow_full/"):
        line = line.replace("+++ tick_flow_full/", "+++ python-worker/tick_flow_full/")
    elif line.startswith("diff -ruN") and " tick_flow_full/" in line:
        line = line.replace(" tick_flow_full/", " python-worker/tick_flow_full/")
        
    new_lines.append(line)

with open("patch_p68_fixed.diff", "w") as f:
    f.writelines(new_lines)
