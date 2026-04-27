import re
import os
import sys

patch_file = "ml_phase3_55_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_v1.patch"

with open(patch_file, "r") as f:
    lines = f.readlines()

current_file = None
current_content = []

for line in lines:
    if line.startswith("+++ ") or line.startswith("++++ ") or line.startswith("+++++ "):
        # Check if it's the expected start
        match = re.match(r'^\+{3,}\s+b/(.*)$', line)
        if match:
            if current_file:
                os.makedirs(os.path.dirname(current_file), exist_ok=True)
                with open(current_file, "w") as out:
                    out.writelines(current_content)
                print(f"Extracted: {current_file}")
                current_content = []
            current_file = match.group(1).strip()
            continue
            
    if line.startswith("+++ /dev/null") or line.startswith("++++ /dev/null"):
        current_file = None
        current_content = []
        continue
        
    if current_file is not None:
        if line.startswith("+") and not re.match(r'^\+{3,}\s', line):
            current_content.append(line[1:])
        elif line.startswith(" ") or line == "\n":
            current_content.append(line[1:] if line.startswith(" ") else line)

if current_file:
    os.makedirs(os.path.dirname(current_file), exist_ok=True)
    with open(current_file, "w") as out:
        out.writelines(current_content)
    print(f"Extracted: {current_file}")
