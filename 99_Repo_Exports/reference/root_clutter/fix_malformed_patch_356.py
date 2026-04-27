import re

with open("ml_phase3_56_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_rollback_v1.patch", "r") as f:
    lines = f.readlines()

out = []
in_embedded = False

for line in lines:
    if line.startswith("+diff --git "):
        in_embedded = True
        out.append(line[1:])
    elif in_embedded:
        if line.startswith("+++ "):
            out.append(line)
        elif line.startswith("++++ "):
            out.append(line[1:])
        elif line.startswith("+--- "):
            out.append(line[1:])
        elif line.startswith("+@@ "):
            out.append(line[1:])
        elif line.startswith("+new file mode "):
            out.append(line[1:])
        elif line.startswith("+index "):
            out.append(line[1:])
        elif line.startswith("+\\ No newline"):
            out.append(line[1:])
        else:
            # For embedded file content, there's an extra + because it was added.
            # E.g. "++import os" -> "+import os"
            if line.startswith("+"):
                out.append(line[1:])
            else:
                out.append(line)
    else:
        out.append(line)

with open("fixed_phase3_56.patch", "w") as f:
    f.writelines(out)

