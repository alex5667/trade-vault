with open("docker-compose-python-workers.yml", "r") as f:
    text = f.read()

# find the last appearance of '  scanner-apply-flow-experiment-incident-rca-results-v3-54:'
# and cut it there
import re
target_string = "\n  scanner-apply-flow-experiment-incident-rca-results-v3-54:"
idx = text.rfind(target_string)
if idx != -1:
    clean_text = text[:idx]
    
    # Now find where 'networks:' block starts
    networks_idx = clean_text.rfind("\nnetworks:")
    if networks_idx != -1:
        # We need to insert the new services BEFORE the networks block starts
        with open("orderflow_services/docker_compose_fragment_ml_phase3_54_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_usefulness_v1.yml", "r") as f:
            frag = f.read()
        
        lines = frag.split("\n")
        service_lines = []
        in_services = False
        for line in lines:
            if line.startswith("services:"):
                in_services = True
                continue
            if in_services:
                if "depends_on: [redis-worker-1]" in line:
                    service_lines.append(line)
                    service_lines.append("    networks:")
                    service_lines.append("    - scanner-core")
                    service_lines.append("    - scanner-infra")
                else:
                    service_lines.append(line)
        
        new_text = clean_text[:networks_idx] + "\n" + "\n".join(service_lines) + "\n" + clean_text[networks_idx:]
        with open("docker-compose-python-workers.yml", "w") as f:
            f.write(new_text)
        print("Fixed structure.")
    else:
        print("Could not find networks block.")
else:
    print("Could not find appended string.")
