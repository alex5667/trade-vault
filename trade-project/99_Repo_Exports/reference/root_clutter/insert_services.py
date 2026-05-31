import yaml
import sys

with open("orderflow_services/docker_compose_fragment_ml_phase3_54_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_usefulness_v1.yml", "r") as f:
    frag = f.read()

# We only need everything from under 'services:\n'
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

with open("docker-compose-python-workers.yml", "a") as f:
    f.write("\n" + "\n".join(service_lines) + "\n")
print("Appended successfully.")
