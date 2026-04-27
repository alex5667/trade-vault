import os

sql_fragment_path = 'orderflow_services/sql/ml_phase3_38_route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_v1.sql'
with open(sql_fragment_path, 'r', encoding='utf-8') as f:
    sql_fragment = f.read()

init_postgres_path = 'init-postgres.sql'
with open(init_postgres_path, 'a', encoding='utf-8') as f:
    f.write("\n" + sql_fragment + "\n")

compose_fragment_path = 'orderflow_services/docker_compose_fragment_ml_phase3_38_route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_v1.yml'
with open(compose_fragment_path, 'r', encoding='utf-8') as f:
    compose_fragment = f.read()

# Filter out the `services:` line if it exists
lines = compose_fragment.split('\n')
filtered_lines = [line for line in lines if line.strip() != 'services:']

# Inject networks into the compose fragment automatically:
final_lines = []
for line in filtered_lines:
    final_lines.append(line)
    if line.strip().startswith("depends_on: [redis-worker-1]"):
        final_lines.append("    networks:")
        final_lines.append("      - scanner-core")
        final_lines.append("      - scanner-infra")

compose_fragment = '\n'.join(final_lines) + "\n\n"

compose_path = 'docker-compose-python-workers.yml'
with open(compose_path, 'r', encoding='utf-8') as f:
    compose_content = f.read()

# Insert before `networks:`
networks_idx = compose_content.rfind('\nnetworks:')
if networks_idx != -1:
    new_compose = compose_content[:networks_idx] + "\n" + compose_fragment + compose_content[networks_idx:]
    with open(compose_path, 'w', encoding='utf-8') as f:
        f.write(new_compose)
    print("Docker compose patched successfully")
else: # maybe the user changed something, let's append at the end
    with open(compose_path, 'a', encoding='utf-8') as f:
         f.write("\n" + compose_fragment)
    print("Docker compose appended to end successfully")
