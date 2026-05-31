import os

# Phase 3.43 SQL
sql_fragment_path = 'orderflow_services/sql/ml_phase3_43_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_v1.sql'
with open(sql_fragment_path, 'r', encoding='utf-8') as f:
    sql_fragment = f.read()

init_postgres_path = 'init-postgres.sql'
with open(init_postgres_path, 'a', encoding='utf-8') as f:
    f.write("\n" + sql_fragment + "\n")

# Phase 3.43 Compose
compose_fragment_path = 'orderflow_services/docker_compose_fragment_ml_phase3_43_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_v1.yml'
with open(compose_fragment_path, 'r', encoding='utf-8') as f:
    compose_fragment = f.read()

# Convert from "KEY: VAL" to "- KEY=VAL" format like the rest of the file
def convert_to_array_env(fragment):
    lines = fragment.split('\n')
    new_lines = []
    in_env = False
    for line in lines:
        if line.strip() == 'environment:':
            in_env = True
            new_lines.append(line)
            continue
        
        if in_env and line.startswith('      ') and ':' in line and not line.strip().startswith('-'):
            key, val = line.split(':', 1)
            new_lines.append(f"      - {key.strip()}={val.strip()}")
        elif in_env and not line.strip():
            new_lines.append(line)
        elif in_env and not line.startswith('      '):
            in_env = False
            new_lines.append(line)
        else:
            new_lines.append(line)
    return '\n'.join(new_lines)

# Apply formatting and variable naming fixes
compose_fragment = convert_to_array_env(compose_fragment)
compose_fragment = compose_fragment.replace('- REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0}', '- REDIS_URL=${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}')
compose_fragment = compose_fragment.replace('- DATABASE_URL=${DATABASE_URL:-}', '- DATABASE_URL=${POSTGRES_URL:-postgresql://trading:trading_password@postgres:5432/scanner_analytics}')

# Clean up redundant prefix
lines = compose_fragment.split('\n')
filtered_lines = [line for line in lines if line.strip() != 'services:']

# Inject networks
final_lines = []
for line in filtered_lines:
    final_lines.append(line)
    if line.strip() == "depends_on: [redis-worker-1]":
        # Replace inline depends_on with the block style
        final_lines[-1] = "    depends_on:\n      redis-worker-1:\n        condition: service_healthy"
        final_lines.append("    networks:")
        final_lines.append("      - scanner-core")
        final_lines.append("      - scanner-infra")

compose_fragment = '\n'.join(final_lines) + "\n\n"

# Patch the main docker-compose-python-workers.yml
compose_path = 'docker-compose-python-workers.yml'
with open(compose_path, 'r', encoding='utf-8') as f:
    compose_content = f.read()

# Insert before `networks:` at the bottom
networks_idx = compose_content.rfind('\nnetworks:')
if networks_idx != -1:
    new_compose = compose_content[:networks_idx] + "\n" + compose_fragment + compose_content[networks_idx:]
    with open(compose_path, 'w', encoding='utf-8') as f:
        f.write(new_compose)
    print("Docker compose patched successfully")
else:
    with open(compose_path, 'a', encoding='utf-8') as f:
         f.write("\n" + compose_fragment)
    print("Docker compose appended to end successfully")
