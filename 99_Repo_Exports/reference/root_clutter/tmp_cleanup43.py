with open('docker-compose-python-workers.yml', 'r') as f:
    content = f.read()

# Split into lines
lines = content.split('\n')

new_lines = []
skip_mode = False
target_key = 'scanner-route-incident-rca-mirror-rca-winner-apply-apply-governance-incident-bundles-v3-43:'

# Correct entire block for 3.43
correct_block = [
    '  scanner-route-incident-rca-mirror-rca-winner-apply-apply-governance-incident-bundles-v3-43:',
    '    build: { context: ., dockerfile: python-worker/Dockerfile.gpu }',
    '    command: ["python", "-m", "orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundle_builder_v3_43"]',
    '    restart: unless-stopped',
    '    environment:',
    '      - REDIS_URL=${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}',
    '      - DATABASE_URL=${POSTGRES_URL:-postgresql://trading:trading_password@postgres:5432/scanner_analytics}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_PORT=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_PORT:-9973}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_LOOKBACK_COUNT=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_LOOKBACK_COUNT:-80}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_RECENT_WINDOW_MIN=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_RECENT_WINDOW_MIN:-360}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_ONLY_SEVERITIES=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_ONLY_SEVERITIES:-warning,critical}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_APPLY_DECISIONS=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_APPLY_DECISIONS:-APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_VERIFY_DECISIONS=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_VERIFY_DECISIONS:-ROLLBACK_PREVIOUS_POLICY}',
    '      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_RETRY_DECISIONS=${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_RETRY_DECISIONS:-EXHAUSTED}',
    '    ports: ["${ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_PORT:-9973}:9973"]',
    '    depends_on:',
    '      redis-worker-1:',
    '        condition: service_healthy',
    '    networks:',
    '      - scanner-core',
    '      - scanner-infra'
]

# Standardize blocks before networks:
for line in lines:
    if target_key in line:
        skip_mode = True
        # Append correct block once
        new_lines.extend(correct_block)
        continue
    
    if skip_mode:
        # If we reach global networks section, stop skipping
        if line.startswith('networks:'):
            skip_mode = False
            new_lines.append('')
            new_lines.append(line)
        continue
    
    new_lines.append(line)

with open('docker-compose-python-workers.yml', 'w') as f:
    f.write('\n'.join(new_lines))
