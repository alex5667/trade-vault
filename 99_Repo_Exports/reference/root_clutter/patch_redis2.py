import os
import re

files_to_patch = [
    'docker-compose-timers.yml',
    'docker-compose-crypto-orderflow.yml'
]

def replace_by_container(container_name, env_var, filename):
    with open(filename, 'r') as f:
        content = f.read()

    # Find the block containing the container_name
    # Since we can't easily parse YAML with regex perfectly, we'll just look for container_name: <name>
    # and replace the REDIS_URL in that generic vicinity (within ~1000 chars forward/backward without hitting another container_name)
    
    # Actually, a better way is to split by `container_name:`
    parts = content.split('container_name:')
    for i in range(1, len(parts)):
        if parts[i].strip().startswith(container_name):
            # This is the block! Let's replace REDIS_URL=${REDIS_URL:- or REDIS_URL=redis://
            # We want to replace it only before the next service or something.
            # Docker services usually start with 2 spaces. 
            # It's easier: just replace the first occurrence of REDIS_URL in this part
            
            # Find first REDIS_URL line
            lines = parts[i].split('\n')
            for j, line in enumerate(lines):
                if '- REDIS_URL=' in line:
                    if env_var == 'REDIS_URL':
                        lines[j] = re.sub(r'-\s+REDIS_URL=.*', f'- REDIS_URL=${{REDIS_URL:-redis://redis-worker-1:6379/0}}', line)
                    else:
                        lines[j] = re.sub(r'-\s+REDIS_URL=.*', f'- REDIS_URL=${{{env_var}:-redis://redis-worker-1:6379/0}}', line)
                    break
            parts[i] = '\n'.join(lines)
            
    content = 'container_name:'.join(parts)
    with open(filename, 'w') as f:
        f.write(content)

replace_by_container('scanner-liquidation-map-service', 'LIQMAP_SNAPSHOT_REDIS_URL', 'docker-compose-timers.yml')
replace_by_container('scanner-liqmap-snapshot-timer', 'LIQMAP_SNAPSHOT_REDIS_URL', 'docker-compose-timers.yml')
replace_by_container('scanner-trade-kpi-liqmap-archiver-timer', 'LIQMAP_SNAPSHOT_REDIS_URL', 'docker-compose-timers.yml')

replace_by_container('of-confirm-service', 'REDIS_URL', 'docker-compose-crypto-orderflow.yml')
# Also check exec-health-slo-checker, etc if they need specific.
# exec-health-slo-checker uses REDIS_URL correctly now!
# wait, exec-health reads from REDIS_URL but writes its own metrics using EXEC_HEALTH_SCOPE_STATE_PREFIX etc.
# Actually, the user's error says `scanner-exec-health-slo-checker` -> `Authentication required.` This should be fixed by REDIS_URL default since all `.env` REDIS_URL has `go_gateway` which is authenticated.

