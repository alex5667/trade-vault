import re
import sys

patch_lines = open('ml_phase3_34_route_incident_rca_mirror_rca_winner_apply_apply_governance_v1.patch').readlines()
compose_lines = []
in_compose = False

for line in patch_lines:
    if line.startswith('diff --git a/docker-compose-python-workers.yml'):
        in_compose = True
    elif in_compose and line.startswith('diff --git'):
        in_compose = False
    
    if in_compose:
        if line.startswith('+') and not line.startswith('+++'):
            compose_lines.append(line[1:]) # remove the '+'

frag = ''.join(compose_lines)
if frag:
    print('Found compose lines to add')
    
    # Inject correct env vars
    def repl(m):
        block = m.group(1)
        if 'environment:' in block:
            block = re.sub(r'REDIS_URL: \$\{REDIS_URL:-.*?\}', 'REDIS_URL: ${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}', block)
            block = re.sub(r'DATABASE_URL: \$\{DATABASE_URL:-.*?\}', 'DATABASE_URL: ${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}', block)
        return block
        
    frag = re.sub(r'(\n  scanner-route-incident-.*?:\n.*?)(?=\n  scanner-route-incident-|\Z)', repl, '\n'+frag, flags=re.DOTALL)
    
    orig = open('docker-compose-python-workers.yml').read()
    
    if 'scanner-route-incident-rca-mirror-rca-winner-apply-apply-slo-v3-34:' not in orig:
        idx = orig.rfind('\nnetworks:')
        out = orig[:idx] + '\n' + frag.strip() + orig[idx:]
        open('docker-compose-python-workers.yml', 'w').write(out)
        print("Applied patch fragment to compose logic.")
    else:
        print("Already inside docker-compose-python-workers.yml")
else:
    print('No compose lines found')
