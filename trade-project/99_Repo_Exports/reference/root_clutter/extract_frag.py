import re
patch = open('ml_phase3_34_route_incident_rca_mirror_rca_winner_apply_apply_governance_v1.patch').read()

import sys
# Let's extract the part of the patch corresponding to docker-compose-python-workers.yml or docker_compose_fragment...
# Since docker-compose-python-workers.yml could be modified directly, let's find:
frag = re.search(r'diff --git.*?docker.compose.python.workers\.yml.*?\n(.*?)diff --git', patch, re.DOTALL)
if not frag:
    frag = re.search(r'diff --git.*?docker.compose.python.workers\.yml.*?\n(.*)', patch, re.DOTALL)

lines = []
if frag:
    content = frag.group(1)
    for line in content.split('\n'):
        if line.startswith('+') and not line.startswith('+++'):
            lines.append(line[1:])

if lines:
    with open('/tmp/extracted_34.yml', 'w') as f:
        f.write('\n'.join(lines))
    print('Found compose lines to add')

    # Apply them directly to docker-compose-python-workers.yml
    def repl(m):
        block = m.group(1)
        if 'environment:' in block:
            block = re.sub(r'REDIS_URL: \$\{REDIS_URL:-.*?\}', 'REDIS_URL: ${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}', block)
            block = re.sub(r'DATABASE_URL: \$\{DATABASE_URL:-.*?\}', 'DATABASE_URL: ${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}', block)
            if 'REDIS_URL=' not in block and 'REDIS_URL:' not in block:
                block = block.replace('    environment:\n', '    environment:\n      REDIS_URL: ${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}\n')
            if 'DATABASE_URL=' not in block and 'DATABASE_URL:' not in block:
                block = block.replace('    environment:\n', '    environment:\n      DATABASE_URL: ${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}\n')
        return block

    frag_str = '\n'.join(lines)
    frag_str = re.sub(r'(\n  scanner-route-incident-.*?:\n.*?)(?=\n  scanner-route-incident-|\Z)', repl, '\n'+frag_str, flags=re.DOTALL)

    orig = open('docker-compose-python-workers.yml').read()
    if 'scanner-route-incident-rca-mirror-rca-winner-apply-apply-slo-v3-34:' not in orig:
        idx = orig.rfind('\nnetworks:')
        out = orig[:idx] + '\n' + frag_str.strip() + orig[idx:]
        open('docker-compose-python-workers.yml', 'w').write(out)
        print("Applied patch fragment to compose logic.")
    else:
        print("Already inside docker-compose-python-workers.yml")
else:
    print('No compose lines found')

