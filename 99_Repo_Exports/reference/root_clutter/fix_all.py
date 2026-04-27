import os
import glob
import re

with open('docker-compose-python-workers.yml', 'r') as f:
    orig = f.read()

files = sorted(glob.glob('python-worker/orderflow_services/docker_compose_fragment_ml_phase3_*.yml'))
frag_texts = []
for file in files:
    with open(file, 'r') as f:
        text = f.read()
    lines = [line for line in text.split('\n') if line.strip() != 'services:']
    frag_texts.append('\n'.join(lines))

frag_combined = '\n'.join(frag_texts)

def repl(m):
    block = m.group(1)
    if 'environment:' in block:
        block = re.sub(r'\$\{REDIS_WORKER_URL:-.*?\}', '${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}', block)
        block = re.sub(r'\$\{POSTGRES_URL:-.*?\}', '${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}', block)
        block = re.sub(r'\$\{DATABASE_URL:-.*?\}', '${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}', block)
        
        # if not present explicitly after replacements
        if 'REDIS_URL:' not in block and 'REDIS_URL=' not in block:
            block = block.replace('    environment:\n', '    environment:\n      REDIS_URL: ${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}\n')
        
    return block

new_frag_combined = re.sub(r'(\n  scanner-route-incident-.*?:\n.*?)(?=\n  scanner-route-incident-|\Z)', repl, '\n'+frag_combined, flags=re.DOTALL)

idx = orig.rfind('\nnetworks:')
out = orig[:idx] + '\n' + new_frag_combined.strip() + orig[idx:]

with open('docker-compose-python-workers.yml', 'w') as f:
    f.write(out)
