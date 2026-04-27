import re
import glob

with open('docker-compose-python-workers.yml', 'r') as f:
    orig = f.read()

files = sorted(glob.glob('python-worker/orderflow_services/docker_compose_fragment_ml_phase3_*.yml'))

def service_name(frag_text):
    for line in frag_text.split('\n'):
        if line.strip().endswith(':'):
            return line.strip()[:-1]
    return None

new_frags = []
for file in files:
    with open(file, 'r') as f:
        text = f.read()
    
    # Check if this service is already in orig
    svc_name = None
    for line in text.split('\n'):
        if line.startswith('  ') and line.endswith(':'):
            svc_name = line.strip()[:-1]
            break
            
    if svc_name and (f"  {svc_name}:" in orig):
        print(f"Skipping {svc_name}, already exists")
        continue

    # Filter out 'services:'
    lines = [line for line in text.split('\n') if line.strip() != 'services:']
    frag_joined = '\n'.join(lines)
    new_frags.append(frag_joined)

frag_combined = '\n'.join(new_frags)

def repl(m):
    block = m.group(1)
    if 'environment:' in block:
        block = re.sub(r'\$\{REDIS_WORKER_URL:-.*?\}', '${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}', block)
        block = re.sub(r'\$\{POSTGRES_URL:-.*?\}', '${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}', block)
        block = re.sub(r'\$\{DATABASE_URL:-.*?\}', '${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}', block)
        
        # if not present explicitly after replacements
        if 'REDIS_URL:' not in block and 'REDIS_URL=' not in block:
            block = block.replace('    environment:\n', '    environment:\n      REDIS_URL: ${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}\n')
        if 'DATABASE_URL:' not in block and 'DATABASE_URL=' not in block:
            block = block.replace('    environment:\n', '    environment:\n      DATABASE_URL: ${POSTGRES_URL:-postgresql://trading:trading_password@scanner-postgres:5432/scanner_analytics}\n')
            
    return block

new_frag_combined = re.sub(r'(\n  scanner-route-incident-.*?:\n.*?)(?=\n  scanner-route-incident-|\Z)', repl, '\n'+frag_combined, flags=re.DOTALL)

if new_frags:
    idx = orig.rfind('\nnetworks:')
    out = orig[:idx] + '\n' + new_frag_combined.strip() + orig[idx:]
    with open('docker-compose-python-workers.yml', 'w') as f:
        f.write(out)
    print(f"Appended {len(new_frags)} services.")
else:
    print("No new services to append.")

