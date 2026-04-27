import os
import re

files_to_patch = [
    'docker-compose.yml',
    'docker-compose-timers.yml',
    'docker-compose-crypto-orderflow.yml'
]

redis_pattern = re.compile(r'(\s+-\s+REDIS_URL=)(redis://redis-worker-1:6379/0)')

for filename in files_to_patch:
    if not os.path.exists(filename):
        continue
    with open(filename, 'r') as f:
        content = f.read()

    # Generic replace:
    content = redis_pattern.sub(r'\1${REDIS_URL:-\2}', content)
    
    # Specific replaces for liqmap:
    # Service blocks start with `  scanner-liquidation-map-service:` etc.
    # We can just replace REDIS_URL=${REDIS_URL:- with REDIS_URL=${LIQMAP_SNAPSHOT_REDIS_URL:-
    # inside specific services by doing string replacement on the whole block
    
    def replace_specific(svc_name, env_var, text):
        # find service block
        block_pattern = re.compile(r'(\s+)' + svc_name + r':(.*?)(?=\n\s+[A-Za-z0-9_-]+:|$)', re.DOTALL)
        match = block_pattern.search(text)
        if match:
            indent = match.group(1)
            block = match.group(2)
            # inside block, replace REDIS_URL=${REDIS_URL:- with REDIS_URL=${env_var:-
            block = block.replace('REDIS_URL=${REDIS_URL:-', f'REDIS_URL=${{{env_var}:-')
            # and replace hardcoded if it wasn't caught
            block = block.replace('REDIS_URL=redis://', f'REDIS_URL=${{{env_var}:-redis://')
            text = text[:match.start(2)] + block + text[match.end(2):]
        return text

    content = replace_specific('scanner-liquidation-map-service', 'LIQMAP_SNAPSHOT_REDIS_URL', content)
    content = replace_specific('scanner-liqmap-snapshot-timer', 'LIQMAP_SNAPSHOT_REDIS_URL', content)
    content = replace_specific('scanner-trade-kpi-liqmap-archiver-timer', 'LIQMAP_SNAPSHOT_REDIS_URL', content)
    # scanner-trade-kpi-liqmap-archiver-timer uses trades:post_sl which is in DB 0. It also uses liqmap:snapshot. So liqmap ACL is good.
    content = replace_specific('of-confirm-service', 'REDIS_URL', content)

    # Note: docker-compose-timers.yml has `scanner-signal-quality-kpi-worker` which could be reading closed trades, REDIS_URL is fine.
    
    with open(filename, 'w') as f:
        f.write(content)
    print(f"Patched {filename}")

