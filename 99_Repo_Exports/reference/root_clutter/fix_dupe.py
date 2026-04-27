import re

def count_keys(env_str):
    lines = env_str.split('\n')
    keys = []
    for line in lines:
        match = re.search(r'^\s*-\s*([A-Za-z0-9_]+)=', line)
        if match:
            keys.append(match.group(1))
        else:
            match = re.search(r'^\s*([A-Za-z0-9_]+):', line)
            if match:
                keys.append(match.group(1))
    return keys

with open('docker-compose-python-workers.yml') as f:
    lines = f.readlines()

out = []
in_env = False
env_keys = set()
for line in lines:
    if line.strip().startswith('environment:') or line.strip() == 'environment:':
        in_env = True
        env_keys = set()
        out.append(line)
        continue
        
    if in_env:
        if line.strip() == '' or line.startswith('      -') or line.startswith('      '): # typical env key indentation
            match1 = re.search(r'^\s*-\s*([A-Za-z0-9_]+)=', line)
            match2 = re.search(r'^\s*([A-Za-z0-9_]+):', line)
            key = None
            if match1: key = match1.group(1)
            elif match2: key = match2.group(1)
            
            if key:
                if key in env_keys:
                    print(f"Skipping duplicate key {key} in environment")
                    continue
                else:
                    env_keys.add(key)
        elif not line.startswith(' ') and line.strip() != '' or line.startswith('    ') and not line.startswith('      '):
            # exited environment block
            in_env = False
            
    out.append(line)

with open('docker-compose-python-workers.yml', 'w') as f:
    f.writelines(out)

