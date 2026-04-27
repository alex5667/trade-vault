import yaml
import collections

def find_duplicates(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)

    services = data.get('services', {})
    for name, service in services.items():
        env = service.get('environment')
        if not env:
            continue
        
        if isinstance(env, list):
            # Check for exact duplicates or key duplicates
            seen = set()
            duplicates = []
            keys_seen = set()
            key_duplicates = []
            
            for item in env:
                if isinstance(item, str):
                    if item in seen:
                        duplicates.append(item)
                    seen.add(item)
                    
                    key = item.split('=', 1)[0]
                    if key in keys_seen:
                        key_duplicates.append(key)
                    keys_seen.add(key)
            
            if duplicates:
                print(f"Service '{name}' has exact duplicates:")
                for d in duplicates:
                    print(f"  - {d}")
            if key_duplicates: # Optional: warn about key redefinition? Docker compose might just take the last one, but strict mode might error.
                pass # The error specifically said "non-unique items", usually implies exact strings or maybe just keys.
                # Let's print unique key duplicates to be safe, as that's likely the issue.
                print(f"Service '{name}' has duplicate keys:")
                for k in set(key_duplicates):
                    print(f"  - {k}")
        elif isinstance(env, dict):
             pass # Dict keys are unique by definition in YAML (loader would likely fail or overwrite)

find_duplicates('docker-compose-python-workers.yml')
