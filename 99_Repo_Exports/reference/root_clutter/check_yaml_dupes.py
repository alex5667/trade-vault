
import yaml

try:
    with open('docker-compose-crypto-orderflow.yml', 'r') as f:
        data = yaml.safe_load(f)

    services = data.get('services', {})
    for name, svc in services.items():
        env = svc.get('environment')
        if isinstance(env, list):
            seen = set()
            dupes = []
            for item in env:
                # Handle KEY=VAL or just KEY
                key = item.split('=')[0] if isinstance(item, str) else str(item)
                # Check for duplicate KEYS
                key = item.split('=')[0].strip() if isinstance(item, str) else str(item)
                if key in seen:
                    dupes.append(item + " (duplicate key: " + key + ")")
                seen.add(key)
            
            if dupes:
                print(f"Service '{name}' has {len(dupes)} duplicate environment entries:")
                for d in dupes:
                    print(f"  - {d}")
        elif isinstance(env, dict):
             print(f"Service '{name}' has dictionary environment (no dupes possible basically)")
        else:
             pass

except Exception as e:
    print(f"Error parsing YAML: {e}")
