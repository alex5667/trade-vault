import yaml
import os

files_to_throttle = [
    "docker-compose-shared.yml",
    "docker-compose-go-workers.yml",
    "docker-compose-news-pipeline.yml",
    "docker-compose-crypto-orderflow.yml",
    "docker-compose-backend.yml",
    "docker-compose.hub-v2.yml",
    "docker-compose-monitoring.yml",
    "docker-compose-monitoring-local.yml",
    "docker-compose.tp-trailing.yml",
    "docker-compose-timers.yml",
    "docker-compose-utilities.yml",
    "docker-compose-of-gate-sre.yml",
    "docker-compose.notify-telegram-v2.yml",
    "docker-compose-binance.yml",
    "docker-compose-networks-volumes.yml",
    "python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-summary-writer-v1.yml",
    "python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-health-exporter-v1.yml",
    "python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-v1.yml"
]

files_with_fixed_512 = ["docker-compose-python-workers.yml"]

def parse_mem(s):
    if not s: return 0
    s = str(s).upper()
    mult = 1
    if 'G' in s: mult = 1024
    num_str = "".join(c for c in s if c.isdigit() or c == '.')
    try: return float(num_str) * mult
    except: return 0

def throttle_file(file_path, target_res="24M"):
    if not os.path.exists(file_path): return
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    if not data or 'services' not in data: return
    
    changed = False
    for name, svc in data['services'].items():
        if 'deploy' not in svc: svc['deploy'] = {}
        if 'resources' not in svc['deploy']: svc['deploy']['resources'] = {}
        if 'reservations' not in svc['deploy']['resources']: svc['deploy']['resources']['reservations'] = {}
        
        svc['deploy']['resources']['reservations']['memory'] = target_res
        
        # Invariant check: limit >= reservation
        limits = svc['deploy']['resources'].get('limits', {})
        if 'memory' in limits:
            if parse_mem(limits['memory']) < parse_mem(target_res):
                limits['memory'] = target_res
                svc['deploy']['resources']['limits'] = limits
        changed = True

    if changed:
        with open(file_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        print(f"Throttled {file_path} to {target_res}")

def set_fixed(file_path, res="512M"):
    if not os.path.exists(file_path): return
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    if not data or 'services' not in data: return
    changed = False
    for name, svc in data['services'].items():
        if 'deploy' not in svc: svc['deploy'] = {}
        if 'resources' not in svc['deploy']: svc['deploy']['resources'] = {}
        if 'reservations' not in svc['deploy']['resources']: svc['deploy']['resources']['reservations'] = {}
        svc['deploy']['resources']['reservations']['memory'] = res
        changed = True
    if changed:
        with open(file_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        print(f"Ensured {file_path} has {res}")

for f in files_to_throttle:
    throttle_file(f, "24M")

for f in files_with_fixed_512:
    set_fixed(f, "512M")
