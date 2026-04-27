import yaml
import os

files = [
    "docker-compose-shared.yml",
    "docker-compose-infrastructure.yml",
    "docker-compose-go-workers.yml",
    "docker-compose-python-workers.yml",
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

def parse_mem(s):
    if not s: return 0
    s = str(s).upper()
    mult = 1
    if 'G' in s: mult = 1024
    num_str = "".join(c for c in s if c.isdigit() or c == '.')
    try:
        return float(num_str) * mult
    except:
        return 0

total_res = 0
for f in files:
    if not os.path.exists(f): continue
    file_res = 0
    with open(f, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if not data or 'services' not in data: continue
            for name, svc in data['services'].items():
                deploy = svc.get('deploy', {})
                resources = deploy.get('resources', {})
                reservations = resources.get('reservations', {})
                res_mem = parse_mem(reservations.get('memory'))
                if res_mem == 0:
                    # Fallback to limit if no reservation? NO, docker doesn't do that for scheduling usually but it's good to know.
                    # res_mem = parse_mem(resources.get('limits', {}).get('memory'))
                    pass
                file_res += res_mem
        except: pass
    if file_res > 0:
        print(f"{f}: {file_res / 1024:.2f} GB")
    total_res += file_res

print(f"Total: {total_res / 1024:.2f} GB")
