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
service_count = 0
for f in files:
    if not os.path.exists(f): 
        # print(f"File {f} not found")
        continue
    with open(f, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if not data or 'services' not in data: continue
            for name, svc in data['services'].items():
                service_count += 1
                deploy = svc.get('deploy', {})
                resources = deploy.get('resources', {})
                reservations = resources.get('reservations', {})
                res_mem = parse_mem(reservations.get('memory'))
                total_res += res_mem
        except Exception as e:
            pass

print(f"Total Services in active compose: {service_count}")
print(f"Total Memory Reservations: {total_res / 1024:.2f} GB")
