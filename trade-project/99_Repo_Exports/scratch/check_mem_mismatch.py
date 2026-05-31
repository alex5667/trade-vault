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
    if s.endswith('G'): return int(float(s[:-1]) * 1024)
    if s.endswith('M'): return int(float(s[:-1]))
    if s.endswith('K'): return int(float(s[:-1]) / 1024)
    return int(float(s))

for f in files:
    if not os.path.exists(f): continue
    with open(f, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if not data or 'services' not in data: continue
            for name, svc in data['services'].items():
                deploy = svc.get('deploy', {})
                resources = deploy.get('resources', {})
                limits = resources.get('limits', {})
                reservations = resources.get('reservations', {})
                
                lim_mem = parse_mem(limits.get('memory'))
                res_mem = parse_mem(reservations.get('memory'))
                
                if lim_mem != 0 and res_mem != 0 and lim_mem < res_mem:
                    print(f"Error in {f} service {name}: limit {lim_mem}M < reservation {res_mem}M")
        except Exception as e:
            print(f"Error parsing {f}: {e}")
