import yaml
import os

def update_compose(file_path, worker_res="512M", timer_res="32M"):
    if not os.path.exists(file_path): return
    
    # We use a custom dumper to preserve formatting as much as possible, 
    # but regular yaml.safe_load/dump is fine for this task.
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
        
    if not data or 'services' not in data: return
    
    changed = False
    for name, svc in data['services'].items():
        # Check if it's a python worker or a timer based on file/service name
        target_res = None
        if "python-workers" in file_path:
            target_res = worker_res
        elif "timers" in file_path:
            target_res = timer_res
            
        if target_res:
            if 'deploy' not in svc: svc['deploy'] = {}
            if 'resources' not in svc['deploy']: svc['deploy']['resources'] = {}
            if 'reservations' not in svc['deploy']['resources']: svc['deploy']['resources']['reservations'] = {}
            
            svc['deploy']['resources']['reservations']['memory'] = target_res
            changed = True
            
            # Also ensure limit is at least as large as reservation
            limits = svc['deploy']['resources'].get('limits', {})
            if 'memory' in limits:
                # Basic check: if limit is e.g. 128M and target is 512M, bump limit
                def to_mb(s):
                    s = str(s).upper()
                    if 'G' in s: return float(s[:-1]) * 1024
                    if 'M' in s: return float(s[:-1])
                    return float(s)
                
                if to_mb(limits['memory']) < to_mb(target_res):
                    limits['memory'] = target_res
                    svc['deploy']['resources']['limits'] = limits

    if changed:
        with open(file_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        print(f"Updated {file_path}")

update_compose("docker-compose-python-workers.yml", worker_res="512M")
update_compose("docker-compose-timers.yml", timer_res="32M")
