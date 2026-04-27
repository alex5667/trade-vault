import re
import glob

def update_file(filename):
    with open(filename, 'r') as f:
        content = f.read()

    # Update EDGE_EXEC_HEALTH_MODE
    content = re.sub(r'EDGE_EXEC_HEALTH_MODE:\s*monitor', 'EDGE_EXEC_HEALTH_MODE: auto', content)
    content = re.sub(r'EDGE_EXEC_HEALTH_MODE=monitor', 'EDGE_EXEC_HEALTH_MODE=auto', content)
    
    # Update GATE_PROFILE
    content = re.sub(r'GATE_PROFILE:\s*default', 'GATE_PROFILE: hard', content)
    content = re.sub(r'GATE_PROFILE=default', 'GATE_PROFILE=hard', content)

    with open(filename, 'w') as f:
        f.write(content)

for fn in glob.glob('*.yml') + glob.glob('*.yaml'):
    update_file(fn)

print("Replacement complete.")
