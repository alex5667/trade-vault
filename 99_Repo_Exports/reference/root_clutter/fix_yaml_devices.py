import re
import sys

for file in ['docker-compose-timers.yml', 'docker-compose-utilities.yml', 'docker-compose-python-workers.yml']:
    try:
        with open(file, 'r') as f:
            lines = f.readlines()
        
        for i in range(len(lines)):
            if 'devices:' in lines[i] and 'reservations' not in lines[i]:
                lines[i] = lines[i].replace('devices:', '# devices:')
            if 'count: all' in lines[i]:
                lines[i] = lines[i].replace('count: all', '# count: all')
            if 'count: 1' in lines[i]:
                lines[i] = lines[i].replace('count: 1', '# count: 1')
                
        with open(file, 'w') as f:
            f.writelines(lines)
        print(f"Fixed devices in {file}")
    except Exception as e:
        print(f"Error {file}: {e}")
