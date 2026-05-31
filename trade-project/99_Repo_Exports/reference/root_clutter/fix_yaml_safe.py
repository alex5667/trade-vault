import sys

for file in ['docker-compose-timers.yml', 'docker-compose-utilities.yml', 'docker-compose-crypto-orderflow.yml', 'docker-compose.tp-trailing.yml']:
    try:
        with open(file, 'r') as f:
            lines = f.readlines()
        
        for i in range(len(lines) - 1):
            if 'reservations:' in lines[i] and 'devices:' in lines[i+1]:
                # Comment them out
                lines[i] = lines[i].replace('reservations:', '# reservations:')
                lines[i+1] = lines[i+1].replace('devices:', '# devices:')
                
        with open(file, 'w') as f:
            f.writelines(lines)
        print(f"Fixed {file}")
    except Exception as e:
        print(f"Error {file}: {e}")
