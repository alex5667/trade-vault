import sys
import glob

files = glob.glob('docker-compose*.yml')
for file in files:
    try:
        with open(file, 'r') as f:
            lines = f.readlines()
        
        modified = False
        for i in range(len(lines)):
            if 'devices:' in lines[i] and 'reservations' not in lines[i]:
                lines[i] = lines[i].replace('devices:', '# devices:')
                modified = True
            if 'count: all' in lines[i]:
                lines[i] = lines[i].replace('count: all', '# count: all')
                modified = True
            if 'count: 1' in lines[i]:
                lines[i] = lines[i].replace('count: 1', '# count: 1')
                modified = True
                
        if modified:
            with open(file, 'w') as f:
                f.writelines(lines)
            print(f"Fixed devices in {file}")
    except Exception as e:
        print(f"Error {file}: {e}")
