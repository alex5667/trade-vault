import sys
import glob
import re

files = glob.glob('docker-compose*.yml')

def has_uncommented_children(lines, start_idx, base_indent):
    for j in range(start_idx + 1, len(lines)):
        if not lines[j].strip(): continue
        if lines[j].strip().startswith('#'): continue
        
        child_indent = len(lines[j]) - len(lines[j].lstrip())
        if child_indent <= base_indent:
            break
        return True
    return False

for file in files:
    try:
        with open(file, 'r') as f:
            lines = f.readlines()
        
        modified = False
        
        # Check resources:
        for i in range(len(lines)):
            if re.match(r'^\s*resources:\s*$', lines[i]):
                base_indent = len(lines[i]) - len(lines[i].lstrip())
                if not has_uncommented_children(lines, i, base_indent):
                    lines[i] = lines[i].replace('resources:', '# resources:')
                    modified = True
                    print(f"Commented out empty resources: at line {i+1} in {file}")
                    
        # Check deploy:
        for i in range(len(lines)):
            if re.match(r'^\s*deploy:\s*$', lines[i]):
                base_indent = len(lines[i]) - len(lines[i].lstrip())
                if not has_uncommented_children(lines, i, base_indent):
                    lines[i] = lines[i].replace('deploy:', '# deploy:')
                    modified = True
                    print(f"Commented out empty deploy: at line {i+1} in {file}")

        if modified:
            with open(file, 'w') as f:
                f.writelines(lines)
    except Exception as e:
        print(f"Error {file}: {e}")
