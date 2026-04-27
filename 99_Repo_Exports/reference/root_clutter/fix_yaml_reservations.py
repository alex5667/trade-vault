import sys
import glob
import re

files = glob.glob('docker-compose*.yml')
for file in files:
    try:
        with open(file, 'r') as f:
            lines = f.readlines()
        
        modified = False
        for i in range(len(lines)):
            if re.match(r'^\s*reservations:\s*$', lines[i]):
                # Look ahead to see if it has any uncommented children
                has_children = False
                base_indent = len(lines[i]) - len(lines[i].lstrip())
                
                for j in range(i+1, len(lines)):
                    if not lines[j].strip(): continue
                    if lines[j].strip().startswith('#'): continue
                    
                    child_indent = len(lines[j]) - len(lines[j].lstrip())
                    if child_indent <= base_indent:
                        break # End of block
                    
                    # If we found an uncommented child, then it has children
                    has_children = True
                    break
                    
                if not has_children:
                    lines[i] = lines[i].replace('reservations:', '# reservations:')
                    modified = True
                    print(f"Commented out empty reservations: at line {i+1} in {file}")

        if modified:
            with open(file, 'w') as f:
                f.writelines(lines)
    except Exception as e:
        print(f"Error {file}: {e}")
