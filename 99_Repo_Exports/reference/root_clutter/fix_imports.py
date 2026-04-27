import os
import re

IMPORT_STMT = "from utils.time_utils import get_ny_time_millis"

for root, dirs, files in os.walk('python-worker'):
    if '.venv' in dirs: dirs.remove('.venv')
    if '__pycache__' in dirs: dirs.remove('__pycache__')
    
    for f in files:
        if f.endswith('.py') and f != "time_utils.py":
            path = os.path.join(root, f)
            try:
                with open(path, 'r', encoding='utf-8', errors='ignore') as file:
                    content = file.read()
                
                if IMPORT_STMT in content:
                    lines = content.split('\n')
                    # remove all exact matches (ignoring leading whitespace)
                    new_lines = []
                    needs_fix = False
                    for line in lines:
                        if line.strip() == IMPORT_STMT:
                            needs_fix = True
                        else:
                            new_lines.append(line)
                            
                    if needs_fix:
                        # Find safe place to insert
                        insert_idx = 0
                        for i, line in enumerate(new_lines):
                            if line.startswith('from __future__'):
                                insert_idx = i + 1
                        
                        new_lines.insert(insert_idx, IMPORT_STMT)
                        
                        with open(path, 'w', encoding='utf-8') as file:
                            file.write('\n'.join(new_lines))
                        print(f"Fixed import in {path}")
            except Exception as e:
                pass
