import re
import os

for root, dirs, files in os.walk('python-worker'):
    if '.venv' in dirs: dirs.remove('.venv')
    if '__pycache__' in dirs: dirs.remove('__pycache__')
    
    for f in files:
        if f.endswith('.py'):
            path = os.path.join(root, f)
            try:
                with open(path, 'r', encoding='utf-8', errors='ignore') as file:
                    content = file.read()
                
                if 'get_ny_time_millis().0' in content:
                    content = content.replace('get_ny_time_millis().0', 'get_ny_time_millis()')
                    with open(path, 'w', encoding='utf-8') as file:
                        file.write(content)
                    print(f"Fixed .0 in {path}")
            except Exception as e:
                pass
