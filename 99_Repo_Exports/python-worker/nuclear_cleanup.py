import os
import re

patterns = [
    r'sys\.path\.insert\(0,\s*.*?\.\..*?\)',
    r'sys\.path\.append\(.*?\.\..*?\)',
    r'sys\.path\.insert\(0,\s*["\'].*?scanner_infra.*?["\']\)'
]

for root, dirs, files in os.walk('.'):
    if '.venv' in root or '.git' in root:
        continue
    for file in files:
        if file.endswith('.py') and file != 'conftest.py':
            filepath = os.path.join(root, file)
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
            except Exception:
                continue
            
            new_content = content
            changed = False
            
            # Simple line-oriented commenting for common sys.path patterns
            lines = content.splitlines()
            new_lines = []
            for line in lines:
# [AUTOGRAVITY NUCLEAR CLEANUP]                 if 'sys.path' in line and ('..' in line or 'scanner_infra' in line):
                    if not line.strip().startswith('#'):
                        new_lines.append(f"# [AUTOGRAVITY NUCLEAR CLEANUP] {line}")
                        changed = True
                    else:
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            
            if changed:
                print(f"Nuclear cleanup: {filepath}")
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(new_lines) + '\n')

