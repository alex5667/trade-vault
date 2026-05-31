import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    made_changes = False

    def repl(m):
        nonlocal made_changes
        made_changes = True
        prefix = m.group(1)
        method = m.group(2)
        args = m.group(3)
        args_lower = args.lower()

        if 'fatal' in method.lower():
            new_method = 'Fatalf' if method.endswith('f') else 'Fatal'
        elif method == 'Println' or method == 'Print':
            if '❌' in args or 'error' in args_lower or 'err' in args_lower or 'fail' in args_lower:
                new_method = 'Error'
            elif '⚠️' in args or 'warn' in args_lower:
                new_method = 'Warn'
            else:
                new_method = 'Info'
        else:
            if '❌' in args or 'error' in args_lower or 'err' in args_lower or 'fail' in args_lower:
                new_method = 'Errorf'
            elif '⚠️' in args or 'warn' in args_lower:
                new_method = 'Warnf'
            else:
                new_method = 'Infof'

        return f"{prefix}{new_method}({args})"

    # more robust match
    content_new = re.sub(r'([\w\.\*&]+(?:\.log|\.logger|\.Logger))\.(Printf|Print|Println|Fatalf|Fatal)\s*\(([\s\S]*?)\)', lambda m: repl(m), content)
    
    if content_new != content:
        made_changes = True
        content = content_new

    if made_changes:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Fixed {filepath}")

for root, dirs, files in os.walk('go-worker'):
    for file in files:
        if file.endswith('.go'):
            process_file(os.path.join(root, file))

