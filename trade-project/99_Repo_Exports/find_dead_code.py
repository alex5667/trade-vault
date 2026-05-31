import re
import os
import sys

def check_file(path):
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    for i in range(len(lines) - 1):
        line = lines[i]
        match = re.match(r'^(\s*)return\b.*', line)
        if match:
            indent = match.group(1)
            next_line = lines[i+1]
            if next_line.strip() == '': continue
            next_indent_match = re.match(r'^(\s*)', next_line)
            next_indent = next_indent_match.group(1) if next_indent_match else ''
            
            # If next line has exact same indentation and isn't a comment/def
            if indent == next_indent and not next_line.strip().startswith('#') and not next_line.strip().startswith('return'):
                if not next_line.strip().startswith('elif') and not next_line.strip().startswith('else') and not next_line.strip().startswith('except'):
                    print(f"{path}:{i+1} Return followed by code: {next_line.strip()}")

if __name__ == "__main__":
    search_dir = sys.argv[1]
    for root, _, files in os.walk(search_dir):
        if 'reference' in root or '.venv' in root:
            continue
        for f in files:
            if f.endswith('.py'):
                check_file(os.path.join(root, f))
