import re
import sys
import os

patch_file = sys.argv[1]
with open(patch_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

current_file = None
current_content = []

for line in lines:
    if line.startswith('+++ b/'):
        if current_file and current_content:
            os.makedirs(os.path.dirname(current_file), exist_ok=True)
            with open(current_file, 'w', encoding='utf-8') as out:
                out.write("".join(current_content))
            current_content = []
        current_file = line[6:].strip()
    elif line.startswith('@@ '):
        pass # ignore hunk header
    elif line.startswith('+') and not line.startswith('+++'):
        if current_file:
            current_content.append(line[1:])
    elif line.startswith(' ') and current_file:
        current_content.append(line[1:])
    elif line.startswith('-') and current_file:
        pass
    else:
        if current_file and not line.startswith('diff ') and not line.startswith('index ') and not line.startswith('--- '):
            if current_content or not line.strip():
                pass

if current_file and current_content:
    os.makedirs(os.path.dirname(current_file), exist_ok=True)
    with open(current_file, 'w', encoding='utf-8') as out:
        out.write("".join(current_content))
print("Done extracting patch:", patch_file)
