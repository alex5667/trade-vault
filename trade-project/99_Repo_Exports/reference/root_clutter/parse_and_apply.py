import re
import os

with open('patch_ofc_contextual_runtime_shadow_v1.diff', 'r') as f:
    lines = f.read().splitlines()

current_file = None
is_new_file = False
file_content = []

def save_current():
    global current_file, is_new_file, file_content
    if current_file and is_new_file:
        # Save to python-worker
        out_path = current_file.replace('a/tick_flow_full/', 'python-worker/')
        out_path = out_path.replace('b/tick_flow_full/', 'python-worker/')
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, 'w') as out:
            out.write('\n'.join(file_content) + '\n')
        print(f"Created {out_path}")
    current_file = None
    is_new_file = False
    file_content = []

i = 0
while i < len(lines):
    line = lines[i]
    if line.startswith('diff --git'):
        save_current()
        parts = line.split()
        current_file = parts[2] # a/...
        # check if new file
        if i + 1 < len(lines) and lines[i+1].startswith('new file'):
            is_new_file = True
            # skip until +++
            while i < len(lines) and not lines[i].startswith('+++'):
                i += 1
            # skip the @@
            i += 1
            while i < len(lines) and lines[i].startswith('@@'):
                i += 1
            continue
    elif is_new_file:
        if line.startswith('+'):
            file_content.append(line[1:])
        elif line == '':
            file_content.append('')
    i += 1

save_current()
