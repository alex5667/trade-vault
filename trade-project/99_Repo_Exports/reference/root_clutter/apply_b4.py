import re
import os

with open("mega_patch_B4_ironclad_train_serve_policy_snapshot_sidecar_v2.git.diff", "r") as f:
    diff_content = f.read()

files = diff_content.split('diff --git ')
for file_diff in files[1:]:
    lines = file_diff.split('\n')
    header = lines[0]
    m = re.match(r'a/(.*?) b/(.*)', header)
    if not m: continue
    filepath = m.group(2)
    
    # only care about new files
    is_new = any(line.startswith('new file mode') for line in lines[:5])
    if not is_new: continue

    # extract content
    content_lines = []
    # find first @@
    idx = 0
    while idx < len(lines) and not lines[idx].startswith('@@ '):
        idx += 1
    
    idx += 1
    while idx < len(lines):
        line = lines[idx]
        if line.startswith('+'):
            content_lines.append(line[1:])
        elif line.startswith(' '):
            content_lines.append(line[1:])
        idx += 1
        
    content = '\n'.join(content_lines) + '\n'
    
    # paths to write
    write_paths = []
    if filepath.startswith('tick_flow_full/'):
        write_paths.append(f"reference/{filepath}")
    else:
        write_paths.append(f"python-worker/{filepath}")
        write_paths.append(f"reference/tick_flow_full/{filepath}")

    for w_path in write_paths:
        os.makedirs(os.path.dirname(w_path), exist_ok=True)
        with open(w_path, 'w') as out_f:
            out_f.write(content)
        print(f"Wrote {w_path}")

