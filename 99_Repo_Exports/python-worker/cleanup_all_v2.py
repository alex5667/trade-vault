import os

with open("all_py_files.txt", "r") as f:
    files = [line.strip() for line in f if line.strip()]

patterns = [
    "sys.path.insert",
    "sys.path.append",
    "sys.path.extend"
]

for filepath in files:
    if not os.path.exists(filepath):
        continue
    if "conftest.py" in filepath:
        continue
        
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Skipping {filepath} due to {e}")
        continue
    
    new_lines = []
    changed = False
    for line in lines:
        is_hack = any(p in line for p in patterns) and (".." in line or "scanner_infra" in line)
        if is_hack:
            if not line.startswith("# [AUTOGRAVITY CLEANUP]"):
                new_lines.append("# [AUTOGRAVITY CLEANUP] " + line)
                changed = True
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
    
    if changed:
        print(f"Cleaning {filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
