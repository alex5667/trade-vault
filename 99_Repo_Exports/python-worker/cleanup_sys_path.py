import os

with open("tests_to_clean.txt", "r") as f:
    files = [line.strip() for line in f if line.strip()]

patterns = [
    "sys.path.insert",
    "sys.path.append",
    "sys.path.extend"
]

for filepath in files:
    if not os.path.exists(filepath):
        continue
    with open(filepath, "r") as f:
        lines = f.readlines()
    
    new_lines = []
    changed = False
    for line in lines:
        if any(p in line for p in patterns) and (".." in line or "scanner_infra" in line):
            new_lines.append("# [AUTOGRAVITY CLEANUP] " + line)
            changed = True
        else:
            new_lines.append(line)
    
    if changed:
        print(f"Cleaning {filepath}")
        with open(filepath, "w") as f:
            f.writelines(new_lines)
