import re
import os

with open("fixed_phase3_56.patch", "r") as f:
    lines = f.readlines()

current_file = None
out_lines = []

for line in lines:
    if line.startswith("diff --git a/"):
        if current_file and out_lines:
            os.makedirs(os.path.dirname(current_file), exist_ok=True)
            with open(current_file, "w") as out:
                out.writelines(out_lines)
        
        parts = line.strip().split(" b/")
        current_file = parts[1]
        out_lines = []
    elif current_file:
        if line.startswith("new file mode") or line.startswith("index") or line.startswith("--- /dev/null") or line.startswith("+++ b/"):
            continue
        elif line.startswith("@@ "):
            continue
        else:
            if line.startswith("+"):
                out_lines.append(line[1:])
            elif line.startswith("\\ No newline"):
                pass
            else:
                out_lines.append(line)

if current_file and out_lines:
    os.makedirs(os.path.dirname(current_file), exist_ok=True)
    with open(current_file, "w") as out:
        out.writelines(out_lines)

print("Extraction complete.")
