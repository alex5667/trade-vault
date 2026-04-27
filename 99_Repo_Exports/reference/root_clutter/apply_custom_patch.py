import sys
import os

for patch_path in sys.argv[1:]:
    with open(patch_path, 'r') as f:
        lines = f.readlines()
    
    current_file = None
    current_lines = []
    
    for line in lines:
        if line.startswith("*** Add File: "):
            if current_file:
                with open("python-worker/" + current_file, 'w') as out:
                    out.write("".join(current_lines))
                print(f"Written python-worker/{current_file}")
            
            current_file = line.strip().split("*** Add File: ")[1]
            os.makedirs(os.path.dirname("python-worker/" + current_file), exist_ok=True)
            current_lines = []
        elif current_file and line.startswith("+"):
            # strip the leading '+'
            current_lines.append(line[1:])
    
    if current_file:
        with open("python-worker/" + current_file, 'w') as out:
            out.write("".join(current_lines))
        print(f"Written python-worker/{current_file}")

