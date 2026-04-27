import re

log_file = "/home/alex/.gemini/antigravity/brain/b45bfacb-0403-48e4-ac08-c6ae272ea39c/.system_generated/logs/overview.txt"
with open(log_file, "r") as f:
    lines = f.readlines()

current_file = None
file_lines = {}
capture = False

for line in lines:
    m = re.match(r'^File Path: `file://(.*?)`$', line.strip())
    if m:
        current_file = m.group(1)
        file_lines[current_file] = []
        capture = False
        continue
    
    if current_file and "The following code has been modified to include a line number" in line:
        capture = True
        continue
        
    if current_file and capture:
        if "The above content shows the entire, complete file contents" in line or "Please note that the above snippet only shows the MODIFIED lines" in line or line.startswith("The above content only shows"):
            capture = False
            continue
            
        m_line = re.match(r'^\d+:\s?(.*)', line) # Match "1: content" or "12: "
        if m_line is not None:
            # We must be careful because some lines end with \n
            file_lines[current_file].append(m_line.group(1).rstrip('\n') + '\n')

for path, content in file_lines.items():
    if content:
        print(f"Recovering {path} (lines: {len(content)})")
        with open(path, "w") as f:
            f.writelines(content)

