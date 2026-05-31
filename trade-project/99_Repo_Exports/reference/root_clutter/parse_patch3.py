import sys, os

def apply_mangled(patch_file):
    with open(patch_file, 'r') as f:
        lines = f.readlines()
        
    current_file = None
    current_content = []
    
    def flush():
        if current_file is not None and current_content:
            os.makedirs(os.path.dirname(current_file), exist_ok=True)
            with open(current_file, 'w') as out:
                out.write("".join(current_content))
                
    for raw in lines:
        line = raw.rstrip('\n')
        # Handle some mangled diff lines
        stripped = line[1:] if line.startswith('+') and not line.startswith('+++') else line
        
        if stripped.startswith('diff --git a/'):
            flush()
            parts = stripped.strip().split()
            current_file = "python-worker/" + parts[2][2:] # a/...
            current_content = []
        elif line.startswith('diff --git a/'):
            flush()
            parts = line.strip().split()
            current_file = "python-worker/" + parts[2][2:] # a/...
            current_content = []
        elif current_file is not None:
            if stripped.startswith('+++ ') or stripped.startswith('--- ') or stripped.startswith('index ') or stripped.startswith('new file mode '):
                continue
            if stripped.startswith('@@ '):
                continue
                
            if line.startswith('++'):
                current_content.append(line[2:] + '\n')
            elif line.startswith('+'):
                current_content.append(line[1:] + '\n')
            elif line.startswith(' '):
                current_content.append(line[1:] + '\n')
            elif line == '':
                current_content.append('\n')

    flush()

apply_mangled(sys.argv[1])
