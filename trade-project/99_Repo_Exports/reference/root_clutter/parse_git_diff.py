import sys, os

def apply_git_patch(patch_file):
    with open(patch_file, 'r') as f:
        lines = f.readlines()
        
    current_file = None
    current_content = []
    
    def flush():
        if current_file is not None and current_content:
            path = os.path.join("python-worker", current_file)
            print(f"Extracting {path}...")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as out:
                out.write("".join(current_content))
                
    in_file = False
    
    for raw in lines:
        line = raw.rstrip('\n')
        if line.startswith('+++ b/'):
            flush()
            current_file = line[len('+++ b/'):].strip()
            current_content = []
            in_file = True
            continue
            
        if line.startswith('diff --git'):
            in_file = False
            continue
            
        if in_file:
            if line.startswith('+'):
                current_content.append(line[1:] + '\n')
            elif line.startswith(' '):
                # We skip context lines if it's purely adding a file
                # But if it's a diff adding a file, usually all lines are +
                pass
            elif line == '':
                current_content.append('\n')

    flush()

if __name__ == '__main__':
    apply_git_patch(sys.argv[1])
