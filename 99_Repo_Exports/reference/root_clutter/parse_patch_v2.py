import sys, os

def apply_patch(patch_file):
    with open(patch_file, 'r') as f:
        lines = f.readlines()
        
    current_file = None
    current_content = []
    
    def flush():
        if current_file is not None and current_content:
            path = os.path.join("python-worker", current_file)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as out:
                out.write("".join(current_content))
                
    for raw in lines:
        line = raw.rstrip('\n')
        if line.startswith('*** Add File: '):
            flush()
            current_file = line[len('*** Add File: '):].strip()
            current_content = []
        elif line == '*** Begin Patch' or line == '*** End Patch':
            pass
        elif current_file is not None:
            if line.startswith('+'):
                current_content.append(line[1:] + '\n')
            elif line.startswith(' '):
                # not strictly needed here but safely handle context lines if they existed
                current_content.append(line[1:] + '\n')
            elif line == '':
                current_content.append('\n')

    flush()

apply_patch(sys.argv[1])
