import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    made_changes = False
    
    if '*log.Logger' in content:
        content = content.replace('*log.Logger', '*zap.SugaredLogger')
        made_changes = True

    if 'log.New(os.Stdout, "", log.LstdFlags)' in content:
        content = content.replace('log.New(os.Stdout, "", log.LstdFlags)', 'zap.S()')
        made_changes = True

    if 'log.New(os.Stdout, "", 0)' in content:
        content = content.replace('log.New(os.Stdout, "", 0)', 'zap.S()')
        made_changes = True

    if made_changes:
        if '"go.uber.org/zap"' not in content:
            import_blocks = list(re.finditer(r'import\s+\((.*?)\)', content, flags=re.DOTALL))
            if import_blocks:
                last_import = import_blocks[-1]
                inner = last_import.group(1)
                new_inner = inner + '\n\t"go.uber.org/zap"\n'
                content = content[:last_import.start(1)] + new_inner + content[last_import.end(1):]
            else:
                pkg_match = re.search(r'package\s+\w+\n', content)
                if pkg_match:
                    content = content[:pkg_match.end()] + '\nimport "go.uber.org/zap"\n' + content[pkg_match.end():]
        
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Updated {filepath}")

def main():
    paths = ['go-worker', 'go-gateway']
    for p in paths:
        for root, dirs, files in os.walk(p):
            for file in files:
                if file.endswith('.go') and 'file_logger.go' not in file:
                    process_file(os.path.join(root, file))

if __name__ == '__main__':
    main()
